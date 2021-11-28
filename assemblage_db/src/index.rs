use crate::{AsDbErrorWithContext, AsIdNotFoundErrorWithContext, DbSnapshot, Error, RestoredNode, Result, Slot, TypedKvSnapshot, broadcast::{self, Broadcast, BroadcastId, BroadcastSubscription, OwnedBroadcast}, data::{Child, Id, Layout, Node, Overlap, Parent, Parents, Styles}};
use assemblage_kv::{
    self,
    storage::{MemoryStorage, Storage},
    timestamp::timestamp_now,
    KvStore,
};
use futures::future::try_join_all;
use serde::{Deserialize, Serialize};
use std::{
    cmp::min,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    convert::TryInto,
};
use uuid::Uuid;

type GramsById = HashMap<Id, Vec<u32>>;

impl<S: Storage> DbSnapshot<'_, S> {
    /// Uploads the specified node and all of its descendants as a broadcast
    /// that can be shared with other DBs via its id.
    ///
    /// If there is already an active broadcast for the specified id, the
    /// broadcast will be updated by appending the contents that were modified
    /// since the last broadcast update.
    pub async fn publish_broadcast(&mut self, id: Id) -> Result<Broadcast> {
        let existing_broadcast = self
            .store
            .get_typed::<_, OwnedBroadcast>(Slot::BroadcastPublished as u8, &id)
            .await
            .with_context("publish_broadcast", "get published broadcast")?;
        let broadcast = broadcast::push(self, id, existing_broadcast.as_ref()).await?;
        let result = (&broadcast).into();
        self.store
            .insert_typed(Slot::BroadcastPublished as u8, id, broadcast)
            .with_context("publish_broadcast", "insert published broadcasts")?;
        Ok(result)
    }

    /// Subscribes to the specified broadcast, fetching it only if no existing
    /// subscription exists and returning the number of bytes received.
    pub async fn subscribe_to_broadcast(&mut self, id: &BroadcastId) -> Result<u32> {
        let subscription = self
            .store
            .get_typed::<_, BroadcastSubscription>(Slot::BroadcastSubscribed as u8, id)
            .await
            .with_context("subscribe_to_broadcast", "get subscription")?;
        if subscription.is_none() {
            let bytes_received = self.fetch_broadcast(id).await?;
            Ok(bytes_received)
        } else {
            Ok(0)
        }
    }

    /// Updates the nodes of the specified broadcast by fetching the latest
    /// updates, returning the number of bytes received.
    ///
    /// Updates the nodes by fetching and importing into the DB all the changes
    /// since the last fetch. Creates a subscription for the broadcast and
    /// fetches all of its content if no existing subscription for it exists.
    pub async fn fetch_broadcast(&mut self, id: &BroadcastId) -> Result<u32> {
        let mut subscription = self
            .store
            .get_typed::<_, BroadcastSubscription>(Slot::BroadcastSubscribed as u8, &id)
            .await
            .with_context("fetch_broadcast", "get subscription")?
            .unwrap_or_default();
        let (bytes, last_pushed) = broadcast::pull(id, subscription.last_updated).await?;
        subscription.last_updated = last_pushed;
        if subscription.namespace == Id::root() {
            subscription.namespace = Id::new();
        }
        self.import(&bytes, subscription.namespace).await?;
        self.store
            .insert_typed(Slot::BroadcastSubscribed as u8, id, subscription)
            .with_context("fetch_broadcasts", "insert subscription")?;
        Ok(bytes.len() as u32)
    }

    /// Returns a list of all active published broadcasts that contain the
    /// specified node.
    pub async fn list_broadcasts(&self, id: Id) -> Result<BTreeSet<Broadcast>> {
        let mut published: Vec<OwnedBroadcast> = Vec::new();
        let keys: Vec<Id> = self
            .store
            .keys_typed(Slot::BroadcastPublished as u8)
            .await
            .with_context("list_broadcasts", "get published broadcast keys")?;
        for id in keys {
            published.push(
                self.store
                    .get_typed(Slot::BroadcastPublished as u8, &id)
                    .await
                    .with_context("list_broadcasts", "get published broadcast")?
                    .unwrap_or_else(|| panic!("Id {} not found in the store", id)),
            );
        }
        let now = timestamp_now();
        Ok(published
            .iter()
            .filter(|b| {
                b.exported.contains(&id)
                    && match b.expiration {
                        Some(expiration) => expiration > now,
                        None => true,
                    }
            })
            .map(|b| b.into())
            .collect())
    }

    /// Updates all active broadcasts that contain the specified node by
    /// fetching the latest version.
    pub async fn update_broadcasts(&mut self, id: Id) -> Result<()> {
        let mut published: HashMap<Id, OwnedBroadcast> = HashMap::new();
        let keys: Vec<Id> = self
            .store
            .keys_typed(Slot::BroadcastPublished as u8)
            .await
            .with_context("update_broadcasts", "get published broadcast keys")?;
        for id in keys {
            let broadcast = self
                .store
                .get_typed(Slot::BroadcastPublished as u8, &id)
                .await
                .with_context("update_broadcasts", "get published broadcast")?
                .unwrap_or_else(|| panic!("Id {} could not be found in the store", id));
            published.insert(id, broadcast);
        }
        let descendants = self.descendants_until_links(id).await?;
        let now = timestamp_now();
        let updated: HashMap<_, _> = {
            let relevant_broadcasts: Vec<_> = published
                .iter()
                .filter(|(_id, b)| {
                    !b.exported.is_disjoint(&descendants)
                        && match b.expiration {
                            Some(expiration) => expiration > now,
                            None => true,
                        }
                })
                .collect();
            let updates = relevant_broadcasts
                .iter()
                .map(|(_id, b)| broadcast::push(self, id, Some(*b)));
            let updated = try_join_all(updates).await?;
            relevant_broadcasts
                .iter()
                .zip(updated)
                .map(|((id, _), b)| (**id, b))
                .collect()
        };
        for (id, b) in published.into_iter() {
            if let Some(broadcast) = updated.get(&id) {
                self.store
                    .insert_typed(Slot::BroadcastPublished as u8, id, broadcast)
                    .with_context("update_broadcasts", "insert updated broadcast")?;
            } else if b.expiration.is_some() && b.expiration.unwrap() <= now {
                self.store
                    .remove_typed(Slot::BroadcastPublished as u8, id)
                    .with_context("update_broadcasts", "remove expired broadcast")?;
            }
        }
        Ok(())
    }

    /// Returns all (textually similar) matches for the specified search term.
    pub async fn search(&self, term: &str) -> Result<Vec<Overlap>> {
        let grams = index_text(term);
        let mut overlaps: Vec<Overlap> = self
            .find(grams.as_slice(), SearchMode::AsymmetricBasedOnSourceOnly)
            .await?
            .into_iter()
            .filter(|o| o.score() >= 0.3)
            .collect();
        overlaps.sort();
        Ok(overlaps)
    }

    /// Returns all nodes with content that overlaps with the specified node.
    pub async fn overlaps(&self, id: Id) -> Result<Vec<Overlap>> {
        self.store
            .get_typed(Slot::Overlaps as u8, &id)
            .await
            .ok_or_invalid(id, "overlaps", "get overlaps in store")
    }

    async fn find(&self, grams: &[u32], mode: SearchMode) -> Result<Vec<Overlap>> {
        let grams = if let SearchMode::AsymmetricBasedOnSourceOnly = mode {
            let dropped_at_each_end = min((grams.len() - 1) / 2, 3);
            &grams[dropped_at_each_end..grams.len() - dropped_at_each_end]
        } else {
            grams
        };
        if !grams.iter().any(|g| *g != 0) {
            return Ok(vec![]);
        }
        let source_count = grams.len() as u32;
        let mut source_occurs = HashMap::new();
        for gram in grams {
            *source_occurs.entry(gram).or_insert(0_u32) += 1;
        }

        let mut overlaps = Vec::new();
        let mut intersections = HashMap::new();
        for (gram, source_occurs) in source_occurs.iter() {
            let matches = self
                .store
                .get_typed::<_, HashMap<Id, Occurrences>>(Slot::Grams as u8, &gram)
                .await
                .with_context("find", "get n-grams")?;
            if let Some(matches) = matches {
                for (id, Occurrences(match_occurs)) in matches.into_iter() {
                    let intersection = min(*source_occurs, match_occurs);
                    if intersection > 0 {
                        *intersections.entry(id).or_insert(0_u32) += intersection;
                    }
                }
            }
        }
        for (id, intersection) in intersections {
            let match_count = match mode {
                SearchMode::SymmetricOverlap => self
                    .store
                    .get_typed::<_, u32>(Slot::Count as u8, &id)
                    .await
                    .with_context("find", "get n-gram count")?
                    .unwrap_or_else(|| panic!("No count for id {} was found in the store", id)),
                SearchMode::AsymmetricBasedOnSourceOnly => source_count,
            };
            overlaps.push(Overlap::new(id, source_count, match_count, intersection));
        }
        Ok(overlaps)
    }

    async fn update_parent_index(
        &mut self,
        id: Id,
        before: &mut Index,
        after: &mut Index,
    ) -> Result<()> {
        let mut stack: Vec<Parent> = self
            .store
            .get_unremoved_typed::<_, Parents>(Slot::Parents as u8, &id)
            .await
            .ok_or_invalid(id, "update_parent_index", "get parents")?
            .into_iter()
            .collect();
        while let Some(Parent { id, .. }) = stack.pop() {
            if before.all.contains_key(&id) && after.all.contains_key(&id) {
                continue;
            }
            before.index(self, id).await?;
            after.index(self, id).await?;

            let diff = Diff::new(&before.blocks, &after.blocks);
            let node =
                self.get(id)
                    .await
                    .ok_or_invalid(id, "update_parent_index", "get parent node")?;
            if self.is_block(&node).await? {
                self.store_count(&after.blocks)?;
                self.store_grams(&diff).await?;
            } else {
                let parents: Vec<Parent> = self
                    .store
                    .get_unremoved_typed::<_, Parents>(Slot::Parents as u8, &id)
                    .await
                    .ok_or_invalid(id, "update_parent_index", "get parents of parent")?
                    .into_iter()
                    .collect();
                stack.extend(parents);
            }

            self.store_overlaps(&after.all, &diff.ids()).await?;
        }
        Ok(())
    }

    async fn store_grams(&mut self, diff: &Diff) -> Result<()> {
        let store = &mut self.store;
        for (gram, occurrences) in diff.0.iter() {
            let mut stored_gram = store
                .get_typed::<_, HashMap<Id, Occurrences>>(Slot::Grams as u8, &gram)
                .await
                .with_context("store_grams", "get n-grams")?
                .unwrap_or_default();
            stored_gram.extend(occurrences);
            store
                .insert_typed(Slot::Grams as u8, gram, stored_gram)
                .with_context("store_grams", "insert n-grams")?;
        }
        Ok(())
    }

    async fn store_overlaps(&mut self, grams: &GramsById, ids: &HashSet<Id>) -> Result<()> {
        let empty_grams = vec![];
        for id in ids.iter().copied() {
            let grams = grams.get(&id).unwrap_or(&empty_grams);
            let before = self.overlaps(id).await.unwrap_or_default();
            let mut after: Vec<Overlap> = self
                .find(grams, SearchMode::SymmetricOverlap)
                .await?
                .into_iter()
                .filter(|o| o.id != id && o.score() > 0.5)
                .collect();

            let before_set: HashSet<&Overlap> = before.iter().collect();
            let after_set: HashSet<&Overlap> = after.iter().collect();
            let removed = before_set.difference(&after_set);
            let added = after_set.difference(&before_set);

            for o in removed {
                let o_rev = o.reverse(id);
                let overlaps_rev: Vec<Overlap> = self
                    .overlaps(o.id)
                    .await?
                    .into_iter()
                    .filter(|o| *o != o_rev)
                    .collect();
                self.store
                    .insert_typed(Slot::Overlaps as u8, o.id, overlaps_rev)
                    .with_context("store_overlaps", "insert removed reverse overlaps")?;
            }
            for o in added {
                let o_rev = o.reverse(id);
                let mut overlaps_rev = self.overlaps(o.id).await.unwrap_or_default();
                overlaps_rev.push(o_rev);
                overlaps_rev.sort();
                self.store
                    .insert_typed(Slot::Overlaps as u8, o.id, overlaps_rev)
                    .with_context("store_overlaps", "insert added reverse overlaps")?;
            }

            after.sort();
            self.store
                .insert_typed(Slot::Overlaps as u8, id, after)
                .with_context("store_overlaps", "insert overlaps")?;
        }
        Ok(())
    }

    fn store_count(&mut self, grams: &GramsById) -> Result<()> {
        for (id, grams) in grams.iter() {
            self.store
                .insert_typed(Slot::Count as u8, id, grams.len())
                .with_context("store_count", "insert n-gram count")?;
        }
        Ok(())
    }

    /// Adds the specified node to the DB and returns its associated id.
    ///
    /// If the added node contains children, these children will be added
    /// recursively (if they are "eager" and have not already been added to the
    /// DB).
    pub async fn add(&mut self, node: Node) -> Result<Id> {
        let id = self.add_unindexed(node).await?;
        let after = Index::from(self, id).await?;
        let diff = Diff::new(&HashMap::new(), &after.blocks);
        self.store_count(&after.blocks)?;
        self.store_grams(&diff).await?;
        self.store_overlaps(&after.all, &diff.ids()).await?;
        Ok(id)
    }

    /// Swaps out the node with the specified id with a replacement node.
    ///
    /// This is the only operation that directly mutates nodes that have already
    /// been added to the DB. All other "edit" operations
    /// ([`DbSnapshot::update()`], [`DbSnapshot::remove()`],
    /// [`DbSnapshot::replace()`], [`DbSnapshot::insert()`], and
    /// [`DbSnapshot::push()`]) are implemented using `swap` and act as
    /// specialized versions of it.
    ///
    /// Obsolete nodes are automatically removed ("moved to trash") and their
    /// parents cleared at the end of a successful swap. Nodes are considered
    /// obsolete if they or their children were descendants of the existing
    /// node, but not of the replacement node (and would be orphaned after the
    /// swap).
    ///
    /// Since orphaned nodes are only moved to the trash and not purged from the
    /// DB, they still exist in the DB (until the next merge) and can be
    /// accessed directly using their id using [`DbSnapshot::get_in_trash`] (and
    /// restored using [`DbSnapshot::restore`], if desired). However, their
    /// parents have been removed and so it is only possible to traverse a tree
    /// of orphaned children downwards, never upwards (unless the ids of their
    /// previous parents are known through other means).
    pub async fn swap(&mut self, id: Id, replacement: Node) -> Result<()> {
        let mut before = Index::from(self, id).await?;
        self.swap_unindexed(id, replacement).await?;
        let mut after = Index::from(self, id).await?;
        let diff = Diff::new(&before.blocks, &after.blocks);
        self.store_count(&after.blocks)?;
        self.store_grams(&diff).await?;
        self.store_overlaps(&after.all, &diff.ids()).await?;
        if !Diff::new(&before.all, &after.all).0.is_empty() {
            self.update_parent_index(id, &mut before, &mut after)
                .await?;
        }
        let ids_before: HashSet<Id> = before.all.keys().copied().collect();
        let ids_after: HashSet<Id> = after.all.keys().copied().collect();
        for removed in ids_before.difference(&ids_after) {
            self.store
                .remove_typed(Slot::Count as u8, removed)
                .with_context("swap", "remove count of removed node")?;
            self.store
                .remove_typed(Slot::Overlaps as u8, removed)
                .with_context("swap", "remove overlaps of removed node")?;
        }
        Ok(())
    }

    /// Restores the node with the specified id if it was moved to the trash.
    ///
    /// If the node was moved to the trash (and has not been purged by a merge),
    /// the node itself is restored together with all its children. All the
    /// parent relationships between restored nodes and their children (both
    /// existing and restored) are re-added.
    ///
    /// If the node exists, but is not in the trash,
    /// [`RestoredNode::NoNeedToRestoreNode`] is returned and neither the node
    /// nor any of its children are modified.
    ///
    /// Trying to restore a node that is not found in the DB (neither "normally"
    /// nor in the trash, because it was already purged by a merge, for example)
    /// will result in a [`Error::IdNotFound`].
    pub async fn restore(&mut self, id: Id) -> Result<RestoredNode> {
        let restored = self.restore_unindexed(id).await?;
        if let RestoredNode::Restored(_) = &restored {
            let mut before = Index::new();
            let mut after = Index::from(self, id).await?;
            let diff = Diff::new(&before.blocks, &after.blocks);
            self.store_count(&after.blocks)?;
            self.store_grams(&diff).await?;
            self.store_overlaps(&after.all, &diff.ids()).await?;
            self.update_parent_index(id, &mut before, &mut after)
                .await?;
        }
        Ok(restored)
    }

    /// Commits the current transaction, thereby persisting all of its changes.
    pub async fn commit(self) -> Result<()> {
        self.store.commit().await.with_context("commit", "")
    }

    /// Copies the node with the specified id and all of its descendants into a
    /// byte vec, returning the bytes and the ids of all exported nodes.
    pub async fn export(&self, id: Id) -> Result<(Vec<u8>, HashSet<Id>)> {
        self.export_since(id, 0).await
    }

    /// Copies the node with the specified id and all of its descendants into a
    /// byte vec.
    ///
    /// Returns the exported bytes and the ids of all exported nodes, exporting
    /// only the nodes modified after the specified time.
    pub async fn export_since(&self, id: Id, timestamp: u64) -> Result<(Vec<u8>, HashSet<Id>)> {
        let mut nodes = BTreeMap::new();
        let mut stack = vec![id];
        while let Some(id) = stack.pop() {
            if nodes.contains_key(&id) {
                continue;
            }
            let node = self
                .get(id)
                .await
                .ok_or_invalid(id, "export_since", "get exported node")?;
            let parents = self.parents(id).await?;
            let last_version = *self.versions(id).await?.last().unwrap();
            stack.extend(node.children().into_iter().map(|c| c.id().unwrap()));
            nodes.insert(id, (node, parents, last_version));
        }
        let ids: HashSet<Id> = nodes.keys().copied().collect();
        let storage = MemoryStorage::new();
        let store = KvStore::open(storage)
            .await
            .with_context("export_since", "open")?;
        let mut transaction = store.current().await;
        for (id, (node, parents, last_version)) in nodes.into_iter() {
            if last_version.timestamp > timestamp {
                transaction
                    .insert_typed(Slot::Node as u8, &id, node)
                    .with_context("export_since", "insert node")?;
                let parents: HashSet<Parent> = parents
                    .into_iter()
                    .filter(|p| ids.contains(&p.id))
                    .collect();
                transaction
                    .insert_typed(Slot::Parents as u8, &id, parents)
                    .with_context("export_since", "insert parents")?;
            }
        }
        // If there is no 'root' node (meaning no node with nil UUID) in the
        // exported nodes we create one with the exported root id (meaning the
        // top-most node of the exported node tree) as its only child:
        let root_id = Id::root();
        if !ids.contains(&root_id) {
            let mut parents = HashSet::new();
            parents.insert(Parent::new(root_id, 0));
            transaction
                .insert_typed(Slot::Parents as u8, &id, parents)
                .with_context("export_since", "insert root as parent")?;
            let node = Node::list(Layout::Page, vec![id]);
            transaction
                .insert_typed(Slot::Node as u8, &root_id, node)
                .with_context("export_since", "insert root as node")?;
            let parents: Parents = HashSet::new();
            transaction
                .insert_typed(Slot::Parents as u8, &root_id, parents)
                .with_context("export_since", "insert parents of root")?;
        }
        transaction
            .commit()
            .await
            .with_context("export_since", "commit")?;
        Ok((
            store
                .into_storage()
                .with_context("export_since", "into_storage")?
                .into_bytes(),
            ids,
        ))
    }

    /// Reads the specified bytes into the store, appending them to the existing
    /// contents of the DB.
    ///
    /// This is a low-level function that can be used to import a tree of nodes
    /// into the DB while ensuring that all of the imported node ids are
    /// transformed in such a way that they do not clash with any of the
    /// existing ids. A "namespace" id is used for this purposed, which will be
    /// applied as an `XOR` mask to all imported ids.
    ///
    /// For example, if nodes with the hypothetical ids `1`, `2` and `3` were
    /// broadcast by a broadcast with id `X` and a malicious broadcaster decided
    /// to broadcast the same node ids with different node content as a
    /// broadcast with id `Y` then, depending on which of the broadcasts `X` or
    /// `Y` would be fetched first, `X` would overwrite `Y` or vice versa. To
    /// make sure that two broadcasts with different ids are guaranteed to never
    /// clash, a random id is generated by the DB for each broadcast it imports,
    /// with this random uuid acting as the "namespace" for all ids of the
    /// broadcast.
    pub async fn import(&mut self, bytes: &[u8], namespace: Id) -> Result<()> {
        let storage = MemoryStorage::from(bytes);
        let store = KvStore::open(storage)
            .await
            .with_context("import", "open storage")?;
        let imported = store.current().await;
        let mut before = Index::new();
        let ids_exported: Vec<Id> = imported
            .keys_typed(Slot::Node as u8)
            .await
            .with_context("import", "get keys of node slot")?;
        let ids_imported: Vec<Id> = ids_exported
            .iter()
            .copied()
            .map(|id| xor_ids(id, namespace))
            .collect();
        for id in ids_imported.iter().copied() {
            let versions = self
                .store
                .versions_typed(Slot::Node as u8, &id)
                .await
                .with_context("import", "get versions of node")?;
            if !versions.is_empty() {
                before.index(self, id).await?;
            }
        }

        for id in ids_exported.iter().copied() {
            // All imported ids are XOR'ed with a randomly chosen u128 to ensure
            // that duplicate broadcasts can never overwrite each other but will
            // be mapped to unique ids.
            let node = imported
                .get_typed::<_, Node>(Slot::Node as u8, &id)
                .await
                .with_context("import", "get node from imported store")?
                .unwrap_or_else(|| panic!("Id {} not found in the store", id));
            let (node, children) = node.split();
            let children: Vec<Child> = children
                .into_iter()
                .map(|c| match c {
                    Child::Lazy(id) => Child::Lazy(xor_ids(id, namespace)),
                    Child::Eager(n) => Child::Eager(n),
                })
                .collect();
            let node = node.with(children)?;
            self.store
                .insert_typed(Slot::Node as u8, xor_ids(id, namespace), node)
                .with_context("import", "insert imported node")?;

            let parents = imported
                .get_typed::<_, Parents>(Slot::Parents as u8, &id)
                .await
                .with_context("import", "get parents from imported store")?
                .unwrap_or_else(|| panic!("Parents of id {} not found in the store", id));
            let parents: Parents = parents
                .into_iter()
                .map(|p| Parent::new(xor_ids(p.id, namespace), p.index))
                .collect();
            self.store
                .insert_typed(Slot::Parents as u8, xor_ids(id, namespace), parents)
                .with_context("import", "insert imported parents")?;
        }

        let mut after = Index::new();
        for id in ids_imported.iter().copied() {
            after.index(self, id).await?;
        }
        let diff = Diff::new(&before.blocks, &after.blocks);
        self.store_count(&after.blocks)?;
        self.store_grams(&diff).await?;
        self.store_overlaps(&after.all, &diff.ids()).await?;
        if !Diff::new(&before.all, &after.all).0.is_empty() {
            for id in ids_imported {
                self.update_parent_index(id, &mut before, &mut after)
                    .await?;
            }
        }
        Ok(())
    }

    /// Transforms a (pre-import) id from a broadcast into the id in this DB
    /// that the original id was replaced with during import.
    ///
    /// Ids originating from a broadcast are transformed when they are imported
    /// to avoid overwriting existing nodes in case the same content is
    /// broadcast and imported multiple times. This function can be used to
    /// access nodes with the id that they had in the broadcast before the
    /// import.
    pub async fn namespaced_id(&self, broadcast_id: &BroadcastId, id: Id) -> Result<Id> {
        let subscription = self
            .store
            .get_typed::<_, BroadcastSubscription>(Slot::BroadcastSubscribed as u8, broadcast_id)
            .await
            .with_context("subscribe_to_broadcast", "get subscription")?;
        if let Some(subscription) = subscription {
            Ok(xor_ids(id, subscription.namespace))
        } else {
            Err(Error::BroadcastIdNotFound(*broadcast_id))
        }
    }
}

fn xor_ids(id1: Id, id2: Id) -> Id {
    Id(Uuid::from_u128(id1.0.as_u128() ^ id2.0.as_u128()))
}

enum SearchMode {
    SymmetricOverlap,
    AsymmetricBasedOnSourceOnly,
}

#[derive(Debug)]
struct Index {
    all: GramsById,
    blocks: GramsById,
}

impl Index {
    fn new() -> Self {
        Self {
            all: HashMap::new(),
            blocks: HashMap::new(),
        }
    }

    async fn from<S: Storage>(snapshot: &DbSnapshot<'_, S>, id: Id) -> Result<Self> {
        let mut idx = Self::new();
        idx.index(snapshot, id).await?;
        Ok(idx)
    }

    async fn index<S: Storage>(&mut self, snapshot: &DbSnapshot<'_, S>, id: Id) -> Result<()> {
        let grams_for_cyclic_children = vec![0, 0, 0];
        let index_all = &mut self.all;
        let index_blocks = &mut self.blocks;
        let mut visited_parents = HashSet::new();
        let mut stack = vec![id];
        while let Some(id) = stack.pop() {
            if index_all.contains_key(&id) {
                continue;
            }

            let node = snapshot
                .get(id)
                .await
                .ok_or_invalid(id, "index", "get node")?;
            let (node, children) = node.split();
            let mut indexed_children = Vec::with_capacity(children.len());
            let mut missing_children = HashSet::new();
            for child in children.iter() {
                let child_id = child.id()?;
                if let Some(grams) = index_all.get(&child_id) {
                    indexed_children.push(grams);
                } else if visited_parents.contains(&child_id) {
                    // to avoid infinite loops in cyclic structures
                    indexed_children.push(&grams_for_cyclic_children);
                } else {
                    missing_children.insert(child_id);
                }
            }
            if indexed_children.len() < children.len() {
                visited_parents.insert(id);
                stack.push(id);
                stack.extend(missing_children);
            } else {
                match node {
                    Node::Text(l) => {
                        let grams = index_text(l.as_str());
                        index_all.insert(id, grams);
                    }
                    Node::List(Layout::Chain, _) => {
                        // N-gram sequences are combined by overlaying the last
                        // 3 n-grams of each child with the first 3 n-grams of
                        // the next child. Here is an example for the n-gram
                        // sequence combination of the three strings "", "ab"
                        // and "c" (with each u32 n-gram in this example written
                        // as a vector of 4 chars in big-endian order):
                        //
                        // "":    [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0] +
                        // "ab":  [0, 0, 0, a], [0, 0, a, b], [0, a, b, 0], [a, b, 0, 0], [b, 0, 0, 0] +
                        // "c":                               [0, 0, 0, c], [0, 0, c, 0], [0, c, 0, 0], [c, 0, 0, 0]
                        // =========================================================================================
                        // "abc": [0, 0, 0, a], [0, 0, a, b], [0, a, b, c], [a, b, c, 0], [b, c, 0, 0], [c, 0, 0, 0]

                        let mut acc = vec![0, 0, 0];
                        for grams in indexed_children.into_iter() {
                            let acc_len = acc.len();
                            for i in 0..3 {
                                *acc.get_mut(acc_len - 3 + i).unwrap() |= grams.get(i).unwrap();
                            }
                            acc.extend(grams.iter().skip(3));
                        }
                        index_all.insert(id, acc);
                    }
                    Node::List(Layout::Page, _) | Node::Styled(Styles::Block(_), _) => {
                        for (child, child_grams) in children.iter().zip(indexed_children.iter()) {
                            index_blocks.insert(child.id()?, (*child_grams).clone());
                        }
                        index_all.insert(id, vec![0, 0, 0]);
                    }
                    Node::Styled(Styles::Span(_), _) => {
                        let child_grams = indexed_children.first().copied().unwrap().clone();
                        index_all.insert(id, child_grams);
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Diff(HashMap<u32, HashMap<Id, Occurrences>>);

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
struct Occurrences(u32);

impl Diff {
    fn new(before: &GramsById, after: &GramsById) -> Self {
        let mut diff = HashMap::new();
        let all_ids: HashSet<&Id> = before.keys().chain(after.keys()).collect();
        for id in all_ids.into_iter() {
            let before = before.get(id);
            let after = after.get(id);
            match (before, after) {
                (Some(grams_before), Some(grams_after)) => {
                    let mut after: HashMap<&u32, u32> = HashMap::new();
                    for gram in grams_after {
                        *(after.entry(gram).or_insert(0)) += 1;
                    }
                    for gram in grams_before {
                        if !after.contains_key(gram) {
                            diff.entry(*gram)
                                .or_insert_with(HashMap::new)
                                .insert(*id, Occurrences(0));
                        }
                    }
                    for (gram, occurrences) in after.into_iter() {
                        diff.entry(*gram)
                            .or_default()
                            .insert(*id, Occurrences(occurrences));
                    }
                }
                (None, Some(grams)) => {
                    for gram in grams {
                        let mut occurrences = diff
                            .entry(*gram)
                            .or_default()
                            .entry(*id)
                            .or_insert(Occurrences(0));
                        occurrences.0 += 1;
                    }
                }
                (Some(grams), None) => {
                    for gram in grams {
                        diff.entry(*gram).or_default().insert(*id, Occurrences(0));
                    }
                }
                (None, None) => {}
            }
        }
        Self(diff)
    }

    fn ids(&self) -> HashSet<Id> {
        let mut ids = HashSet::new();
        for matches in self.0.values() {
            for id in matches.keys() {
                ids.insert(*id);
            }
        }
        ids
    }
}

fn index_text(s: &str) -> Vec<u32> {
    let bytes = s.as_bytes();
    let mut b = Vec::with_capacity(bytes.len() + 6);
    b.extend_from_slice(&[0, 0, 0]);
    b.extend_from_slice(bytes);
    b.extend_from_slice(&[0, 0, 0]);
    b.windows(4)
        .map(|b| u32::from_be_bytes(b.try_into().unwrap()))
        .collect()
}
