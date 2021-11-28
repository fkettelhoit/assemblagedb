use crate::{
    data::{BlockStyle, Child, Id, Layout, Node, Parent, Parents, Styles},
    AsDbErrorWithContext, AsIdNotFoundErrorWithContext, Db, DbSnapshot, RestoredNode, Result, Slot,
    TypedKvSnapshot,
};
use assemblage_kv::{self, storage::Storage, KvStore, Version};
use async_recursion::async_recursion;
use std::collections::{HashMap, HashSet};

/// The direction of the sibling relative to the node.
#[derive(Debug, Copy, Clone)]
enum SiblingDirection {
    /// The sibling occurs before (usually displayed vertically above) the node.
    Before,
    /// The sibling occurs after (usually displayed vertically below) the node.
    After,
}

impl<S: Storage> Db<S> {
    /// Opens and reads a DB from storage or creates it if none exists.
    ///
    /// If the storage is empty, a new empty list with page layout will be
    /// automatically added as the root node of the DB.
    pub async fn open(storage: S) -> Result<Self> {
        let db = Self {
            store: KvStore::open(storage).await.with_context("open", "")?,
        };
        if db.store.is_empty().await {
            let root = Node::List(Layout::Page, vec![]);
            let id = Id::root();
            let mut t = db.current().await;
            t.store
                .insert_typed(Slot::Node as u8, &id, root)
                .with_context("open", "insert root node")?;

            let v: Parents = HashSet::new();
            t.store
                .insert_typed(Slot::Parents as u8, &id, v)
                .with_context("open", "insert parents of root node")?;
            t.commit().await?;
        }
        Ok(db)
    }

    /// Returns a transactional snapshot of the DB at the current point in time.
    ///
    /// A transaction is a snapshot of the DB at the point in time when the
    /// transaction was started. Nodes can be read, added and modified inside
    /// the transaction, but writes from other transactions are isolated from
    /// the current transaction. Reads are cached for each transaction, so that
    /// multiple reads of the same node(s) only have to access storage once.
    /// Writes are only persisted at the end of a successful transaction, until
    /// then all writes simply mutate in-memory data structures.
    ///
    /// The [tx! macro](tx) provides a less verbose alternative to this method.
    pub async fn current(&self) -> DbSnapshot<'_, S> {
        DbSnapshot {
            store: self.store.current().await,
        }
    }

    /// Returns the name of the storage.
    pub fn name(&self) -> &str {
        self.store.name()
    }

    /// Returns the size of the DB's storage in bytes.
    pub async fn size(&self) -> Result<u64> {
        Ok(self.store.len().await)
    }

    /// Merges and compacts the DB by removing old versions.
    ///
    /// Merging a store reclaims space by removing all versions that were
    /// superseded by newer writes with the same id. As a side effect, a merge
    /// "empties the trash" and ensures that removed values cannot be restored
    /// anymore.
    pub async fn merge(&mut self) -> Result<()> {
        Ok(self.store.merge().await.with_context("merge", "")?)
    }

    /// Consumes the DB and returns its underlying storage.
    pub fn into_storage(self) -> Result<S> {
        self.store.into_storage().with_context("into_storage", "")
    }
}

impl<S: Storage> DbSnapshot<'_, S> {
    /// Returns the timestamp of the last write to the store (in milliseconds
    /// since the Unix epoch).
    pub async fn last_updated(&self) -> Result<Option<u64>> {
        Ok(self
            .store
            .last_updated()
            .await
            .with_context("last_updated", "")?)
    }

    /// Returns the latest version of the node associated with the specified id
    /// or `None` if the node could not be found in the DB.
    pub async fn get(&self, id: Id) -> Result<Option<Node>> {
        self.store
            .get_typed::<_, Node>(Slot::Node as u8, &id)
            .await
            .with_context("get", &format!("id {}", id))
    }

    /// Returns the last unremoved version of the node associated with the
    /// specified id.
    ///
    /// If the node was removed, this method returns the last version "from the
    /// trash" (without restoring it, see [`DbSnapshot::restore`]). If the node
    /// exists in the DB, this method acts exactly like [`DbSnapshot::get`].
    pub async fn get_in_trash(&self, id: Id) -> Result<Option<Node>> {
        self.store
            .get_unremoved_typed::<_, Node>(Slot::Node as u8, &id)
            .await
            .with_context("get", &format!("id {}", id))
    }

    /// Returns the latest version of the parents of the node with the specified
    /// id or an [`crate::Error::IdNotFound`] error if the id could not be found
    /// in the DB.
    pub async fn parents(&self, id: Id) -> Result<Parents> {
        self.store
            .get_typed::<_, Parents>(Slot::Parents as u8, &id)
            .await
            .ok_or_invalid(id, "parents", "get parents by id")
    }

    /// Returns the latest version of the siblings that occur directly _before_
    /// the node with the specified id.
    ///
    /// Siblings which are blank or empty are skipped.
    ///
    /// For example, consider a node `N` that is contained in 2 lists (the
    /// layouts of the lists don't matter, so we'll just write `[A, B]` for a
    /// list containg first `A` and then `B`):
    ///
    ///   1. `[A, B, N, C, D]`
    ///   2. `[X, [N, Y], Z]`
    ///
    /// In this example, the siblings of `N` in the first list would be `B`
    /// (because it occurs directly before `N`) and `C` (because it occurs
    /// directly afterwards). The siblings of `N` in the second list would be
    /// `X` (because the inner list `[N, Y]` ends without a sibling before `N`,
    /// so all parents of the inner lists are checked for siblings and `X` is
    /// found) and `Y` (because it occurs directly afterwards).
    ///
    /// If, however, `B` had been blank or empty (a list or styled node without
    /// children) in the above example, then `B` would have been skipped and `A`
    /// would have been returned as one of the siblings instead.
    pub async fn before(&self, id: Id) -> Result<HashSet<Id>> {
        self.adjacent(id, SiblingDirection::Before).await
    }

    /// Returns the latest version of the siblings that occur directly _after_
    /// the node with the specified id.
    ///
    /// Siblings which are blank or empty are skipped.
    ///
    /// For example, consider a node `N` that is contained in 2 lists (the
    /// layouts of the lists don't matter, so we'll just write `[A, B]` for a
    /// list containg first `A` and then `B`):
    ///
    ///   1. `[A, B, N, C, D]`
    ///   2. `[X, [N, Y], Z]`
    ///
    /// In this example, the siblings of `N` in the first list would be `B`
    /// (because it occurs directly before `N`) and `C` (because it occurs
    /// directly afterwards). The siblings of `N` in the second list would be
    /// `X` (because the inner list `[N, Y]` ends without a sibling before `N`,
    /// so all parents of the inner lists are checked for siblings and `X` is
    /// found) and `Y` (because it occurs directly afterwards).
    ///
    /// If, however, `B` had been blank or empty (a list or styled node without
    /// children) in the above example, then `B` would have been skipped and `A`
    /// would have been returned as one of the siblings instead.
    pub async fn after(&self, id: Id) -> Result<HashSet<Id>> {
        self.adjacent(id, SiblingDirection::After).await
    }

    #[async_recursion(?Send)]
    async fn adjacent(&self, id: Id, direction: SiblingDirection) -> Result<HashSet<Id>> {
        if self.is_blank(id).await? {
            return Ok(HashSet::new());
        }
        let node = self
            .get(id)
            .await
            .ok_or_invalid(id, "adjacent", "get main node")?;
        let mut siblings = HashSet::new();

        // We need to check the children of the parent of the current node if
        // they are children. But only those children before or after the
        // current parent index can be siblings in the relevant direction, so we
        // restrict our search to these candidates.
        let relevant_children = |id, index, children: Vec<Child>| {
            let children: Vec<(Id, usize, Child)> = children
                .into_iter()
                .enumerate()
                .map(|(index, child)| (id, index, child))
                .collect();

            // We want to use the children as a stack (and pop from it), so we
            // reverse the ::After direction and not ::Before.
            match direction {
                SiblingDirection::Before => {
                    if children.len() >= index {
                        children[..index].to_vec()
                    } else {
                        children.to_vec()
                    }
                }
                SiblingDirection::After => {
                    if children.len() > index + 1 {
                        children[index + 1..].to_vec().into_iter().rev().collect()
                    } else {
                        vec![]
                    }
                }
            }
        };

        // To find the siblings we need to check all parents and see if they
        // contain other children before or after.
        for parent in self.parents(id).await? {
            let (parent_node, children) = self
                .get(parent.id)
                .await
                .ok_or_invalid(id, "adjacent", "get parent of main node")?
                .split();
            let mut candidates = relevant_children(parent.id, parent.index as usize, children);
            let mut visited: HashSet<Id> = HashSet::new();
            let mut sibling = None;
            while let Some((parent_id, index, Child::Lazy(id))) = candidates.pop() {
                let parent_node =
                    self.get(parent_id)
                        .await
                        .ok_or_invalid(id, "adjacent", "get parent node")?;
                let child =
                    self.get(id)
                        .await
                        .ok_or_invalid(id, "adjacent", "get child of parent")?;

                // We have found a sibling if the child is displayed as:
                //   - a link (if the parent is a span and the child a block)
                //   - or the sibling itself is an atom.
                //
                // In all other cases it's a sequence that we need to check
                // recursively for all children.
                //
                // If the child is blank or an aside, it will be skipped.
                let is_link = self.is_link(&child, &parent_node).await?;
                let is_aside = match &child {
                    Node::Styled(Styles::Block(styles), _) => styles.contains(&BlockStyle::Aside),
                    _ => false,
                };
                if !is_aside && !self.is_blank(id).await? && (is_link || child.is_atom()) {
                    sibling.replace(id);
                    break;
                } else if !visited.contains(&id) {
                    // The above check + this are necessary to handle cycles:
                    visited.insert(id);

                    let (_, children) = child.split();
                    // Again, we need to reverse ::After instead of ::Before,
                    // since we are using it as a stack and pop from the end:
                    let children = match direction {
                        SiblingDirection::Before => children,
                        SiblingDirection::After => children.to_vec().into_iter().rev().collect(),
                    };
                    // Now treat the children as candidates for siblings, unless
                    // they are styled as an aside.
                    //
                    // We want the index of the top level sibling, so we reuse
                    // the current parent id + index for all descendants:
                    if !is_aside {
                        candidates.extend(children.into_iter().map(|child| (id, index, child)));
                    }
                }
            }

            // If we found a sibling for the parent, great, we're done for this
            // parent. Otherwise, we need to check the parents of the current
            // parent for siblings, but only if the possible siblings would be
            // "visible on the same level", which means only if the current node
            // is not displayed as a linked block. This is true if the node
            // itself is a span in which case it will never be displayed as a
            // (block) link, or alternatively if both the node and the parent
            // are blocks, in  which case the node is just included directly in
            // the parent as a block without being shown as a link.
            if let Some(sibling) = sibling {
                siblings.insert(sibling);
            } else if !self.is_link(&node, &parent_node).await? {
                siblings.extend(self.adjacent(parent.id, direction).await?);
            }
        }
        Ok(siblings)
    }

    /// Returns all the versions (with their timestamps) of the specified id in
    /// the DB, ordered from earliest to latest.
    pub async fn versions(&self, id: Id) -> Result<Vec<Version>> {
        Ok(self
            .store
            .versions_typed(Slot::Node as u8, &id)
            .await
            .with_context("versions", &format!("id {}", id))?)
    }
}

impl<S: Storage> DbSnapshot<'_, S> {
    #[async_recursion(?Send)]
    pub(crate) async fn add_unindexed(&mut self, node: Node) -> Result<Id> {
        let id = Id::new();

        let (node, children) = node.split();
        let mut lazy_children = Vec::with_capacity(children.len());
        for (index, child) in children.into_iter().enumerate() {
            let parent = Parent::new(id, index as u32);
            let id = match child {
                Child::Eager(node) => {
                    let id = self.add_unindexed(node).await?;
                    let mut parents = HashSet::new();
                    parents.insert(parent);
                    self.store
                        .insert_typed(Slot::Parents as u8, &id, parents)
                        .with_context("add", "insert parents of eager child")?;
                    id
                }
                Child::Lazy(id) => {
                    self.restore_unindexed(id).await?;
                    // Only get the parents that were not removed:
                    let mut parents = self
                        .store
                        .get_typed::<_, Parents>(Slot::Parents as u8, &id)
                        .await
                        .with_context("add", "get parents of lazy child")?
                        .unwrap_or_else(HashSet::new);
                    if !parents.contains(&parent) {
                        parents.insert(parent);
                        self.store
                            .insert_typed(Slot::Parents as u8, &id, parents)
                            .with_context("add", "insert parents of lazy child")?;
                    }
                    id
                }
            };
            lazy_children.push(Child::Lazy(id));
        }
        let node = node.with(lazy_children)?;
        self.store
            .insert_typed(Slot::Node as u8, &id, node)
            .with_context("add", "insert added node")?;

        let v: Parents = HashSet::new();
        self.store
            .insert_typed(Slot::Parents as u8, &id, v)
            .with_context("add", "insert parents of added node")?;

        Ok(id)
    }

    pub(crate) async fn swap_unindexed(&mut self, id: Id, replacement: Node) -> Result<()> {
        let existing = self
            .store
            .get_unremoved_typed::<_, Node>(Slot::Node as u8, &id)
            .await
            .ok_or_invalid(id, "swap_unindexed", "get existing node")?;

        // if the existing node is a parent node we may need to delete the
        // obsolete parent relationship for each of the children:
        let mut obsolete_parents = HashMap::new();
        for (i, child) in existing.children().iter().enumerate() {
            obsolete_parents
                .entry(child.id()?)
                .or_insert_with(HashSet::new)
                .insert(Parent::new(id, i as u32));
        }

        // All eager children must be inserted into the db. For all lazy and
        // thus already inserted children we delete the obsolete parents. If the
        // replacement node is a parent with the child at the same index as the
        // existing parent, we just add it as a parent, cancelling out one
        // obsolete parent. We still need to consider all obsolete parents
        // though, the existing parent might have contained the child multiple
        // times.
        let (replacement, children) = replacement.split();
        let mut lazy_children = Vec::with_capacity(children.len());
        let mut lazy_child_ids = HashSet::<Id>::with_capacity(children.len());
        for (index, child) in children.into_iter().enumerate() {
            let parent = Parent::new(id, index as u32);
            let id = match child {
                Child::Eager(node) => {
                    let child_id = self.add_unindexed(node).await?;
                    let mut parents = HashSet::new();
                    parents.insert(parent);
                    self.store
                        .insert_typed(Slot::Parents as u8, &child_id, parents)
                        .with_context("swap", "insert parents of eager child")?;
                    child_id
                }
                Child::Lazy(id) => {
                    self.restore_unindexed(id).await?;
                    // Only get the parents that were not removed:
                    let mut parents = self
                        .store
                        .get_typed::<_, Parents>(Slot::Parents as u8, &id)
                        .await
                        .with_context("swap", "get parents of lazy child")?
                        .unwrap_or_else(HashSet::new);
                    if obsolete_parents.contains_key(&id) {
                        for obsolete in obsolete_parents[&id].iter() {
                            parents.remove(obsolete);
                        }
                    }
                    parents.insert(parent);
                    self.store
                        .insert_typed(Slot::Parents as u8, &id, parents)
                        .with_context("swap", "insert parents of lazy child")?;
                    id
                }
            };
            lazy_child_ids.insert(id);
            lazy_children.push(Child::Lazy(id));
        }

        // Now we can figure out which of the children are removed (and were
        // children of the existing parent but not the replacement).
        let mut removed = HashSet::with_capacity(existing.children().len());
        for child in existing.children().into_iter() {
            removed.insert(child.id()?);
        }
        for child in lazy_children.iter() {
            removed.remove(&child.id()?);
        }

        // For these removed children we find the whole subtrees of obsolete
        // children (children without any parents that are not also obsolete)...
        let swapped_id = id;
        let mut obsolete = HashSet::new();
        let mut remaining_children = HashSet::new();
        let mut candidates: Vec<Id> = removed.iter().copied().collect();
        while let Some(id) = candidates.pop() {
            let is_obsolete = self
                .store
                .get_unremoved_typed::<_, Parents>(Slot::Parents as u8, &id)
                .await
                .ok_or_invalid(id, "swap_unindexed", "get parents of obsolete node")?
                .into_iter()
                .all(|p| swapped_id == p.id || obsolete.contains(&p.id));
            if is_obsolete && !lazy_child_ids.contains(&id) {
                // If a parent is newly added to the obsolete set, we need to
                // check all its children, even if they were checked before, to
                // make sure diamond dependencies are properly removed.
                if !obsolete.contains(&id) {
                    let obsolete_node = self
                        .store
                        .get_unremoved_typed::<_, Node>(Slot::Node as u8, &id)
                        .await
                        .ok_or_invalid(id, "swap_unindexed", "get obsolete")?;
                    for child in obsolete_node.children() {
                        candidates.push(child.id()?);
                    }
                    obsolete.insert(id);
                    remaining_children.remove(&id);
                }
            } else {
                remaining_children.insert(id);
            }
        }
        // ...and remove these obsolete children and their subtrees from the
        // store.
        for id in obsolete.iter() {
            self.store
                .remove_typed(Slot::Parents as u8, &id)
                .with_context("swap", "remove parents of obsolete node")?;

            // The node contents should remain accessible if accessed directly
            // by id, so we just remove them without overwriting first.
            self.store
                .remove_typed(Slot::Node as u8, &id)
                .with_context("swap", "remove obsolete node")?;
        }

        // Some nodes might be children of obsolete nodes, but still have other
        // (non-obsolete) parents (so they are not obsolete themselves), in
        // which case we remove only their obsolete parent(s).
        for id in remaining_children {
            let remaining_parent = |p: &Parent| -> bool {
                obsolete_parents.get(&id).map_or(true, |ps| !ps.contains(p))
                    && !obsolete.contains(&p.id)
            };
            let parents: Parents = self
                .store
                .get_unremoved_typed::<_, Parents>(Slot::Parents as u8, &id)
                .await
                .ok_or_invalid(id, "swap_unindexed", "get parents of remaining node child")?
                .into_iter()
                .filter(remaining_parent)
                .collect();
            self.store
                .insert_typed(Slot::Parents as u8, &id, parents)
                .with_context("swap", "insert parents of remaining child")?;
        }

        // We also need to remove all the obsolete parents of the subtrees that
        // we just removed. Obsolete parents are all the parents that have only
        // removed children (+ their supertrees of obsolete parents).
        let mut visited = HashSet::new();
        let mut candidates: Vec<Id> = removed.difference(&obsolete).copied().collect();
        while let Some(id) = candidates.pop() {
            visited.insert(id);
            let parents = self
                .store
                .get_unremoved_typed::<_, Parents>(Slot::Parents as u8, &id)
                .await
                .ok_or_invalid(id, "swap_unindexed", "get parents of removed node")?;
            let parents_len = parents.len();
            let remaining_parents: Vec<Parent> = parents
                .into_iter()
                .filter(|p| !obsolete.contains(&p.id) && p.id != swapped_id)
                .collect();

            if remaining_parents.len() != parents_len && obsolete.contains(&id) {
                self.store
                    .insert_typed(Slot::Parents as u8, &id, remaining_parents)
                    .with_context("swap", "insert remaining parents")?;
            }
            let removed_node = self
                .store
                .get_unremoved_typed::<_, Node>(Slot::Node as u8, &id)
                .await
                .ok_or_invalid(id, "swap_unindexed", "get removed")?;
            for child in removed_node.children() {
                if !visited.contains(&child.id()?) {
                    candidates.push(child.id()?);
                }
            }
        }

        // Now that all the children and their parents are handled, we can
        // finally insert the swapped node with its children.
        let v = replacement.with(lazy_children)?;
        self.store
            .insert_typed(Slot::Node as u8, &id, v)
            .with_context("swap", "insert replacement node")?;

        Ok(())
    }

    #[async_recursion(?Send)]
    pub(crate) async fn restore_unindexed(&mut self, id: Id) -> Result<RestoredNode> {
        let is_removed = self
            .store
            .versions_typed(Slot::Node as u8, &id)
            .await
            .map(|mut versions| versions.pop())
            .ok_or_invalid(id, "restore_unindexed", "get versions")?
            .is_removed;
        if !is_removed {
            return Ok(RestoredNode::NoNeedToRestoreNode);
        }

        let node = self
            .store
            .get_unremoved_typed::<_, Node>(Slot::Node as u8, &id)
            .await
            .ok_or_invalid(id, "restore_unindexed", "get removed node")?;
        self.store
            .insert_typed(Slot::Node as u8, &id, &node)
            .with_context("restore_unindexed", "insert restored node")?;
        self.store
            .insert_typed(Slot::Parents as u8, &id, HashSet::<Parent>::new())
            .with_context("restore_unindexed", "insert empty parents")?;

        for (index, child) in node.children().into_iter().enumerate() {
            let restored_parent = Parent::new(id, index as u32);
            let id = child.id()?;
            let mut parents = match self.restore_unindexed(id).await? {
                RestoredNode::Restored(_) => HashSet::new(),
                RestoredNode::NoNeedToRestoreNode => self
                    .store
                    .get_unremoved_typed::<_, Parents>(Slot::Parents as u8, &id)
                    .await
                    .ok_or_invalid(id, "restore_unindexed", "get parents of restored child")?,
            };
            parents.insert(restored_parent);
            self.store
                .insert_typed(Slot::Parents as u8, &id, parents)
                .with_context("restore_unindexed", "insert restored parents of child")?;
        }
        Ok(RestoredNode::Restored(node))
    }
}
