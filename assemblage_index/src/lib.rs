use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
};

use crate::data::{Parent, Result};
use assemblage_kv::{
    storage::{MemoryStorage, Storage},
    KvStore,
};
use data::{ContentType, Error, Id, Match, NodeTree};
use log::{debug, error, info};
use sequitur::sequitur;

pub mod data;
pub mod sequitur;

pub struct Db<S: Storage, Rng: rand::Rng> {
    kv: KvStore<S>,
    rng: Mutex<Rng>,
}

impl<S: Storage, Rng: rand::Rng> Db<S, Rng> {
    /// Opens and reads a DB from storage or creates it if none exists.
    ///
    /// If the storage is empty, an empty list will be automatically added as the root node.
    pub async fn open(storage: S, rng: Rng) -> Result<Self> {
        info!("\n\n--- Opening DB {}", storage.name());
        let kv = KvStore::open(storage).await?;
        let rng = Mutex::new(rng);
        Ok(Self { kv, rng })
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
    pub async fn current(&self) -> Snapshot<'_, S, Rng> {
        Snapshot {
            kv: self.kv.current().await,
            rng: &self.rng,
        }
    }
}

impl<Rng: rand::Rng> Db<MemoryStorage, Rng> {
    pub async fn build_from(rng: Rng, ty: ContentType, bytes: &[u8]) -> Result<(Id, Self)> {
        debug!("Building DB from {bytes:?}...");
        let storage = MemoryStorage::new();
        let db = Db::open(storage, rng).await?;
        let mut snapshot = db.current().await;
        let id = snapshot.import_bytes(ty, bytes).await?;
        snapshot.commit().await?;
        debug!("DB build was successful!");
        Ok((id, db))
    }
}

#[derive(Clone)]
pub struct Snapshot<'a, S: Storage, Rng: rand::Rng> {
    kv: assemblage_kv::Snapshot<'a, S>,
    rng: &'a Mutex<Rng>,
}

impl<'a, S: Storage, Rng: rand::Rng> Snapshot<'a, S, Rng> {
    pub async fn import<'b, S2: Storage, Rng2: rand::Rng>(
        &mut self,
        other: &Snapshot<'b, S2, Rng2>,
    ) -> Result<()> {
        self.print().await?;
        other.print().await?;
        other.check_consistency().await?;
        debug!("Starting import...");
        let mut terminal_bytes = vec![];
        let content_types = self.get_parents(Id::bottom()).await?;
        for Parent { id, .. } in content_types {
            let bytes = self.get_parents(id).await?;
            for Parent { id, .. } in bytes {
                debug!("own terminal_byte: {id:?} = {:?}", id.as_bytes());
                terminal_bytes.push(id);
            }
        }
        let content_types = other.get_parents(Id::bottom()).await?;
        for Parent { id, .. } in content_types {
            let bytes = other.get_parents(id).await?;
            for Parent { id, .. } in bytes {
                debug!("other terminal_byte: {id:?} = {:?}", id.as_bytes());
                terminal_bytes.push(id);
            }
        }
        terminal_bytes.sort();
        terminal_bytes.dedup();
        debug!("terminal bytes: {terminal_bytes:?}");

        let mut own_ids = HashSet::new();
        let mut other_ids = Vec::new();
        let mut own_contents = HashMap::new();
        let mut other_contents = HashMap::new();
        let mut bytes_of_children = HashMap::new();

        while let Some(id) = terminal_bytes.pop() {
            let own_parents = self.get_parents_if_exists(id).await?;
            let other_parents = other.get_parents_if_exists(id).await?;
            match (own_parents, other_parents) {
                (None, None) => {}
                (Some(_), None) => {}
                (None, Some(other_parents)) => {
                    for Parent { id: parent_id, .. } in other_parents {
                        other_ids.push(parent_id);
                    }
                }
                (Some(own_parents), Some(other_parents)) => {
                    for Parent { id: parent_id, .. } in own_parents {
                        own_ids.insert(parent_id);
                    }
                    for Parent { id: parent_id, .. } in other_parents {
                        other_ids.push(parent_id);
                    }
                }
            }
        }
        other_ids.sort();
        other_ids.dedup();
        debug!("own_ids: {own_ids:?}");
        debug!("other_ids: {other_ids:?}");

        // For each node in the other DB, check if it overlaps with any of the subsequences:
        while let Some(other_id) = other_ids.pop() {
            debug!(">>>>> other_id: {other_id}");

            for &own_id in own_ids.iter() {
                if !own_contents.contains_key(&own_id) {
                    own_contents.insert(own_id, self.get_children(own_id).await?);
                }
            }
            if !other_contents.contains_key(&other_id) {
                other_contents.insert(other_id, other.get_children(other_id).await?);
            }
            debug!("got children for own and other ids");

            // For each own node, get the content and store all subsequences (which must be unique
            // due to digram uniqueness; each subsequence can occur in at most one node in each DB):
            let mut own_subseqs = HashMap::new();
            for (&own_id, own_content) in own_contents.iter() {
                for i in 0..own_content.len() - 1 {
                    for j in i + 2..own_content.len() + 1 {
                        own_subseqs.insert(&own_content[i..j], (own_id, i, j));
                    }
                }
            }

            let other_content = other_contents.get(&other_id).unwrap();
            let mut other_content_updated = None;
            let mut overlap = None;
            for i in 0..other_content.len() - 1 {
                for j in i + 2..other_content.len() + 1 {
                    let other_subseq = &other_content[i..j];
                    let own_subseq = own_subseqs.get(other_subseq);
                    if let Some(&(own_id, own_i, own_j)) = own_subseq {
                        // make sure that all children of the other node have been imported,
                        // otherwise skip for now (and import the children first):
                        if other_content
                            .iter()
                            .any(|id| !id.points_to_byte() && !bytes_of_children.contains_key(id))
                        {
                            break;
                        }
                        debug!("other_subseq: {other_subseq:?}, {i}, {j}");
                        debug!(
                            "own_subseq: {:?}, {own_i}, {own_j}",
                            &own_contents.get(&own_id).unwrap()[own_i..own_j]
                        );
                        overlap = Some((other_subseq, (own_id, own_i, own_j), (other_id, i, j)));
                    } else {
                        break;
                    }
                }
                if overlap.is_some() {
                    break;
                }
            }
            if let Some((subseq, own_subseq, other_subseq)) = overlap {
                let (own_id, own_i, own_j) = own_subseq;
                let (other_id, other_i, other_j) = other_subseq;
                debug!("getting children of own_id {own_id}");
                let own_content = self.get_children(own_id).await?;

                debug!(">> found match for {own_id} and {other_id}: '{subseq:?}'");
                assert_eq!(other_content[other_i..other_j], own_content[own_i..own_j]);

                let subseq_equals_own = own_j - own_i == own_content.len();
                let subseq_equals_other = other_j - other_i == other_content.len();

                // 1. store subseq as a new node (if necessary)
                let subseq_id = if subseq_equals_other {
                    self.insert_children(other_id, subseq)?;
                    self.insert_total_bytes(other_id, subseq, &mut bytes_of_children)
                        .await?;
                    other_id
                } else if subseq_equals_own {
                    own_id
                } else {
                    let subseq_id = self.new_id()?;
                    self.insert_children(subseq_id, subseq)?;
                    self.insert_total_bytes(subseq_id, subseq, &mut bytes_of_children)
                        .await?;
                    subseq_id
                };
                debug!(">> 1. subseq_id: {subseq_id}");

                // 2. store parents of subseq
                if subseq_equals_own && subseq_equals_other {
                    // - change children of own id parents from own id to other id
                    for parent in self.get_parents(own_id).await?.iter() {
                        let mut children_of_own_parent = self.get_children(parent.id).await?;
                        children_of_own_parent[parent.index as usize] = other_id;
                        self.insert_children(parent.id, &children_of_own_parent)?;
                        self.insert_total_bytes(
                            parent.id,
                            &children_of_own_parent,
                            &mut bytes_of_children,
                        )
                        .await?;
                        own_contents.insert(own_id, children_of_own_parent);
                    }
                    // - add all other parents to own parents
                    let own_parents = self.get_parents(own_id).await?;
                    let mut parents = self
                        .get_parents_if_exists(other_id)
                        .await?
                        .unwrap_or_default();
                    parents.extend(own_parents);
                    parents.sort();
                    parents.dedup();
                    self.insert_parents(other_id, &parents)?;
                    debug!(">> 2.a) parents: {parents:?}");
                } else if subseq_equals_other {
                    // - set own id as parent of subseq
                    let mut parents = self
                        .get_parents_if_exists(other_id)
                        .await?
                        .unwrap_or_default();
                    parents.push(Parent::new(own_id, own_i as u32));
                    parents.sort();
                    parents.dedup();
                    self.insert_parents(other_id, &parents)?;
                    debug!(">> 2.b) parents: {parents:?}");
                } else if subseq_equals_own {
                    // - set other id as parent of subseq
                    let mut parents = self.get_parents(own_id).await?;
                    parents.push(Parent::new(other_id, other_i as u32));
                    parents.sort();
                    parents.dedup();
                    self.insert_parents(own_id, &parents)?;
                    debug!(">> 2.c) parents: {parents:?}");
                } else {
                    // - set own id as parent of subseq
                    // - set other id as parent of subseq
                    let mut subseq_parents = vec![
                        Parent::new(own_id, own_i as u32),
                        Parent::new(other_id, other_i as u32),
                    ];
                    subseq_parents.sort();
                    self.insert_parents(subseq_id, &subseq_parents)?;
                    debug!(">> 2.d) parents: {subseq_parents:?}");
                }

                // 3. use subseq as child (use id of subseq instead of subseq directly)
                if !subseq_equals_own {
                    let space_freed = (own_j - own_i) + 1;
                    let mut compressed = Vec::with_capacity(own_content.len() - space_freed);
                    compressed.extend(&own_content[..own_i]);
                    compressed.push(subseq_id);
                    compressed.extend(&own_content[own_j..]);
                    self.insert_children(own_id, &compressed)?;
                    self.insert_total_bytes(own_id, &compressed, &mut bytes_of_children)
                        .await?;
                    debug!(">> 3.a) own {own_id}: {own_content:?} -> {compressed:?}");
                    own_contents.insert(own_id, compressed);

                    // - shift the parent index (to own id) of all children after subseq
                    for (after_subseq, &child_id) in own_content[own_j..].iter().enumerate() {
                        let index_in_own = own_j + after_subseq;
                        let mut parents = self.get_parents(child_id).await?;
                        for parent in parents.iter_mut() {
                            if parent.id == own_id && parent.index == index_in_own as u32 {
                                parent.index -= (subseq.len() - 1) as u32;
                            }
                        }
                        parents.sort();
                        self.insert_parents(child_id, &parents)?;
                    }
                }
                if !subseq_equals_other {
                    let space_freed = (other_j - other_i) + 1;
                    let mut compressed = Vec::with_capacity(other_content.len() - space_freed);
                    compressed.extend(&other_content[..other_i]);
                    compressed.push(subseq_id);
                    compressed.extend(&other_content[other_j..]);
                    self.insert_children(other_id, &compressed)?;
                    self.insert_total_bytes(other_id, &compressed, &mut bytes_of_children)
                        .await?;
                    debug!(">> 3.b) other {other_id}: {other_content:?} -> {compressed:?}");
                    other_content_updated = Some(compressed);
                }

                // 4. fix parents of children of subseq
                if !subseq_equals_own || subseq_equals_other {
                    // - point to subseq instead of own id
                    // - point to subseq id instead of other id
                    for (subseq_i, &child) in subseq.iter().enumerate() {
                        let mut parents_of_subseq_child = self.get_parents(child).await?;
                        for parent in parents_of_subseq_child.iter_mut() {
                            if parent.id == own_id && parent.index == (own_i + subseq_i) as u32 {
                                parent.id = subseq_id;
                                parent.index = subseq_i as u32;
                            }
                            if parent.id == other_id && parent.index == (other_i + subseq_i) as u32
                            {
                                parent.id = subseq_id;
                                parent.index = subseq_i as u32;
                            }
                        }
                        parents_of_subseq_child.sort();
                        parents_of_subseq_child.dedup();
                        self.insert_parents(child, &parents_of_subseq_child)?;
                        debug!(">> 4. parents of {child}: {parents_of_subseq_child:?}");
                    }
                }

                // 5. collect ids for next round
                if subseq_equals_own {
                    for parent in self.get_parents(own_id).await? {
                        if parent.id != other_id {
                            own_ids.insert(parent.id);
                        }
                    }
                } else {
                    own_ids.insert(own_id);
                }
                if subseq_equals_other {
                    other_ids.extend(other.get_parents(other_id).await?.into_iter().map(|p| p.id));
                } else {
                    other_ids.push(other_id);
                }

                // 6. remove own id if it was replaced by other id
                if subseq_equals_own && subseq_equals_other {
                    debug!("Removing own_id {own_id:?}");
                    own_ids.remove(&own_id);
                    own_contents.remove(&own_id);
                    self.remove_children(own_id)?;
                    self.remove_parents(own_id)?;
                }
            } else {
                let mut all_children_previously_inserted = true;
                for (i, &child_id) in other_content.iter().enumerate() {
                    if !child_id.points_to_byte()
                        && self.get_children_if_exists(child_id).await?.is_none()
                    {
                        debug!(">>>> child {child_id} of {other_id} is still missing");
                        all_children_previously_inserted = false;
                        break;
                    }
                    let mut parents = self
                        .get_parents_if_exists(child_id)
                        .await?
                        .unwrap_or_default();
                    parents.push(Parent::new(other_id, i as u32));
                    parents.sort();
                    parents.dedup();
                    self.insert_parents(child_id, &parents)?;
                }
                if all_children_previously_inserted {
                    debug!(">>>> no overlap for {other_id}, can be inserted");
                    let parents = other.get_parents(other_id).await?;
                    if parents.is_empty() {
                        self.insert_parents(other_id, &[])?;
                    } else {
                        other_ids.extend(parents.iter().map(|p| p.id));
                    }
                    self.insert_children(other_id, &other_content)?;
                    self.insert_total_bytes(other_id, &other_content, &mut bytes_of_children)
                        .await?;
                }
            }
            if let Some(updated) = other_content_updated {
                other_contents.insert(other_id, updated);
            }
        }
        debug!("Import was successful!");
        Ok(())
    }

    pub async fn print(&self) -> Result<()> {
        info!("***** DB PRINTOUT FOR '{}' *****", self.kv.name());
        info!("*** NODES: ***");
        for key in self.kv.keys().await? {
            if key[0] == KvKeyPrefix::Children as u8 {
                let id = Id::parse_all(&key[1..])?[0];
                let children = self.get_children(id).await?;
                info!("{id:?} -> {:?}", children);
            }
        }
        info!("*** PARENTS: ***");
        for key in self.kv.keys().await? {
            if key[0] == KvKeyPrefix::Parents as u8 {
                let id = Id::parse_all(&key[1..])?[0];
                let parents = self.get_parents(id).await?;
                info!("{id:?} -> {:?}", parents);
            }
        }
        Ok(())
    }

    pub async fn check_consistency(&self) -> Result<()> {
        for key in self.kv.keys().await? {
            if key[0] == KvKeyPrefix::Children as u8 {
                let id = Id::parse_all(&key[1..])?[0];
                if id.points_to_byte() {
                    continue;
                }
                let children = self.get_children(id).await?;
                for (i, &child_id) in children.iter().enumerate() {
                    let parents = self.get_parents(child_id).await?;
                    let mut found_child = false;
                    for parent in parents {
                        if parent.id == id && parent.index == i as u32 {
                            found_child = true;
                        }
                    }
                    if !found_child {
                        panic!("Child {i} with id {child_id} of {id} is missing!");
                    }
                }
                let bytes = self.get_bytes_if_exists(id).await?;
                if bytes.is_none() {
                    panic!("Bytes for id {id} are missing!");
                }
                info!("{id} -> {:?}", children);
            }
        }
        for key in self.kv.keys().await? {
            if key[0] == KvKeyPrefix::Parents as u8 {
                let id = Id::parse_all(&key[1..])?[0];
                let parents = self.get_parents(id).await?;
                for parent in parents {
                    if parent.id.points_to_byte() {
                        continue;
                    }
                    let children = self.get_children(parent.id).await?;
                    if children.len() <= parent.index as usize
                        || children[parent.index as usize] != id
                    {
                        panic!(
                            "Parent {} does not have child '{id}' at index {}",
                            parent.id, parent.index
                        );
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn add(&mut self, ty: ContentType, bytes: &[u8]) -> Result<Id> {
        debug!("Adding {bytes:?}...");
        let other_storage = MemoryStorage::new();
        let other_kv = KvStore::open(other_storage).await?;
        let mut other_snapshot = Snapshot {
            kv: other_kv.current().await,
            rng: &self.rng,
        };
        let id = other_snapshot.import_bytes(ty, bytes).await?;
        self.import(&other_snapshot).await?;
        debug!("Add was sucessful!");
        Ok(id)
    }

    /// Commits the current transaction, thereby persisting all changes.
    pub async fn commit(self) -> Result<()> {
        Ok(self.kv.commit().await?)
    }

    pub async fn get(&self, id: Id) -> Result<Option<NodeTree>> {
        if let Some(mut ids) = self.get_children_if_exists(id).await? {
            let mut children = HashMap::new();
            let mut parents = HashMap::new();
            let mut bytes = HashMap::new();
            children.insert(id, ids.clone());
            parents.insert(id, self.get_parents(id).await?);
            bytes.insert(id, self.get_bytes(id).await?);
            while let Some(id) = ids.pop() {
                if !children.contains_key(&id) {
                    let (children_of_id, bytes_of_id) = if id.points_to_byte() {
                        (vec![], 1)
                    } else {
                        (self.get_children(id).await?, self.get_bytes(id).await?)
                    };
                    let parents_of_id = self.get_parents(id).await?;
                    ids.extend(children_of_id.iter());
                    children.insert(id, children_of_id);
                    parents.insert(id, parents_of_id);
                    bytes.insert(id, bytes_of_id);
                }
            }
            Ok(Some(NodeTree {
                children,
                parents,
                bytes,
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn search(
        &self,
        ty: ContentType,
        bytes: &[u8],
    ) -> Result<(NodeTree, HashMap<Id, HashMap<Id, Match>>)> {
        let mut snapshot = Snapshot {
            kv: self.kv.clone(),
            rng: self.rng,
        };
        let id = snapshot.add(ty, bytes).await?;
        let nodes = snapshot.get(id).await?.unwrap();
        let similar = snapshot.similar(&nodes).await?;
        Ok((nodes, similar))
    }

    pub async fn similar(&self, tree: &NodeTree) -> Result<HashMap<Id, HashMap<Id, Match>>> {
        // to find the similarities of nodes _inside_ the tree with nodes _outside_ the tree...
        type InsideTree = Id;
        type OutsideTree = Id;
        type BytesInside = u32; // overlapping bytes of a node inside the tree
        type BytesOutside = u32; // overlapping bytes of a node outside the tree

        // 1. find all "borders": nodes inside the tree with parents outside the tree
        let mut borders = Vec::<(InsideTree, OutsideTree, BytesOutside)>::new();
        for (&child_id, parents) in tree.parents.iter() {
            let mut overlaps = HashMap::<OutsideTree, BytesOutside>::new();
            for parent in parents {
                if !tree.children.contains_key(&parent.id) {
                    let overlapping_bytes_in_ancestor = *tree.bytes.get(&child_id).unwrap();
                    borders.push((child_id, parent.id, overlapping_bytes_in_ancestor));
                    *overlaps.entry(parent.id).or_default() += overlapping_bytes_in_ancestor;
                }
            }
        }
        println!("## BORDERS:");
        for x in borders.iter() {
            println!(">> {x:?}");
        }

        // 2. find all (outside) ancestors of these "border parents"
        let mut fully_contained = HashMap::<InsideTree, HashMap<OutsideTree, BytesOutside>>::new();
        {
            let mut borders_and_ancestors = borders.clone();
            while let Some((inside_id, outside_id, bytes)) = borders_and_ancestors.pop() {
                *fully_contained
                    .entry(inside_id)
                    .or_default()
                    .entry(outside_id)
                    .or_default() += bytes;
                let parents = self
                    .get_parents(outside_id)
                    .await?
                    .into_iter()
                    .map(|parent| (inside_id, parent.id, bytes));
                borders_and_ancestors.extend(parents);
            }
        }
        println!("## FULLY CONTAINED:");
        for x in fully_contained.iter() {
            println!(">> {x:?}");
        }

        // 3. (outside) ancestors fully contain the "border children" and their descendants
        {
            let mut borders_and_descendants: Vec<(InsideTree, InsideTree)> = borders
                .iter()
                .map(|(inside_id, _, _)| (*inside_id, *inside_id))
                .collect();
            while let Some((border_id, descendant_id)) = borders_and_descendants.pop() {
                let bytes = *tree.bytes.get(&descendant_id).unwrap();

                if descendant_id != border_id {
                    let mut descendant_fully_contained =
                        fully_contained.remove(&descendant_id).unwrap_or_default();
                    for &ancestor_id in fully_contained.get(&border_id).unwrap().keys() {
                        *descendant_fully_contained.entry(ancestor_id).or_default() += bytes;
                    }
                    fully_contained.insert(descendant_id, descendant_fully_contained);
                }

                if let Some(children) = tree.children.get(&descendant_id) {
                    let children = children
                        .into_iter()
                        .map(|descendant_id| (border_id, *descendant_id));
                    borders_and_descendants.extend(children);
                }
            }
        }
        println!("## FULLY CONTAINED:");
        for x in fully_contained.iter() {
            println!(">> {x:?}");
        }

        // 4. (outside) ancestors partially overlap the (inside) ancestors of "border children"
        let mut partially_overlapping =
            HashMap::<InsideTree, HashMap<OutsideTree, (BytesInside, BytesOutside)>>::new();
        let mut borders_and_ancestors: Vec<(InsideTree, InsideTree, BytesOutside)> = borders
            .into_iter()
            .map(|(inside_id, _, bytes)| (inside_id, inside_id, bytes))
            .collect();
        while let Some((border_id, ancestor_id, descendant_bytes)) = borders_and_ancestors.pop() {
            if let Some(parents) = tree.parents.get(&ancestor_id) {
                for parent in parents {
                    if tree.children.contains_key(&parent.id) {
                        let overlapping_with_inside_ancestor =
                            partially_overlapping.entry(parent.id).or_default();
                        for (outside_id, bytes_outside) in fully_contained.get(&border_id).unwrap()
                        {
                            let (bytes_in_inside_ancestor, bytes_in_outside_ancestor) =
                                overlapping_with_inside_ancestor
                                    .entry(*outside_id)
                                    .or_default();
                            *bytes_in_inside_ancestor += descendant_bytes;
                            *bytes_in_outside_ancestor += bytes_outside;
                        }
                        borders_and_ancestors.push((border_id, parent.id, descendant_bytes));
                    }
                }
            }
        }
        println!("## PARTIALLY OVERLAPPING:");
        for x in partially_overlapping.iter() {
            println!(">> {x:?}");
        }

        // 5. calculate similarity of each node in tree with outside ancestors

        let mut matches = HashMap::<Id, HashMap<Id, Match>>::new();
        for (&id, &bytes) in tree.bytes.iter() {
            if let Some(partially_overlapping) = partially_overlapping.get(&id) {
                for (&outside_id, &(overlapping_bytes_inside, overlapping_bytes_outside)) in
                    partially_overlapping
                {
                    let bytes_outside = self.get_bytes(outside_id).await?;
                    let overlap_in_match = overlapping_bytes_outside as f32 / bytes_outside as f32;
                    let overlap_in_source = overlapping_bytes_inside as f32 / bytes as f32;
                    let m = Match {
                        overlap_in_source,
                        overlap_in_match,
                    };
                    matches.entry(id).or_default().insert(outside_id, m);
                }
            }
            if let Some(fully_contained) = fully_contained.get(&id) {
                for (&outside_id, &overlapping_bytes_outside) in fully_contained {
                    let bytes_outside = self.get_bytes(outside_id).await?;
                    let overlap_in_source = 1.0;
                    let mut overlap_in_match =
                        overlapping_bytes_outside as f32 / bytes_outside as f32;

                    let matches_of_id = matches.entry(id).or_default();
                    if let Some(partial_overlap) = matches_of_id.get_mut(&outside_id) {
                        overlap_in_match += partial_overlap.overlap_in_match;
                    }
                    let m = Match {
                        overlap_in_source,
                        overlap_in_match,
                    };
                    matches_of_id.insert(outside_id, m);
                }
            }
        }
        println!("## MATCHES:");
        for x in matches.iter() {
            println!(">> {x:?}");
        }
        Ok(matches)
    }

    async fn get_children(&self, id: Id) -> Result<Vec<Id>> {
        match self.get_children_if_exists(id).await {
            Ok(Some(parents)) => Ok(parents),
            Ok(None) => Err(Error::ChildIdNotFound(id)),
            Err(e) => Err(e),
        }
    }

    async fn get_children_if_exists(&self, id: Id) -> Result<Option<Vec<Id>>> {
        let id_bytes = id.as_bytes();
        let mut k = Vec::with_capacity(1 + id_bytes.len());
        k.push(KvKeyPrefix::Children as u8);
        k.extend_from_slice(&id_bytes);

        if let Some(child_bytes) = self.kv.get(&k).await? {
            Ok(Some(Id::parse_all(&child_bytes)?))
        } else {
            Ok(None)
        }
    }

    fn insert_children(&mut self, id: Id, children: &[Id]) -> Result<()> {
        let id_bytes = id.as_bytes();
        let mut k = Vec::with_capacity(1 + id_bytes.len());
        k.push(KvKeyPrefix::Children as u8);
        k.extend_from_slice(&id_bytes);

        let mut v: Vec<u8> = Vec::with_capacity(Id::num_bytes() * children.len());
        for id in children {
            v.extend(id.as_bytes());
        }
        Ok(self.kv.insert(k, v)?)
    }

    fn remove_children(&mut self, id: Id) -> Result<()> {
        let id_bytes = id.as_bytes();
        let mut k = Vec::with_capacity(1 + id_bytes.len());
        k.push(KvKeyPrefix::Children as u8);
        k.extend_from_slice(&id_bytes);

        Ok(self.kv.remove(k)?)
    }

    async fn get_parents(&self, id: Id) -> Result<Vec<Parent>> {
        match self.get_parents_if_exists(id).await {
            Ok(Some(parents)) => Ok(parents),
            Ok(None) => Err(Error::ParentIdNotFound(id)),
            Err(e) => Err(e),
        }
    }

    async fn get_parents_if_exists(&self, id: Id) -> Result<Option<Vec<Parent>>> {
        let id_bytes = id.as_bytes();
        let mut k = Vec::with_capacity(1 + id_bytes.len());
        k.push(KvKeyPrefix::Parents as u8);
        k.extend_from_slice(&id_bytes);

        if let Some(parent_bytes) = self.kv.get(&k).await? {
            Ok(Some(Parent::parse_all(&parent_bytes)?))
        } else {
            Ok(None)
        }
    }

    fn insert_parents(&mut self, id: Id, parents: &[Parent]) -> Result<()> {
        let id_bytes = id.as_bytes();
        let mut k = Vec::with_capacity(1 + id_bytes.len());
        k.push(KvKeyPrefix::Parents as u8);
        k.extend_from_slice(&id_bytes);

        let mut v: Vec<u8> = Vec::with_capacity(Parent::num_bytes() * parents.len());
        for parent in parents {
            v.extend(parent.as_bytes());
        }
        Ok(self.kv.insert(k, v)?)
    }

    fn remove_parents(&mut self, id: Id) -> Result<()> {
        let id_bytes = id.as_bytes();
        let mut k = Vec::with_capacity(1 + id_bytes.len());
        k.push(KvKeyPrefix::Parents as u8);
        k.extend_from_slice(&id_bytes);

        Ok(self.kv.remove(k)?)
    }

    async fn get_bytes(&self, id: Id) -> Result<u32> {
        match self.get_bytes_if_exists(id).await {
            Ok(Some(bytes)) => Ok(bytes),
            Ok(None) => Err(Error::BytesOfIdNotFound(id)),
            Err(e) => Err(e),
        }
    }

    async fn get_bytes_if_exists(&self, id: Id) -> Result<Option<u32>> {
        let id_bytes = id.as_bytes();
        let mut k = Vec::with_capacity(1 + id_bytes.len());
        k.push(KvKeyPrefix::Bytes as u8);
        k.extend_from_slice(&id_bytes);

        if let Some(bytes) = self.kv.get(&k).await? {
            let mut byte_array = [0u8; 4];
            for i in 0..4 {
                byte_array[i] = bytes[i];
            }
            Ok(Some(u32::from_be_bytes(byte_array)))
        } else {
            Ok(None)
        }
    }

    async fn insert_total_bytes(
        &mut self,
        id: Id,
        children: &[Id],
        bytes_of_children: &mut HashMap<Id, u32>,
    ) -> Result<()> {
        debug!("self.insert_total_bytes({id}, {children:?}, {bytes_of_children:?})");
        let mut total_bytes = 0;
        for &child_id in children {
            if let Some(bytes) = bytes_of_children.get(&child_id) {
                total_bytes += bytes;
            } else if child_id.points_to_byte() {
                total_bytes += 1;
            } else if let Some(bytes) = self.get_bytes_if_exists(child_id).await? {
                bytes_of_children.insert(child_id, bytes);
                total_bytes += bytes;
            } else {
                error!("Bytes of child {child_id} of node {id} are missing!");
                return Err(Error::BytesOfIdNotFound(child_id));
            }
        }
        bytes_of_children.insert(id, total_bytes);
        self.insert_bytes(id, total_bytes)
    }

    fn insert_bytes(&mut self, id: Id, bytes: u32) -> Result<()> {
        let id_bytes = id.as_bytes();
        let mut k = Vec::with_capacity(1 + id_bytes.len());
        k.push(KvKeyPrefix::Bytes as u8);
        k.extend_from_slice(&id_bytes);
        Ok(self.kv.insert(k, bytes.to_be_bytes().to_vec())?)
    }

    fn new_id(&mut self) -> Result<Id> {
        let id: u128 = self.rng.lock()?.gen();
        Ok(Id::from(id))
    }

    async fn import_bytes(&mut self, ty: ContentType, bytes: &[u8]) -> Result<Id> {
        let (main_rule, grammar) = sequitur(bytes);
        let mut inserted_rules = HashMap::<u32, Id>::new();
        let mut parents = HashMap::<Id, Vec<Parent>>::new();
        let mut terminals = Vec::new();
        for (rule_number, rule) in grammar {
            let id = if let Some(&id) = inserted_rules.get(&rule_number) {
                id
            } else {
                let id = self.new_id()?;
                inserted_rules.insert(rule_number, id);
                id
            };
            if !parents.contains_key(&id) {
                parents.insert(id, Vec::new());
            }

            let mut children = Vec::with_capacity(rule.content.len());
            for (i, symbol) in rule.content.into_iter().enumerate() {
                let child_id = if symbol <= 255 {
                    // terminal (= normal byte)
                    let id = Id::of_byte(ty, symbol as u8);
                    // pseudo-parent to allow finding all terminal bytes of a particular type:
                    terminals.push(Parent::new(id, 0));
                    id
                } else if let Some(&id) = inserted_rules.get(&symbol) {
                    // rule
                    id
                } else {
                    // rule
                    let id = self.new_id()?;
                    inserted_rules.insert(symbol, id);
                    id
                };
                children.push(child_id);
                parents
                    .entry(child_id)
                    .or_default()
                    .push(Parent::new(id, i as u32));
            }
            self.insert_children(id, &children)?;
            self.insert_bytes(id, rule.total_symbols)?;
        }
        for (id, mut parents) in parents {
            parents.sort();
            parents.dedup();
            self.insert_parents(id, &parents)?;
        }
        terminals.sort();
        terminals.dedup();
        self.insert_parents(Id::of_content_type(ty), &terminals)?;
        self.insert_parents(Id::bottom(), &[Parent::new(Id::of_content_type(ty), 0)])?;

        let &main_id = inserted_rules.get(&main_rule).unwrap();
        Ok(main_id)
    }
}

enum KvKeyPrefix {
    Children = 0,
    Parents = 1,
    Bytes = 2,
}
