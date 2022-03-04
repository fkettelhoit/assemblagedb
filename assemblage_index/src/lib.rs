use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
};

use crate::data::{NodeKind, Parent, Result};
use assemblage_kv::{
    storage::{MemoryStorage, Storage},
    KvStore,
};
use data::{ContentType, Id, Match, Node};
use log::{debug, info};
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
    pub async fn build_from(rng: Rng, ty: ContentType, bytes: &[u8]) -> Result<Self> {
        let grammar = sequitur(bytes);

        let storage = MemoryStorage::new();
        let db = Db::open(storage, rng).await?;

        let mut snapshot = db.current().await;
        let mut inserted_rules = HashMap::<u32, Id>::new();
        let mut parents = HashMap::<Id, Vec<Parent>>::new();
        let mut terminals = Vec::new();
        for (rule_number, rule) in grammar {
            let id = if let Some(&id) = inserted_rules.get(&rule_number) {
                id
            } else {
                let id = snapshot.new_id()?;
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
                    let id = snapshot.new_id()?;
                    inserted_rules.insert(symbol, id);
                    id
                };
                children.push(child_id);
                parents
                    .entry(child_id)
                    .or_default()
                    .push(Parent::new(id, i as u32));
            }
            snapshot.insert_children(id, &children)?;
        }
        for (id, mut parents) in parents {
            parents.sort();
            parents.dedup();
            snapshot.insert_parents(id, &parents)?;
        }
        terminals.sort();
        terminals.dedup();
        snapshot.insert_parents(Id::of_content_type(ty), &terminals)?;
        snapshot.insert_parents(Id::bottom(), &[Parent::new(Id::of_content_type(ty), 0)])?;

        snapshot.commit().await?;
        Ok(db)
    }
}

pub struct Snapshot<'a, S: Storage, Rng: rand::Rng> {
    kv: assemblage_kv::Snapshot<'a, S>,
    rng: &'a Mutex<Rng>,
}

impl<'a, S: Storage, Rng: rand::Rng> Snapshot<'a, S, Rng> {
    pub async fn import<'b, S2: Storage, Rng2: rand::Rng>(
        &mut self,
        other: &Snapshot<'b, S2, Rng2>,
    ) -> Result<()> {
        let content_types = self.get_parents(Id::bottom()).await?.unwrap();
        let mut terminal_bytes = Vec::with_capacity(content_types.len() * 256);
        for Parent { id, .. } in content_types {
            let bytes = self.get_parents(id).await?.unwrap();
            for Parent { id, .. } in bytes {
                terminal_bytes.push(id);
            }
        }

        let mut own_ids = HashSet::new();
        let mut other_ids = HashSet::new();
        let mut own_contents = HashMap::new();
        let mut other_contents = HashMap::new();

        while let Some(id) = terminal_bytes.pop() {
            match (self.get_parents(id).await?, other.get_parents(id).await?) {
                (None, None) => {}
                (Some(_), None) => {}
                (None, Some(other_parents)) => {
                    for Parent { id: parent_id, .. } in other_parents {
                        other_ids.insert(parent_id);
                        // TODO: do I need the following?
                        self.insert_parents(id, &[])?;
                    }
                }
                (Some(own_parents), Some(other_parents)) => {
                    for Parent { id: parent_id, .. } in own_parents {
                        own_ids.insert(parent_id);
                    }
                    for Parent { id: parent_id, .. } in other_parents {
                        other_ids.insert(parent_id);
                    }
                }
            }
        }

        let mut counter = 0;
        while !other_ids.is_empty() {
            counter += 1;
            let mut own_ids_next_iteration = HashSet::new();
            let mut other_ids_next_iteration = HashSet::new();
            let mut own_contents_next_iteration = HashMap::new();
            let mut other_contents_next_iteration = HashMap::new();

            for &own_id in own_ids.iter() {
                if !own_contents.contains_key(&own_id) {
                    own_contents.insert(own_id, self.get_children(own_id).await?.unwrap());
                }
            }
            for &other_id in other_ids.iter() {
                if !other_contents.contains_key(&other_id) {
                    other_contents.insert(other_id, other.get_children(other_id).await?.unwrap());
                }
            }

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
            // For each node in the other DB, check if it overlaps with any of the subsequences:
            for &other_id in other_ids.iter() {
                let mut overlap = None;
                let other_content = other_contents.get(&other_id).unwrap();
                for i in 0..other_content.len() - 1 {
                    for j in i + 2..other_content.len() + 1 {
                        let other_subseq = &other_content[i..j];
                        let own_subseq = own_subseqs.get(other_subseq);
                        if let Some(&(own_id, own_i, own_j)) = own_subseq {
                            overlap =
                                Some((other_subseq, (own_id, own_i, own_j), (other_id, i, j)));
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
                    let own_content = self.get_children(own_id).await?.unwrap();

                    debug!(">> found match for {own_id} and {other_id}: '{subseq:?}'");
                    assert_eq!(other_content[other_i..other_j], own_content[own_i..own_j]);

                    let subseq_equals_own = own_j - own_i == own_content.len();
                    let subseq_equals_other = other_j - other_i == other_content.len();

                    // 1. store subseq as a new node (if necessary)
                    let subseq_id = if subseq_equals_other {
                        self.insert_children(other_id, subseq)?;
                        other_id
                    } else if subseq_equals_own {
                        own_id
                    } else {
                        let subseq_id = self.new_id()?;
                        self.insert_children(subseq_id, subseq)?;
                        subseq_id
                    };
                    debug!(">> 1. subseq_id: {subseq_id}");

                    // 2. store parents of subseq
                    if subseq_equals_own && subseq_equals_other {
                        // - change children of own id parents from own id to other id
                        for parent in self.get_parents(own_id).await?.unwrap().iter() {
                            let mut children_of_own_parent =
                                self.get_children(parent.id).await?.unwrap();
                            children_of_own_parent[parent.index as usize] = other_id;
                            self.insert_children(parent.id, &children_of_own_parent)?;
                            own_contents_next_iteration.insert(own_id, children_of_own_parent);
                        }
                        // - add all other parents to own parents
                        let own_parents = self.get_parents(own_id).await?.unwrap();
                        let mut parents = self.get_parents(other_id).await?.unwrap_or_default();
                        parents.extend(own_parents);
                        parents.sort();
                        parents.dedup();
                        self.insert_parents(other_id, &parents)?;
                        debug!(">> 2.a) parents: {parents:?}");
                    } else if subseq_equals_other {
                        // - set own id as parent of subseq
                        let mut parents = self.get_parents(other_id).await?.unwrap_or_default();
                        parents.push(Parent::new(own_id, own_i as u32));
                        parents.sort();
                        parents.dedup();
                        self.insert_parents(other_id, &parents)?;
                        debug!(">> 2.b) parents: {parents:?}");
                    } else if subseq_equals_own {
                        // - set other id as parent of subseq
                        let mut parents = self.get_parents(own_id).await?.unwrap();
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
                        debug!(">> 3.a) own {own_id}: {own_content:?} -> {compressed:?}");
                        own_contents_next_iteration.insert(own_id, compressed);

                        // - shift the parent index (to own id) of all children after subseq
                        for (after_subseq, &child_id) in own_content[own_j..].iter().enumerate() {
                            let index_in_own = own_j + after_subseq;
                            let mut parents = self.get_parents(child_id).await?.unwrap();
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
                        debug!(">> 3.b) other {other_id}: {other_content:?} -> {compressed:?}");
                        other_contents_next_iteration.insert(other_id, compressed);
                    }

                    // 4. fix parents of children of subseq
                    if !subseq_equals_own || subseq_equals_other {
                        // - point to subseq instead of own id
                        // - point to subseq id instead of other id
                        for (subseq_i, &child) in subseq.iter().enumerate() {
                            let mut parents_of_subseq_child =
                                self.get_parents(child).await?.unwrap();
                            for parent in parents_of_subseq_child.iter_mut() {
                                if parent.id == own_id && parent.index == (own_i + subseq_i) as u32
                                {
                                    parent.id = subseq_id;
                                    parent.index = subseq_i as u32;
                                }
                                if parent.id == other_id
                                    && parent.index == (other_i + subseq_i) as u32
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
                        for parent in self.get_parents(own_id).await?.unwrap() {
                            if parent.id != other_id {
                                own_ids_next_iteration.insert(parent.id);
                            }
                        }
                    } else {
                        own_ids_next_iteration.insert(own_id);
                    }
                    if subseq_equals_other {
                        for parent in other.get_parents(other_id).await?.unwrap() {
                            other_ids_next_iteration.insert(parent.id);
                        }
                    } else {
                        other_ids_next_iteration.insert(other_id);
                    }

                    // 6. remove own id if it was replaced by other id
                    if subseq_equals_own && subseq_equals_other {
                        self.remove_children(own_id)?;
                        self.remove_parents(own_id)?;
                    }
                } else {
                    let mut all_children_previously_inserted = true;
                    for (i, &child_id) in other_content.iter().enumerate() {
                        if !child_id.points_to_byte()
                            && self.get_children(child_id).await?.is_none()
                        {
                            debug!(">>>> child {child_id} of {other_id} is still missing");
                            all_children_previously_inserted = false;
                            break;
                        }
                        let mut parents = self.get_parents(child_id).await?.unwrap_or_default();
                        parents.push(Parent::new(other_id, i as u32));
                        parents.sort();
                        parents.dedup();
                        self.insert_parents(child_id, &parents)?;
                    }
                    if all_children_previously_inserted {
                        debug!(">>>> no overlap for {other_id}, can be inserted");
                        let parents = other.get_parents(other_id).await?.unwrap();
                        other_ids_next_iteration.extend(parents.iter().map(|p| p.id));
                        self.insert_children(other_id, &other_content)?;
                    }
                }
            }
            own_ids = own_ids_next_iteration;
            other_ids = other_ids_next_iteration;
            own_contents.extend(own_contents_next_iteration);
            other_contents.extend(other_contents_next_iteration);
            if counter >= 15 {
                panic!("endless loop?");
            }
        }
        Ok(())
    }

    pub async fn print(&self) -> Result<()> {
        info!("***** DB PRINTOUT FOR '{}' *****", self.kv.name());
        info!("*** NODES: ***");
        for key in self.kv.keys().await? {
            if key[0] == KvKeyPrefix::Node as u8 {
                let id = Id::parse_all(&key[1..])?[0];
                let children = self.get_children(id).await?;
                info!("{id} -> {:?}", children);
            }
        }
        info!("*** PARENTS: ***");
        for key in self.kv.keys().await? {
            if key[0] == KvKeyPrefix::Parents as u8 {
                let id = Id::parse_all(&key[1..])?[0];
                let parents = self.get_parents(id).await?;
                info!("{id} -> {:?}", parents);
            }
        }
        Ok(())
    }

    pub async fn check_consistency(&self) -> Result<()> {
        for key in self.kv.keys().await? {
            if key[0] == KvKeyPrefix::Node as u8 {
                let id = Id::parse_all(&key[1..])?[0];
                let children = self.get_children(id).await?.unwrap_or_default();
                for (i, &child_id) in children.iter().enumerate() {
                    let parents = self.get_parents(child_id).await?.unwrap();
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
                info!("{id} -> {:?}", children);
            }
        }
        for key in self.kv.keys().await? {
            if key[0] == KvKeyPrefix::Parents as u8 {
                let id = Id::parse_all(&key[1..])?[0];
                let parents = self.get_parents(id).await?.unwrap_or_default();
                for parent in parents {
                    if parent.id.points_to_byte() {
                        continue;
                    }
                    let children = self.get_children(parent.id).await?.unwrap_or_default();
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

    /*pub async fn add(&mut self, ty: ContentType, bytes: &[u8]) -> Result<Id> {
        let other_db = Db::build_from(ty, bytes).await?;
        let other_snapshot = other_db.current().await;
        self.import(&other_snapshot).await
    }*/

    pub async fn search(&self, ty: ContentType, term: &[u8]) -> Result<Vec<Match>> {
        //let term = Db::build_from(ty, term).await?;
        /*let grammar = sequitur(term);
        for byte in term.iter().copied() {
            let id = Id::from_byte(ty, byte);

        }*/
        todo!()
    }

    pub async fn get(&self, id: Id) -> Result<Option<Node>> {
        if let Some(mut unvisited) = self.get_children(id).await? {
            // 1. build a map from ids to their children:
            let mut children = HashMap::new();
            let mut parents = HashMap::new();
            let mut incomplete_nodes = vec![];
            while let Some(id) = unvisited.pop() {
                incomplete_nodes.push(id);
                if !children.contains_key(&id) {
                    parents.insert(id, self.get_parents(id).await?.unwrap());
                    if !id.points_to_byte() {
                        let child_ids = self.get_children(id).await?.unwrap();
                        unvisited.extend(&child_ids);
                        children.insert(id, child_ids);
                    }
                }
            }
            // 2. iterate through incomplete nodes bottom up, so that children already exist:
            let mut nodes: HashMap<Id, Node> = HashMap::new();
            while let Some(id) = incomplete_nodes.pop() {
                let node = if id.points_to_byte() {
                    NodeKind::Byte(*id.as_bytes().last().unwrap())
                } else {
                    let children = children
                        .get(&id)
                        .unwrap()
                        .iter()
                        .map(|id| nodes.get(id).expect("Child nodes should already exist"))
                        .cloned()
                        .collect();
                    NodeKind::List(children)
                };
                let parents = parents.get(&id).unwrap();
                nodes.insert(id, Node::new(id, node, parents.clone()));
            }
            Ok(nodes.remove(&id))
        } else {
            Ok(None)
        }
    }

    /// Commits the current transaction, thereby persisting all of its changes.
    pub async fn commit(self) -> Result<()> {
        Ok(self.kv.commit().await?)
    }

    fn new_id(&mut self) -> Result<Id> {
        let id: u128 = self.rng.lock()?.gen();
        Ok(Id::from(id))
    }

    fn insert_children(&mut self, id: Id, children: &[Id]) -> Result<()> {
        let id_bytes = id.as_bytes();
        let mut k = Vec::with_capacity(1 + id_bytes.len());
        k.push(KvKeyPrefix::Node as u8);
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
        k.push(KvKeyPrefix::Node as u8);
        k.extend_from_slice(&id_bytes);

        Ok(self.kv.remove(k)?)
    }

    async fn get_children(&self, id: Id) -> Result<Option<Vec<Id>>> {
        let id_bytes = id.as_bytes();
        let mut k = Vec::with_capacity(1 + id_bytes.len());
        k.push(KvKeyPrefix::Node as u8);
        k.extend_from_slice(&id_bytes);

        if let Some(child_bytes) = self.kv.get(&k).await? {
            Ok(Some(Id::parse_all(&child_bytes)?))
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

    async fn get_parents(&self, id: Id) -> Result<Option<Vec<Parent>>> {
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
}

enum KvKeyPrefix {
    Node = 0,
    Parents = 1,
}
