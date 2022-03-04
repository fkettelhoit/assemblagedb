use std::collections::{HashMap, HashSet};

use crate::data::{NodeKind, Parent, Result};
use assemblage_kv::{
    storage::{MemoryStorage, Storage},
    KvStore,
};
use data::{ContentType, Id, Match, Node};
use sequitur::sequitur;

pub mod data;
pub mod sequitur;

pub struct Db<S: Storage> {
    kv: KvStore<S>,
}

impl<S: Storage> Db<S> {
    /// Opens and reads a DB from storage or creates it if none exists.
    ///
    /// If the storage is empty, an empty list will be automatically added as the root node.
    pub async fn open(storage: S) -> Result<Self> {
        let db = Self {
            kv: KvStore::open(storage).await?,
        };
        /*if db.kv.is_empty().await {
            let id = Id::root();
            let mut snapshot = db.current().await;
            snapshot.insert_children(id, &[])?;
            snapshot.insert_parents(id, &[])?;
            snapshot.commit().await?;
        }*/
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
    pub async fn current(&self) -> Snapshot<'_, S> {
        Snapshot {
            kv: self.kv.current().await,
        }
    }
}

impl Db<MemoryStorage> {
    pub async fn build_from(ty: ContentType, bytes: &[u8]) -> Result<Self> {
        let grammar = sequitur(bytes);

        let storage = MemoryStorage::new();
        let db = Db::open(storage).await?;

        let mut snapshot = db.current().await;
        let mut inserted_rules = HashMap::<u32, Id>::new();
        let mut parents = HashMap::<Id, Vec<Parent>>::new();
        for (rule_number, rule) in grammar {
            let id = *inserted_rules.entry(rule_number).or_default();
            if !parents.contains_key(&id) {
                parents.insert(id, Vec::new());
            }

            let mut children = Vec::with_capacity(rule.content.len());
            for (i, symbol) in rule.content.into_iter().enumerate() {
                let child_id = if symbol <= 255 {
                    // terminal (= normal byte)
                    Id::from_byte(ty, symbol as u8)
                } else {
                    // rule
                    *inserted_rules.entry(symbol).or_default()
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

        snapshot.commit().await?;
        Ok(db)
    }
}

pub struct Snapshot<'a, S: Storage> {
    kv: assemblage_kv::Snapshot<'a, S>,
}

impl<'a, S: Storage> Snapshot<'a, S> {
    pub async fn import<'b, S2: Storage>(&mut self, other: &Snapshot<'b, S2>) -> Result<()> {
        let mut terminal_bytes = Vec::with_capacity(256 * 256);
        // TODO: 0 primitives as indexes for content_types?
        // so that I first check 255 content_types for parents? or even better _1_ bottom node?
        for ty in 0..=1 {
            let ty = ContentType(ty);
            for byte in 0..=255 {
                terminal_bytes.push(Id::from_byte(ty, byte));
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

                    println!(">> found match for {own_id} and {other_id}: '{subseq:?}'");
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
                        let subseq_id = Id::new();
                        self.insert_children(subseq_id, subseq)?;
                        subseq_id
                    };
                    println!(">> 1. subseq_id: {subseq_id}");

                    // 2. store parents of subseq
                    if subseq_equals_own && subseq_equals_other {
                        // nothing to do
                        println!(">> 2.a) parents: (nothing to do)");
                    } else if subseq_equals_other {
                        // - set other id as parent of subseq
                        let mut parents = self.get_parents(other_id).await?.unwrap_or_default();
                        parents.push(Parent::new(own_id, own_i as u32));
                        parents.sort();
                        parents.dedup();
                        self.insert_parents(other_id, &parents)?;
                        println!(">> 2.b) parents: {parents:?}");
                    } else if subseq_equals_own {
                        // - set own id as parent of subseq
                        let mut parents = self.get_parents(own_id).await?.unwrap();
                        parents.push(Parent::new(other_id, other_i as u32));
                        parents.sort();
                        parents.dedup();
                        self.insert_parents(own_id, &parents)?;
                        println!(">> 2.c) parents: {parents:?}");
                    } else {
                        // - set own id as parent of subseq
                        // - set other id as parent of subseq
                        let mut subseq_parents = vec![
                            Parent::new(own_id, own_i as u32),
                            Parent::new(other_id, other_i as u32),
                        ];
                        subseq_parents.sort();
                        self.insert_parents(subseq_id, &subseq_parents)?;
                        println!(">> 2.d) parents: {subseq_parents:?}");
                    }

                    // 3. use subseq as child (use id of subseq instead of subseq directly)
                    if !subseq_equals_own {
                        let space_freed = (own_j - own_i) + 1;
                        let mut compressed = Vec::with_capacity(own_content.len() - space_freed);
                        compressed.extend(&own_content[..own_i]);
                        compressed.push(subseq_id);
                        compressed.extend(&own_content[own_j..]);
                        self.insert_children(own_id, &compressed)?;
                        println!(">> 3.a) own {own_id}: {own_content:?} -> {compressed:?}");
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
                        println!(">> 3.b) other {other_id}: {other_content:?} -> {compressed:?}");
                        other_contents_next_iteration.insert(other_id, compressed);
                    }

                    // 4. fix parents of children of subseq
                    if !subseq_equals_own {
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
                } else {
                    println!(">>>> no overlap for {other_id}, can be inserted...");
                    let mut all_children_previously_inserted = true;
                    for (i, &child_id) in other_content.iter().enumerate() {
                        let parents = self.get_parents(child_id).await?;
                        if let Some(mut parents) = parents {
                            parents.push(Parent::new(other_id, i as u32));
                            parents.sort();
                            parents.dedup();
                            self.insert_parents(child_id, &parents)?;
                        } else {
                            all_children_previously_inserted = false;
                            break;
                        }
                    }
                    if all_children_previously_inserted {
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

    pub async fn import2<'b, S2: Storage>(&mut self, other: &Snapshot<'b, S2>) -> Result<()> {
        let mut terminal_bytes = Vec::with_capacity(256 * 256);
        // TODO: 0 primitives as indexes for content_types?
        // so that I first check 255 content_types for parents? or even better _1_ bottom node?
        for ty in 0..=1 {
            let ty = ContentType(ty);
            for byte in 0..=255 {
                terminal_bytes.push(Id::from_byte(ty, byte));
            }
        }

        let mut own_ids_next_round = HashSet::new();
        let mut other_ids_next_round = HashSet::new();

        while let Some(id) = terminal_bytes.pop() {
            match (self.get_parents(id).await?, other.get_parents(id).await?) {
                (None, None) => {}
                (Some(_), None) => {}
                (None, Some(other_parents)) => {
                    for Parent { id: parent_id, .. } in other_parents {
                        other_ids_next_round.insert(parent_id);
                        // TODO: do I need the following?
                        self.insert_parents(id, &[])?;
                    }
                }
                (Some(own_parents), Some(other_parents)) => {
                    for Parent { id: parent_id, .. } in own_parents {
                        own_ids_next_round.insert(parent_id);
                    }
                    for Parent { id: parent_id, .. } in other_parents {
                        other_ids_next_round.insert(parent_id);
                    }
                }
            }
        }
        let mut own_contents = HashMap::new();
        let mut other_contents = HashMap::new();
        while !other_ids_next_round.is_empty() {
            for id in own_ids_next_round {
                if !own_contents.contains_key(&id) {
                    own_contents.insert(id, self.get_children(id).await?.unwrap());
                }
            }
            for id in other_ids_next_round {
                if !other_contents.contains_key(&id) {
                    other_contents.insert(id, other.get_children(id).await?.unwrap());
                }
            }
            own_ids_next_round = HashSet::new();
            other_ids_next_round = HashSet::new();
            let mut own_contents_next_round = HashMap::new();
            let mut other_contents_next_round = HashMap::new();

            // 1. for each own node, get the content and store all subsequences (which must be unique
            //    due to digram uniqueness; each subsequence can occur in at most one node in each DB):
            let mut own_subsequences = HashMap::new();
            for (id, content) in own_contents.iter() {
                // TODO: move both subsequence loops into the loops above to make sure that only the
                // newly added contents are indexed (the rest is already indexed)
                for i in 0..content.len() - 1 {
                    for j in i + 2..content.len() + 1 {
                        own_subsequences.insert(&content[i..j], (id, i, j));
                    }
                }
            }
            // 2. for each node in the other DB, check if it overlaps with any of the subsequences:
            for (&other_id, other_content) in other_contents.iter() {
                let mut overlap = None;
                for i in 0..other_content.len() - 1 {
                    for j in i + 2..other_content.len() + 1 {
                        if let Some((&own_id, own_i, own_j)) =
                            own_subsequences.get(&other_content[i..j])
                        {
                            overlap = Some(((own_id, *own_i, *own_j), (other_id, i, j)));
                        } else {
                            break;
                        }
                    }
                    if overlap.is_some() {
                        break;
                    }
                }
                if let Some(((own_id, i, j), (other_id, other_i, other_j))) = overlap {
                    // ...found an overlap:
                    //
                    // a. insert subsequence as a node and point to the overlapping nodes as
                    //    parents:
                    let subseq = &other_content[other_i..other_j];
                    println!("Found match for {own_id} and {other_id}: '{subseq:?}'");
                    let own_content = own_contents.get(&own_id).unwrap();
                    let subseq_equals_own = j - i == own_content.len();
                    let subseq_equals_other = other_j - other_i == other_content.len();
                    let id_of_subsequence = if subseq_equals_own && subseq_equals_other {
                        println!("subseq == own && subseq == other");
                        for &child_id in own_content {
                            let mut parents_of_child = self.get_parents(child_id).await?.unwrap();
                            for parent in parents_of_child.iter_mut() {
                                if parent.id == own_id {
                                    parent.id = other_id;
                                }
                            }
                            parents_of_child.sort();
                            parents_of_child.dedup();
                            self.insert_parents(child_id, &parents_of_child)?;
                        }
                        let own_parents = self.get_parents(own_id).await?.unwrap();
                        for parent in own_parents.iter() {
                            let mut children_of_parent =
                                self.get_children(parent.id).await?.unwrap();
                            children_of_parent[parent.index as usize] = other_id;
                            self.insert_children(parent.id, &children_of_parent)?;
                        }
                        let other_parents = other.get_parents(other_id).await?.unwrap();
                        self.insert_children(other_id, &other_content)?;
                        self.remove_children(own_id)?;
                        self.remove_parents(own_id)?;

                        for own_parent in own_parents {
                            own_ids_next_round.insert(own_parent.id);
                        }
                        for other_parent in other_parents {
                            other_ids_next_round.insert(other_parent.id);
                        }

                        other_id
                    } else if subseq_equals_other {
                        println!("subseq == other");
                        self.insert_children(other_id, &other_content)?;
                        self.insert_parents(other_id, &[Parent::new(own_id, i as u32)])?;

                        // b. use id of subsequence as a child of the overlapping nodes (instead of the
                        //    subsequence directly):
                        let mut compressed = Vec::with_capacity(own_content.len() - (j - i) + 1);
                        compressed.extend(&own_content[..i]);
                        compressed.push(other_id);
                        compressed.extend(&own_content[j..]);
                        println!("own_id {own_id}: {own_content:?} -> {compressed:?}");
                        self.insert_children(own_id, &compressed)?;

                        // shift all children of own id after subsequence to the left
                        for (after_subsequence, &child_id) in own_content[j..].iter().enumerate() {
                            let index_in_own = j + after_subsequence;
                            let mut parents = self.get_parents(child_id).await?.unwrap();
                            for parent in parents.iter_mut() {
                                if parent.id == own_id && parent.index == index_in_own as u32 {
                                    parent.index -= (subseq.len() - 1) as u32;
                                }
                            }
                            parents.sort();
                            self.insert_parents(child_id, &parents)?;
                        }

                        own_contents_next_round.insert(other_id, other_content.to_vec());
                        own_contents_next_round.insert(own_id, compressed);

                        own_ids_next_round.insert(own_id);
                        let other_parents = other.get_parents(other_id).await?.unwrap();
                        for other_parent in other_parents {
                            other_ids_next_round.insert(other_parent.id);
                        }

                        other_id
                    } else if subseq_equals_own {
                        println!("subseq == own");
                        // own sequence is identical to subsequence, no need to create a new node,
                        // but other_id needs to be added as a parent:
                        let mut parents = self.get_parents(own_id).await?.unwrap();
                        parents.push(Parent::new(other_id, other_i as u32));
                        parents.sort();
                        parents.dedup();
                        self.insert_parents(own_id, &parents)?;

                        let own_parents = self.get_parents(own_id).await?.unwrap();
                        for own_parent in own_parents {
                            own_ids_next_round.insert(own_parent.id);
                        }
                        other_ids_next_round.insert(other_id);

                        own_id
                    } else {
                        println!("subseq != own && subseq != other");
                        // need to store subsequence as a new node and point to it from own_id:
                        let id_of_subsequence = Id::new();
                        let mut parents_of_subsequence = vec![
                            Parent::new(own_id, i as u32),
                            Parent::new(other_id, other_i as u32),
                        ];
                        parents_of_subsequence.sort();
                        parents_of_subsequence.dedup();
                        self.insert_parents(id_of_subsequence, &parents_of_subsequence)?;

                        self.insert_children(id_of_subsequence, subseq)?;

                        // b. use id of subsequence as a child of the overlapping nodes (instead of the
                        //    subsequence directly):
                        let mut compressed = Vec::with_capacity(own_content.len() - (j - i) + 1);
                        compressed.extend(&own_content[..i]);
                        compressed.push(id_of_subsequence);
                        compressed.extend(&own_content[j..]);
                        println!("own_id {own_id}: {own_content:?} -> {compressed:?}");
                        self.insert_children(own_id, &compressed)?;

                        // shift all children of own id after subsequence to the left
                        for (after_subsequence, &child_id) in own_content[j..].iter().enumerate() {
                            let index_in_own = j + after_subsequence;
                            let mut parents = self.get_parents(child_id).await?.unwrap();
                            for parent in parents.iter_mut() {
                                if parent.id == own_id && parent.index == index_in_own as u32 {
                                    parent.index -= (subseq.len() - 1) as u32;
                                }
                            }
                            parents.sort();
                            self.insert_parents(child_id, &parents)?;
                        }

                        own_contents_next_round.insert(id_of_subsequence, subseq.to_vec());
                        own_contents_next_round.insert(own_id, compressed);

                        own_ids_next_round.insert(own_id);
                        other_ids_next_round.insert(other_id);

                        id_of_subsequence
                    };

                    if !subseq_equals_other {
                        let mut compressed =
                            Vec::with_capacity(other_content.len() - (other_j - other_i) + 1);
                        compressed.extend(&other_content[..other_i]);
                        compressed.push(id_of_subsequence);
                        compressed.extend(&other_content[other_j..]);
                        self.insert_children(other_id, &compressed)?;
                        println!("other_id {other_id}: {other_content:?} -> {compressed:?}");
                        other_contents_next_round.insert(other_id, compressed);
                    }

                    // c. let the children of the overlapping part of the nodes point to the id of
                    //    the subsequence instead:
                    for (index_in_subseq, &child) in subseq.iter().enumerate() {
                        let mut parents_of_child = self.get_parents(child).await?.unwrap();
                        for parent in parents_of_child.iter_mut() {
                            if parent.id == own_id && parent.index == (i + index_in_subseq) as u32 {
                                parent.id = id_of_subsequence;
                                parent.index = index_in_subseq as u32;
                            }
                            if parent.id == other_id
                                && parent.index == (other_i + index_in_subseq) as u32
                            {
                                parent.id = id_of_subsequence;
                                parent.index = index_in_subseq as u32;
                            }
                        }
                        parents_of_child.sort();
                        parents_of_child.dedup();
                        self.insert_parents(child, &parents_of_child)?;
                    }
                } else {
                    // ...no overlap:
                    println!("No overlap for {other_id}, can be inserted...");
                    self.insert_children(other_id, other_content)?;
                    for (i, &child_id) in other_content.iter().enumerate() {
                        let mut parents = self.get_parents(child_id).await?.unwrap();
                        parents.push(Parent::new(other_id, i as u32));
                        parents.sort();
                        parents.dedup();
                        self.insert_parents(child_id, &parents)?;
                    }
                    let parents = other.get_parents(other_id).await?.unwrap();
                    other_ids_next_round.extend(parents.iter().map(|p| p.id));
                }
            }
            own_contents.extend(own_contents_next_round);
            other_contents.extend(other_contents_next_round);
        }
        Ok(())
    }

    pub async fn print(&self) -> Result<()> {
        println!("\n***** DB PRINTOUT FOR '{}' *****", self.kv.name());
        println!("*** NODES: ***");
        for key in self.kv.keys().await? {
            if key[0] == KvKeyPrefix::Node as u8 {
                let id = Id::parse_all(&key[1..])?[0];
                let children = self.get_children(id).await?;
                println!("{id} -> {:?}", children);
            }
        }
        println!("*** PARENTS: ***");
        for key in self.kv.keys().await? {
            if key[0] == KvKeyPrefix::Parents as u8 {
                let id = Id::parse_all(&key[1..])?[0];
                let parents = self.get_parents(id).await?;
                println!("{id} -> {:?}", parents);
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
                println!("{id} -> {:?}", children);
            }
        }
        for key in self.kv.keys().await? {
            if key[0] == KvKeyPrefix::Parents as u8 {
                let id = Id::parse_all(&key[1..])?[0];
                let parents = self.get_parents(id).await?.unwrap_or_default();
                for parent in parents {
                    let children = self.get_children(parent.id).await?.unwrap_or_default();
                    if children.len() <= parent.index as usize
                        || children[parent.index as usize] != id
                    {
                        panic!(
                            "Parent {} does not have child {id} at index {}",
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

    fn insert_children(&mut self, id: Id, children: &[Id]) -> Result<()> {
        let id_bytes = id.0.as_bytes();
        let mut k = Vec::with_capacity(1 + id_bytes.len());
        k.push(KvKeyPrefix::Node as u8);
        k.extend_from_slice(id_bytes);

        let mut v: Vec<u8> = Vec::with_capacity(Id::num_bytes() * children.len());
        for id in children {
            v.extend(id.as_bytes());
        }
        Ok(self.kv.insert(k, v)?)
    }

    fn remove_children(&mut self, id: Id) -> Result<()> {
        let id_bytes = id.0.as_bytes();
        let mut k = Vec::with_capacity(1 + id_bytes.len());
        k.push(KvKeyPrefix::Node as u8);
        k.extend_from_slice(id_bytes);

        Ok(self.kv.remove(k)?)
    }

    async fn get_children(&self, id: Id) -> Result<Option<Vec<Id>>> {
        let id_bytes = id.0.as_bytes();
        let mut k = Vec::with_capacity(1 + id_bytes.len());
        k.push(KvKeyPrefix::Node as u8);
        k.extend_from_slice(id_bytes);

        if let Some(child_bytes) = self.kv.get(&k).await? {
            Ok(Some(Id::parse_all(&child_bytes)?))
        } else {
            Ok(None)
        }
    }

    fn insert_parents(&mut self, id: Id, parents: &[Parent]) -> Result<()> {
        let id_bytes = id.0.as_bytes();
        let mut k = Vec::with_capacity(1 + id_bytes.len());
        k.push(KvKeyPrefix::Parents as u8);
        k.extend_from_slice(id_bytes);

        let mut v: Vec<u8> = Vec::with_capacity(Parent::num_bytes() * parents.len());
        for parent in parents {
            v.extend(parent.as_bytes());
        }
        Ok(self.kv.insert(k, v)?)
    }

    fn remove_parents(&mut self, id: Id) -> Result<()> {
        let id_bytes = id.0.as_bytes();
        let mut k = Vec::with_capacity(1 + id_bytes.len());
        k.push(KvKeyPrefix::Parents as u8);
        k.extend_from_slice(id_bytes);

        Ok(self.kv.remove(k)?)
    }

    async fn get_parents(&self, id: Id) -> Result<Option<Vec<Parent>>> {
        let id_bytes = id.0.as_bytes();
        let mut k = Vec::with_capacity(1 + id_bytes.len());
        k.push(KvKeyPrefix::Parents as u8);
        k.extend_from_slice(id_bytes);

        if let Some(parent_bytes) = self.kv.get(&k).await? {
            Ok(Some(Parent::parse_all(&parent_bytes)?))
        } else {
            Ok(None)
        }
    }

    /*fn insert_indexed(&mut self, ty: ContentType, byte: u8, parents: Parents) -> Result<()> {
        let k = vec![KvKeyPrefix::Index as u8, ty.0, byte];
        let v = parents.into();
        Ok(self.kv.insert(k, v)?)
    }

    async fn get_indexed(&self, ty: ContentType, byte: u8) -> Result<Option<Parents>> {
        let k = vec![KvKeyPrefix::Index as u8, ty.0, byte];
        let v = self.kv.get(&k).await?;
        Ok(if let Some(v) = v {
            Some(Parents::try_from(v)?)
        } else {
            None
        })
    }*/
}

enum KvKeyPrefix {
    Node = 0,
    Parents = 1,
}
