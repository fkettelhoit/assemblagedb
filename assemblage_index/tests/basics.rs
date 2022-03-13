use assemblage_index::{
    data::{ContentType, Id},
    Db, Snapshot,
};
use assemblage_kv::storage::{PlatformStorage, Storage};
use rand::thread_rng;
use std::{collections::HashMap, future::Future};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

const TEXT_CONTENT: ContentType = ContentType(0);

#[test]
fn index_text1() {
    with_storage(file!(), line!(), |_| async {
        let (x_id, x) = Db::build_from(thread_rng(), TEXT_CONTENT, "bafoobar".as_bytes()).await?;
        let (y_id, y) = Db::build_from(thread_rng(), TEXT_CONTENT, "babaqux".as_bytes()).await?;

        let mut x_snapshot = x.current().await;
        assert_grammar!(x_snapshot, x_id, {
            0 -> 1 "foo" 1 "r",
            1 -> "ba",
        });

        let y_snapshot = y.current().await;
        assert_grammar!(y_snapshot, y_id, {
            0 -> 1 1 "qux",
            1 -> "ba",
        });

        x_snapshot.import(&y_snapshot).await?;

        assert_grammar!(x_snapshot, x_id, {
            0 -> 1 "foo" 1 "r",
            1 -> "ba",
        });
        assert_grammar!(x_snapshot, y_id, {
            0 -> 1 1 "qux",
            1 -> "ba",
        });

        x_snapshot.check_consistency().await?;
        Ok(())
    })
}

#[test]
fn index_text2() {
    with_storage(file!(), line!(), |_| async {
        let (x_id, x) = Db::build_from(thread_rng(), TEXT_CONTENT, "foobarbaz".as_bytes()).await?;
        let (y_id, y) = Db::build_from(thread_rng(), TEXT_CONTENT, "xybarqux".as_bytes()).await?;

        let mut x_snapshot = x.current().await;
        assert_grammar!(x_snapshot, x_id, {
            0 -> "foo" 1 "r" 1 "z",
            1 -> "ba",
        });

        let y_snapshot = y.current().await;
        assert_grammar!(y_snapshot, y_id, {
            0 -> "xybarqux"
        });

        x_snapshot.import(&y_snapshot).await?;

        assert_grammar!(x_snapshot, x_id, {
            0 -> "foo" 1 2 "z",
            1 -> 2 "r",
            2 -> "ba",
        });
        assert_grammar!(x_snapshot, y_id, {
            0 -> "xy" 1 "qux",
            1 -> 2 "r",
            2 -> "ba",
        });

        x_snapshot.check_consistency().await?;
        Ok(())
    })
}

fn with_storage<T, Fut>(file: &str, line: u32, mut t: T)
where
    T: FnMut(PlatformStorage) -> Fut,
    Fut: Future<Output = assemblage_index::data::Result<()>>,
{
    let _ignored = env_logger::Builder::from_default_env()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        .try_init();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let file = std::path::Path::new(file)
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap();
        let name = format!("{}_{}", file, line);
        assemblage_kv::storage::purge(&name)
            .await
            .expect("Could not purge storage before test");
        let storage = assemblage_kv::storage::open(&name)
            .await
            .expect("Could not open storage for test");

        let result = t(storage).await;
        assert!(result.is_ok());

        assemblage_kv::storage::purge(&name)
            .await
            .expect("Could not purge storage after test");
    })
}

#[derive(Debug)]
enum Symbol<'a> {
    Rule(u32),
    Bytes(&'a str),
}

impl From<u32> for Symbol<'_> {
    fn from(rule: u32) -> Self {
        Self::Rule(rule)
    }
}

impl<'a> From<&'a str> for Symbol<'a> {
    fn from(terminals: &'a str) -> Self {
        Self::Bytes(terminals)
    }
}

async fn check_grammar<'a, S: Storage, Rng: rand::Rng>(
    db: &Snapshot<'a, S, Rng>,
    id: Id,
    grammar: &HashMap<u32, Vec<Symbol<'_>>>,
) -> Result<(), String> {
    let main_rule = grammar
        .get(&0)
        .expect("Grammar does not contain a main rule with number 0");
    let tree = db
        .get(id)
        .await
        .expect(&format!("Could not find main id {id}"))
        .expect(&format!("Could not find main id {id}"));
    let node = tree
        .children
        .get(&id)
        .expect(&format!("Could not find main id {id}"));
    let mut mapping = HashMap::new();
    let mut comparisons = vec![(0, main_rule, node)];
    mapping.insert(0, id);
    while let Some((head, main_rule, node)) = comparisons.pop() {
        let mut children = node.iter().copied();
        let mut symbol_index = 0;
        let mut rule_pretty = format!("{head} ->");
        for symbol in main_rule {
            rule_pretty += &match symbol {
                Symbol::Rule(r) => format!(" {r}"),
                Symbol::Bytes(s) => format!(" \"{s}\""),
            };
        }
        for symbol in main_rule.iter() {
            match symbol {
                Symbol::Rule(rule_num) => {
                    if let Some(next) = children.next() {
                        if next.points_to_byte() {
                            return Err(format!("{rule_pretty} ({symbol_index}): Expected rule {rule_num}, but found {next}"));
                        } else if let Some(&expected_id) = mapping.get(rule_num) {
                            if expected_id != next {
                                return Err(format!("{rule_pretty} ({symbol_index}): Expected {rule_num} to match {expected_id}, but found {next}"));
                            }
                        } else if let Some(rule) = grammar.get(rule_num) {
                            let next_children = tree.children.get(&next).unwrap();
                            mapping.insert(*rule_num, next);
                            comparisons.push((*rule_num, rule, next_children));
                        } else {
                            return Err(format!("{rule_pretty} ({symbol_index}): Could not find rule {rule_num} in grammar"));
                        }
                        symbol_index += 1;
                    } else {
                        return Err(format!(
                            "{rule_pretty} ({symbol_index}): Expected rule, but found nothing"
                        ));
                    }
                }
                Symbol::Bytes(s) => {
                    for c in s.chars() {
                        if let Some(next) = children.next() {
                            let expected_id = Id::of_byte(ContentType(0), c as u8);
                            if expected_id != next {
                                return Err(format!(
                                "{rule_pretty} ({symbol_index}): Expected {expected_id}, but found {next}"
                            ));
                            }
                            symbol_index += 1;
                        } else {
                            return Err(format!(
                                "{rule_pretty} ({symbol_index}): Expected byte, but found nothing"
                            ));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[macro_export]
macro_rules! assert_grammar {
    ($snapshot:ident, $id:ident, { $ ( $head:literal -> $ ( $symbol:literal ) + ) , + $(,)? }) => {
        {
            let mut grammar = HashMap::new();
            $(
                let symbols = vec![
                    $(
                        Symbol::from($symbol),
                    )+
                ];
                grammar.insert($head, symbols);
            )+
            if let Err(msg) = check_grammar(&$snapshot, $id, &grammar).await {
                panic!("{}", msg);
            }
        }
    };
}
