use std::collections::HashMap;

type RuleOrTerminal = u32; // 0-255 for terminals, > 255 for rules
type Digram = [RuleOrTerminal; 2];

#[derive(Debug, Clone, Copy)]
struct DigramInRule {
    rule_num: u32,
    index_in_rule: usize,
}

impl DigramInRule {
    fn new(rule_num: u32, index_in_rule: usize) -> Self {
        Self {
            rule_num,
            index_in_rule,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Rule {
    pub(crate) content: Vec<RuleOrTerminal>,
    pub(crate) total_symbols: u32,
    pointers_to_rule: usize,
}

pub(crate) fn sequitur(bytes: &[u8]) -> (u32, HashMap<u32, Rule>) {
    let main_rule_num = 256;
    if bytes.len() < 4 {
        let mut grammar = HashMap::new();
        let rule = Rule {
            content: bytes.iter().copied().map(|b| b as u32).collect(),
            total_symbols: bytes.len() as u32,
            pointers_to_rule: 1,
        };
        grammar.insert(main_rule_num, rule);
        return (main_rule_num, grammar);
    }
    let mut rule_counter = main_rule_num + 1;
    let mut rules = HashMap::<u32, Rule>::new();
    let mut digrams = HashMap::<Digram, DigramInRule>::new();
    let mut pushed_back = vec![];
    let mut bytes = bytes.iter().copied().map(|b| b as u32);
    let mut main = Rule {
        content: vec![bytes.next().unwrap()],
        total_symbols: 0, // will be correctly set at the end of the fn
        pointers_to_rule: 1,
    };
    while let Some(byte) = pushed_back.pop().or_else(|| bytes.next()) {
        let digram = [*main.content.last().unwrap(), byte];
        let index_in_main_rule = main.content.len() - 1;

        if let Some(matched) = digrams.get(&digram).copied() {
            let (main, other) = if matched.rule_num == main_rule_num {
                (&mut main, None)
            } else {
                (&mut main, Some(matched.rule_num))
            };
            match (main, other) {
                (main, None) if index_in_main_rule - matched.index_in_rule <= 1 => {
                    // digrams overlap, thus cannot be replaced with rule
                    main.content.push(byte as u32);
                }
                (main, Some(rule_num)) if rules.get(&rule_num).unwrap().content.len() == 2 => {
                    // digram is the complete rule, so replace digram with rule
                    let digram_in_main = DigramInRule::new(main_rule_num, index_in_main_rule);
                    let needs_pushback = enforce_digram_uniqueness(
                        &mut digrams,
                        digram_in_main,
                        main,
                        matched.rule_num,
                    );

                    let mut rule = rules.remove(&rule_num).unwrap();
                    rule.pointers_to_rule += 1;

                    enforce_rule_utility(&mut digrams, &mut rules, matched.rule_num, &mut rule);

                    if needs_pushback {
                        pushed_back.push(main.content.pop().unwrap());
                    }

                    rules.insert(rule_num, rule);
                }
                (main, other) => {
                    // create new rule, replace both occurrences with it
                    let total_symbols = total_symbols(&rules, &digram);
                    let mut r = Rule {
                        content: digram.to_vec(),
                        total_symbols,
                        pointers_to_rule: 2,
                    };
                    let r_num = rule_counter;
                    let digram_in_rule = DigramInRule::new(r_num, 0);

                    let digram_in_main = DigramInRule::new(main_rule_num, index_in_main_rule);
                    enforce_digram_uniqueness(&mut digrams, digram_in_main, main, r_num);

                    let other_rule = rules.get_mut(&other.unwrap_or_default()).unwrap_or(main);
                    enforce_digram_uniqueness(&mut digrams, matched, other_rule, r_num);

                    digrams.insert(digram, digram_in_rule);

                    let removed_rules =
                        enforce_rule_utility(&mut digrams, &mut rules, r_num, &mut r);
                    if let Some(other) = other {
                        rules.get_mut(&other).unwrap().total_symbols -= removed_rules;
                    }

                    rules.insert(r_num, r);
                    rule_counter += 1;
                }
            }
        } else {
            // digram exists only once, so append to main rule and add to digram index
            let r = DigramInRule {
                rule_num: main_rule_num,
                index_in_rule: index_in_main_rule,
            };
            digrams.insert(digram, r);
            main.content.push(byte as u32);
        }
    }
    main.total_symbols = total_symbols(&rules, &main.content);
    rules.insert(main_rule_num, main);
    (main_rule_num, rules)
}

fn enforce_digram_uniqueness(
    digrams: &mut HashMap<Digram, DigramInRule>,
    d: DigramInRule,
    rule: &mut Rule,
    r: u32,
) -> bool {
    // To replace a, b in a rule with r:
    // [pre_a, a, b, post_b]; with digrams index {(pre_a, a), (a, b), (b, post_b)} -->
    // [pre_a, r, post_b]; with digrams index {(pre_a, r), (r, post_b)}
    let i = d.index_in_rule;
    let pre_a = if i > 0 { rule.content.get(i - 1) } else { None };
    let a = rule.content.get(i);
    let b = rule.content.get(i + 1);
    let post_b = rule.content.get(i + 2);

    let mut needs_pushback = false;

    if let (Some(&pre_a), Some(&a)) = (pre_a, a) {
        digrams.remove(&[pre_a, a]);
        let digram = DigramInRule::new(d.rule_num, i - 1);
        if digrams.contains_key(&[pre_a, r]) {
            needs_pushback = true;
        } else {
            digrams.insert([pre_a, r], digram);
        }
    }
    if let (Some(&b), Some(&post_b)) = (b, post_b) {
        digrams.remove(&[b, post_b]);
        let digram = DigramInRule::new(d.rule_num, i);
        digrams.insert([r, post_b], digram);
    }
    if let Some(&x) = post_b {
        let mut x = x;
        for i in i + 3..rule.content.len() {
            let y = rule.content[i];
            if let Some(digram_in_rule) = digrams.get_mut(&[x, y]) {
                digram_in_rule.index_in_rule -= 1;
            }
            x = y;
        }
    }
    rule.total_symbols += 1;
    rule.content.remove(i);
    if i < rule.content.len() {
        rule.content[i] = r;
    } else {
        rule.content.push(r);
    }
    needs_pushback
}

fn enforce_rule_utility(
    digrams: &mut HashMap<Digram, DigramInRule>,
    rules: &mut HashMap<u32, Rule>,
    new_rule_num: u32,
    new_rule: &mut Rule,
) -> u32 {
    let mut i = 0;
    let mut removed_rules = 0;
    while i < new_rule.content.len() {
        let symbol = new_rule.content[i];
        if let Some(rule) = rules.get_mut(&symbol) {
            if rule.pointers_to_rule == 2 {
                let rule = rules.remove(&symbol).unwrap();
                let content_len = rule.content.len();
                if i > 0 {
                    let a = new_rule.content[i - 1];
                    let b = rule.content[0];
                    digrams.insert([a, b], DigramInRule::new(new_rule_num, i - 1));
                }
                {
                    let mut a = rule.content[0];
                    for j in 1..content_len {
                        let b = rule.content[j];
                        digrams.insert([a, b], DigramInRule::new(new_rule_num, i + j - 1));
                        a = b;
                    }
                }
                if i < new_rule.content.len() - 1 {
                    let a = *rule.content.last().unwrap();
                    let b = new_rule.content[i + 1];
                    digrams.insert([a, b], DigramInRule::new(new_rule_num, i + content_len - 1));
                }
                new_rule.content.splice(i..i + 1, rule.content);
                new_rule.total_symbols -= 1;
                i += content_len;
                removed_rules += 1;
            } else {
                i += 1;
                rule.pointers_to_rule -= 1;
            }
        } else {
            i += 1;
        }
    }
    removed_rules
}

fn total_symbols(rules: &HashMap<u32, Rule>, symbols: &[u32]) -> u32 {
    symbols
        .into_iter()
        .map(|&symbol| rules.get(&symbol).map(|r| r.total_symbols).unwrap_or(0) + 1)
        .sum()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{sequitur, Rule};

    #[test]
    fn test_sequitur_for_aaa() {
        let mut expected = HashMap::new();
        expected.insert(1, "a,a,a,");
        expect_sequitur_result("aaa", expected);
    }

    #[test]
    fn test_sequitur_for_aaaa() {
        let mut expected = HashMap::new();
        expected.insert(1, "2,2,");
        expected.insert(2, "a,a,");
        expect_sequitur_result("aaaa", expected);
    }

    #[test]
    fn test_sequitur_for_aaaaa() {
        let mut expected = HashMap::new();
        expected.insert(1, "2,2,a,");
        expected.insert(2, "a,a,");
        expect_sequitur_result("aaaaa", expected);
    }

    #[test]
    fn test_sequitur_for_aaaaaa() {
        let mut expected = HashMap::new();
        expected.insert(1, "2,2,2,");
        expected.insert(2, "a,a,");
        expect_sequitur_result("aaaaaa", expected);
    }

    #[test]
    fn test_sequitur_for_barbar() {
        let mut expected = HashMap::new();
        expected.insert(1, "3,3,");
        expected.insert(3, "b,a,r,");
        expect_sequitur_result("barbar", expected);
    }

    #[test]
    fn test_sequitur_for_barbarba() {
        let mut expected = HashMap::new();
        expected.insert(1, "3,3,4,");
        expected.insert(3, "4,r,");
        expected.insert(4, "b,a,");
        expect_sequitur_result("barbarba", expected);
    }

    #[test]
    fn test_sequitur_for_barbarbar() {
        let mut expected = HashMap::new();
        expected.insert(1, "3,3,3,");
        expected.insert(3, "b,a,r,");
        expect_sequitur_result("barbarbar", expected);
    }

    #[test]
    fn test_sequitur_for_foobarbarbaz() {
        let mut expected = HashMap::new();
        expected.insert(1, "f,o,o,3,3,4,z,");
        expected.insert(3, "4,r,");
        expected.insert(4, "b,a,");
        expect_sequitur_result("foobarbarbaz", expected);
    }

    #[test]
    fn test_sequitur_for_foofooboo() {
        let mut expected = HashMap::new();
        expected.insert(1, "3,3,b,4,");
        expected.insert(3, "f,4,");
        expected.insert(4, "o,o,");
        expect_sequitur_result("foofooboo", expected);
    }

    #[test]
    fn test_sequitur_for_ababcabcdabcdeabcdef() {
        let mut expected = HashMap::new();
        expected.insert(1, "2,3,4,5,5,f,");
        expected.insert(2, "a,b,");
        expected.insert(3, "2,c,");
        expected.insert(4, "3,d,");
        expected.insert(5, "4,e,");
        expect_sequitur_result("ababcabcdabcdeabcdef", expected);
    }

    #[test]
    fn test_sequitur_for_aabacadaeafa() {
        let mut expected = HashMap::new();
        expected.insert(1, "a,a,b,a,c,a,d,a,e,a,f,a,");
        expect_sequitur_result("aabacadaeafa", expected);
    }

    #[test]
    fn test_sequitur_for_aaaaaaaa() {
        let mut expected = HashMap::new();
        expected.insert(1, "3,3,");
        expected.insert(2, "a,a,");
        expected.insert(3, "2,2,");
        expect_sequitur_result("aaaaaaaa", expected);
    }

    #[test]
    fn test_sequitur_for_aaaaababacacadad() {
        let mut expected = HashMap::new();
        expected.insert(1, "2,2,3,3,4,4,5,5,");
        expected.insert(2, "a,a,");
        expected.insert(3, "a,b,");
        expected.insert(4, "a,c,");
        expected.insert(5, "a,d,");
        expect_sequitur_result("aaaaababacacadad", expected);
    }

    #[test]
    fn test_sequitur_for_yzxyzwxyzvwxy() {
        let mut expected = HashMap::new();
        expected.insert(1, "2,3,w,3,v,w,x,y,");
        expected.insert(2, "y,z,");
        expected.insert(3, "x,2,");
        expect_sequitur_result("yzxyzwxyzvwxy", expected);
    }

    #[test]
    fn test_sequitur_for_abcdeabcdeabcde() {
        let mut expected = HashMap::new();
        expected.insert(1, "5,5,5,");
        expected.insert(5, "a,b,c,d,e,");
        expect_sequitur_result("abcdeabcdeabcde", expected);
    }

    fn expect_sequitur_result(s: &str, expected: HashMap<u32, &str>) {
        let (main_rule_num, rules) = sequitur(s.as_bytes());
        println!("{rules:?}");
        if expected.len() != rules.len() {
            pretty_print_rules(main_rule_num, &rules);
            panic!(
                "Expected {} rules, but found {}",
                expected.len(),
                rules.len()
            );
        }
        for (r, expected) in expected.iter() {
            if let Some(rule) = rules.get(&(r + main_rule_num - 1)) {
                let pretty = prettify(main_rule_num, rule);
                assert_eq!(expected, &pretty);
            } else {
                panic!("No rule for {}, expected {}", r, expected);
            }
        }
        assert_eq!(
            rules.get(&main_rule_num).unwrap().total_symbols,
            count_total_symbols(1, &expected)
        );
    }

    fn count_total_symbols(rule: u32, rules: &HashMap<u32, &str>) -> u32 {
        let mut total_symbols = 0;
        for symbol in rules.get(&rule).unwrap().chars() {
            if symbol == ',' {
                continue;
            }
            if let Some(rule) = symbol.to_digit(10) {
                total_symbols += count_total_symbols(rule, rules) + 1;
            } else {
                total_symbols += 1;
            }
        }
        total_symbols
    }

    fn pretty_print_rules(main_rule_num: u32, rules: &HashMap<u32, Rule>) {
        let mut keys: Vec<u32> = rules.keys().copied().collect();
        keys.sort();
        for k in keys {
            let rule = prettify(main_rule_num, rules.get(&k).unwrap());
            println!("{} -> {}", k + 1 - main_rule_num, rule);
        }
    }

    fn prettify(main_rule_num: u32, rule: &Rule) -> String {
        let mut printable_rule = "".to_string();
        for rule_or_terminal in rule.content.clone() {
            if rule_or_terminal < 256 {
                if rule_or_terminal > 0 {
                    printable_rule.push(rule_or_terminal as u8 as char);
                }
            } else {
                printable_rule += &format!("{}", rule_or_terminal + 1 - main_rule_num);
            }
            if rule_or_terminal > 0 {
                printable_rule.push(',');
            }
        }
        printable_rule
    }
}
