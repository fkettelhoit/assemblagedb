use assemblage_db::data::{BlockStyle, Layout, Node, SpanStyle, Styles};
use assemblage_view::{
    markup::{block_to_markup, markup_to_block, markup_to_node},
    model::{Block, Span},
    styles,
};

#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::*;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen_test]
fn markup_as_json_string_with_visible_markup() {
    use assemblage_view::markup::markup_to_json;

    assert_eq!(
        markup_to_json("*some markup*").unwrap(),
        "{\"type\":\"Text\",\"spans\":[{\"type\":\"Text\",\"styles\":[\"Bold\"],\"text\":\"some markup\"}]}",
    );
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen_test]
fn markup_json_roundtrip() {
    use assemblage_view::markup::{json_to_markup, markup_to_json};

    let markup = "># A quoted heading, with some _italic and *bold*_ text!";
    let json = markup_to_json(markup).unwrap();
    assert_eq!(json_to_markup(&json).unwrap(), markup);
}

fn assert_roundtrip(before: &str, after: &str, block: Block) {
    let markup = format!("{}{}", before, after);
    assert_eq!(markup_to_block(&markup).unwrap(), block);
    assert_eq!(block_to_markup(&block).unwrap(), markup);
}

fn assert_completed_roundtrip(before: &str, after: &str, complete: &str, block: Block) {
    let markup = format!("{}{}", before, after);
    assert_eq!(markup_to_block(&markup).unwrap(), block);
    assert_eq!(block_to_markup(&block).unwrap(), complete);
}

#[test]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn parse_markup_to_node() {
    let markup = "just text";
    let node = markup_to_node(markup).unwrap();

    assert_eq!(
        node,
        Node::list(Layout::Page, vec![Node::text("just text")])
    );

    let markup = "# A Heading";
    let node = markup_to_node(markup).unwrap();

    assert_eq!(
        node,
        Node::styled(
            Styles::Block(styles!(BlockStyle::Heading)),
            Node::text("A Heading")
        )
    );

    let markup = "# A *Bold* Heading";
    let node = markup_to_node(markup).unwrap();

    assert_eq!(
        node,
        Node::styled(
            Styles::Block(styles!(BlockStyle::Heading)),
            Node::list(
                Layout::Chain,
                vec![
                    Node::text("A "),
                    Node::styled(Styles::Span(styles!(SpanStyle::Bold)), Node::text("Bold")),
                    Node::text(" Heading")
                ]
            )
        )
    );
}

#[test]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn parse_block_without_markup() {
    let markup = "some block without special markup";
    let block = Block::text(vec![Span::text(markup)]);
    assert_roundtrip(markup, "", block);
}

#[test]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn parse_block_with_block_markup() {
    let markup = "># A Heading & Quote";
    let block = Block::Text {
        styles: styles![BlockStyle::Heading, BlockStyle::Quote],
        spans: vec![Span::text("A Heading & Quote")],
    };
    assert_roundtrip(markup, "", block);

    let block = Block::Text {
        styles: styles![BlockStyle::Heading, BlockStyle::Quote],
        spans: vec![Span::text("A Heading & Quote")],
    };
    assert_eq!(
        markup_to_block("##>#>>#>> A Heading & Quote").unwrap(),
        block
    );
    assert_eq!(block_to_markup(&block).unwrap(), markup);

    let markup = ",>-# All block styles";
    let block = Block::Text {
        styles: styles![
            BlockStyle::Aside,
            BlockStyle::List,
            BlockStyle::Heading,
            BlockStyle::Quote
        ],
        spans: vec![Span::text("All block styles")],
    };
    assert_roundtrip(markup, "", block);

    let markup = ",>-#no styles because the space after the prefix is missing";
    let block = Block::text(vec![Span::text(markup)]);
    assert_roundtrip(markup, "", block);
}

#[test]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn parse_block_with_span_markup() {
    let markup = "A *very bold* statement!";
    let block = Block::text(vec![
        Span::text("A "),
        Span::Text {
            styles: styles![SpanStyle::Bold],
            text: "very bold".to_string(),
        },
        Span::text(" statement!"),
    ]);
    assert_roundtrip(markup, "", block);

    let markup = "~_*struck bold italic*_~ _*bold italic*_";
    let block = Block::text(vec![
        Span::Text {
            styles: styles![SpanStyle::Struck, SpanStyle::Bold, SpanStyle::Italic],
            text: "struck bold italic".to_string(),
        },
        Span::text(" "),
        Span::Text {
            styles: styles![SpanStyle::Bold, SpanStyle::Italic],
            text: "bold italic".to_string(),
        },
    ]);
    assert_roundtrip(markup, "", block);

    let markup = "*bold*_italic_~struck~`mono`|marked|";
    let block = Block::text(vec![
        Span::Text {
            styles: styles![SpanStyle::Bold],
            text: "bold".to_string(),
        },
        Span::Text {
            styles: styles![SpanStyle::Italic],
            text: "italic".to_string(),
        },
        Span::Text {
            styles: styles![SpanStyle::Struck],
            text: "struck".to_string(),
        },
        Span::Text {
            styles: styles![SpanStyle::Mono],
            text: "mono".to_string(),
        },
        Span::Text {
            styles: styles![SpanStyle::Marked],
            text: "marked".to_string(),
        },
    ]);
    assert_roundtrip(markup, "", block);

    let markup = "*bold and_italic ~text~_ markup*!";
    let block = Block::text(vec![
        Span::Text {
            styles: styles![SpanStyle::Bold],
            text: "bold and".to_string(),
        },
        Span::Text {
            styles: styles![SpanStyle::Bold, SpanStyle::Italic],
            text: "italic ".to_string(),
        },
        Span::Text {
            styles: styles![SpanStyle::Bold, SpanStyle::Italic, SpanStyle::Struck],
            text: "text".to_string(),
        },
        Span::Text {
            styles: styles![SpanStyle::Bold],
            text: " markup".to_string(),
        },
        Span::text("!"),
    ]);
    assert_roundtrip(markup, "", block);
}

#[test]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn parse_block_with_incomplete_span_markup() {
    let incomplete_markup = "italic starts _here but never ends...";
    let complete_markup = "italic starts _here but never ends..._";
    let block = Block::text(vec![
        Span::text("italic starts "),
        Span::Text {
            styles: styles![SpanStyle::Italic],
            text: "here but never ends...".to_string(),
        },
    ]);

    assert_completed_roundtrip(incomplete_markup, "", complete_markup, block);
}

#[test]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn parse_block_with_escaped_span_markup() {
    let incomplete_markup = "\\_not \\italic, _italic \\_ until here_";
    let complete_markup = "\\_not \\\\italic, _italic \\_ until here_";
    let block = Block::text(vec![
        Span::text("_not \\italic, "),
        Span::Text {
            styles: styles![SpanStyle::Italic],
            text: "italic _ until here".to_string(),
        },
    ]);

    assert_completed_roundtrip(incomplete_markup, "", complete_markup, block);
}

#[test]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn parse_block_with_escaped_block_markup() {
    let markup = "\\#> neither a heading nor a quote";
    let block = Block::text(vec![Span::text("#> neither a heading nor a quote")]);

    assert_roundtrip(markup, "", block);

    let incomplete_markup = "\\#>neither a heading nor a quote";
    let complete_markup = "\\\\#>neither a heading nor a quote";
    let block = Block::text(vec![Span::text(incomplete_markup)]);

    assert_completed_roundtrip(incomplete_markup, "", complete_markup, block);
}

#[test]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn parse_block_with_overlapping_span_markup() {
    let markup = "bold *and _italic* and ~struck overlap_ here~";
    let block = Block::text(vec![
        Span::text("bold "),
        Span::Text {
            styles: styles![SpanStyle::Bold],
            text: "and ".to_string(),
        },
        Span::Text {
            styles: styles![SpanStyle::Bold, SpanStyle::Italic],
            text: "italic".to_string(),
        },
        Span::Text {
            styles: styles![SpanStyle::Italic],
            text: " and ".to_string(),
        },
        Span::Text {
            styles: styles![SpanStyle::Italic, SpanStyle::Struck],
            text: "struck overlap".to_string(),
        },
        Span::Text {
            styles: styles![SpanStyle::Struck],
            text: " here".to_string(),
        },
    ]);
    assert_roundtrip(markup, "", block);
}

#[test]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
fn parse_block_with_empty_inline_markup() {
    let incomplete_markup = "a text that contains empty ** bold markup";
    let complete_markup = "a text that contains empty  bold markup";
    let block = Block::text(vec![
        Span::text("a text that contains empty "),
        Span::text(" bold markup"),
    ]);

    assert_completed_roundtrip(incomplete_markup, "", complete_markup, block);

    let incomplete_markup = "a text that contains empty markup_*";
    let complete_markup = "a text that contains empty markup";
    let block = Block::text(vec![Span::text("a text that contains empty markup")]);

    assert_completed_roundtrip(incomplete_markup, "", complete_markup, block);
}
