//! A minimal markup language for Assemblage text blocks.
//!
//! ## Features
//!
//!   - _extremely minimal_: Only 4 block styles and 5 span styles.
//!   - _simple to parse_: Each style corresponds to a single character.
//!   - _unambiguous_: Only one way to write each style.
//!   - _flat_: No nesting, neither for headings nor lists.
//!
//! ## Markup Example
//!
//! (Note that the following code block is not strictly speaking the markup language
//! that is parsed by the functions provided in this crate, as these functions
//! always parse _a single line of markup into a single AssemblageDB block_.)
//!
//! ```text
//! # Headings start with "#".
//! > Block quotes start with ">".
//! - Lists...
//! - ...start...
//! - ...with...
//! - ..."-"!
//! , Oh and by the way, asides start with ",".
//!
//! The above 4 block styles are all there is to block styling.
//! They can be combined in any order:
//!
//! #>, A block quote heading aside.
//! ,>#> Also a block quote heading aside.
//!
//! But " " is needed to separate the block markers from the text:
//!
//! #This is just regular text, as block styles need to end with a " ".
//! #>-This is also just regular text...
//!
//! There are also 5 different span styles:
//!
//! *These three words* are bold.
//! And _this_ is italic.
//! Words can be ~struck from a sentence~.
//! Code can be displayed with a `monospaced typeface`!
//! Some |parts of a sentence| can be marked and thus highlighted.
//!
//! Each span style can be escaped, for example in: 2 \* 2 = 4.
//!
//! And that's it!
//! ```
//!
//! ## Why not Markdown?
//!
//! Markdown is relatively easy to write, but is far from simple to parse and
//! process, with many different implementations that do not always follow the
//! same specification. More importantly however, Markdown provides markup
//! capabilities for _full documents_ including multiple (nested) hierarchy
//! levels and the ability to include arbitrary HTML, which ties Markdown to the
//! web.
//!
//! Instead, the ultra-minimal markup language implemented here provides markup
//! only for text blocks (not full documents) and does not support any nesting,
//! neither of headings nor of lists or other structures. This is deliberate, as
//! nested structure and rich hierarchies arise from the _graph structure and
//! interplay of different AssemblageDB nodes_, not as the result of a single and
//! complex markup block.
//!
//! Minimal markup encourages structure through the combination of different
//! documents, whereas sophisticated markup encourages siloization into fewer
//! less richly connected documents.
//!
//! ## Specification (as ABNF)
//!
//! ```abnf
//! markup       = [block-markup] span-markup
//! block-markup = 1*(heading / quote / list / aside) " "
//! heading      = "#"
//! quote        = ">"
//! list         = "-"
//! aside        = ","
//! span-markup  = normal / bold / italic / struck / mono / marked
//! normal       = *(unescaped / escaped)
//! unescaped    = ; all characters except "\", "*", "_", "~", "`", "|" and newline
//! escaped      = "\\" / "\*" / "\_" / "\~" / "\`" / "|"
//! bold         = "*" span-markup "*"
//! italic       = "_" span-markup "_"
//! struck       = "~" span-markup "~"
//! mono         = "`" span-markup "`"
//! marked       = "|" span-markup "|"
//! ```
use std::collections::{BTreeSet, HashSet};

use assemblage_db::data::{BlockStyle, Layout, Node, SpanStyle};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

use crate::model::{Block, Span};

/// The error type for conversions from markup to blocks.
#[derive(Debug)]
pub enum DeserializationError {
    /// Errors raised while converting to/from JSON using serde.
    SerdeError(serde_json::Error),
    /// Markup for a single block must never contain any newlines.
    FoundNewline,
}

impl From<serde_json::Error> for DeserializationError {
    fn from(e: serde_json::Error) -> Self {
        Self::SerdeError(e)
    }
}

#[cfg(target_arch = "wasm32")]
impl From<DeserializationError> for JsValue {
    fn from(e: DeserializationError) -> Self {
        match e {
            DeserializationError::SerdeError(e) => JsValue::from_str(&e.to_string()),
            DeserializationError::FoundNewline => {
                JsValue::from_str("Found newline in block markup")
            }
        }
    }
}

/// The error type for conversions from blocks to markup.
#[derive(Debug)]
pub enum SerializationError {
    /// Block type does not support serialization.
    InvalidBlockType(Block),
    /// Span type does not support serialization.
    InvalidSpanType(Span),
}

#[cfg(target_arch = "wasm32")]
impl From<SerializationError> for JsValue {
    fn from(e: SerializationError) -> Self {
        match e {
            SerializationError::InvalidBlockType(b) => {
                JsValue::from_str(&format!("Invalid block type: {:?}", b))
            }
            SerializationError::InvalidSpanType(s) => {
                JsValue::from_str(&format!("Invalid span type: {:?}", s))
            }
        }
    }
}

/// Parses a single line of markup and converts it into a node tree.
pub fn markup_to_node(markup: &str) -> Result<Node, DeserializationError> {
    let block = parse_block(markup)?;
    Ok(match block {
        Block::Text { styles, spans } => {
            let mut spans: Vec<Node> = spans
                .iter()
                .map(|s| match s {
                    Span::Text { styles, text, .. } => {
                        if styles.is_empty() {
                            Node::text(text)
                        } else {
                            Node::styled(styles.clone(), Node::text(text))
                        }
                    }
                    Span::Link { .. } => {
                        panic!("Link spans should never be the result of parsing markup")
                    }
                })
                .collect();
            let span_node = if spans.len() == 1 {
                spans.pop().unwrap()
            } else {
                Node::list(Layout::Chain, spans)
            };
            if styles.is_empty() {
                Node::list(Layout::Page, vec![span_node])
            } else {
                Node::styled(styles, span_node)
            }
        }
        Block::Cyclic => panic!("Cyclic blocks should never be the result of parsing markup"),
    })
}

/// Parses a single line of markup and returns a block as a JSON string.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
#[cfg(target_arch = "wasm32")]
pub fn markup_to_json(markup: &str) -> std::result::Result<String, JsValue> {
    Ok(serde_json::to_string(&parse_block(markup)?).unwrap())
}

/// Parses a single line of markup and returns a block.
pub fn markup_to_block(markup: &str) -> Result<Block, DeserializationError> {
    parse_block(markup)
}

/// Converts a block (in form of a JSON string) into its markup string
/// representation.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn json_to_markup(markup: &str) -> std::result::Result<String, JsValue> {
    let block: std::result::Result<Block, serde_json::Error> = serde_json::from_str(markup);
    match block {
        Ok(block) => Ok(block_to_markup(&block)?),
        Err(e) => Err(JsValue::from_str(&format!("{:?}", e))),
    }
}

/// Converts a block to its markup string representation.
pub fn block_to_markup(block: &Block) -> Result<String, SerializationError> {
    match block {
        Block::Text { styles, spans } => as_markup(styles, spans),
        Block::Cyclic => Err(SerializationError::InvalidBlockType(block.clone())),
    }
}

fn parse_block(markup: &str) -> Result<Block, DeserializationError> {
    if markup.contains('\n') {
        return Err(DeserializationError::FoundNewline);
    }
    let (index, block_styles) = parse_block_styles_from_prefix(markup);
    let markup = &markup[index..];
    Ok(Block::Text {
        styles: block_styles,
        spans: parse_spans(markup),
    })
}

fn parse_block_styles_from_prefix(markup: &str) -> (usize, BTreeSet<BlockStyle>) {
    let mut styles = BTreeSet::new();
    let (markup, is_escaped) = markup
        .strip_prefix('\\')
        .map_or((markup, false), |stripped| (stripped, true));
    for (i, char) in markup.chars().enumerate() {
        styles.insert(match char {
            ',' => BlockStyle::Aside,
            '>' => BlockStyle::Quote,
            '-' => BlockStyle::List,
            '#' => BlockStyle::Heading,
            ' ' if is_escaped => return (1, BTreeSet::new()),
            ' ' if styles.is_empty() => break,
            ' ' => return (i + 1, styles),
            _ => break,
        });
    }
    (0, BTreeSet::new())
}

fn parse_spans(markup: &str) -> Vec<Span> {
    let mut spans = Vec::new();
    let mut buffer = Vec::new();
    let mut active_styles = HashSet::new();
    let mut is_escaped = false;
    for char in markup.chars() {
        let style = match char {
            '*' => Some(SpanStyle::Bold),
            '_' => Some(SpanStyle::Italic),
            '~' => Some(SpanStyle::Struck),
            '`' => Some(SpanStyle::Mono),
            '|' => Some(SpanStyle::Marked),
            _ => None,
        };
        if let Some(style) = style {
            if is_escaped {
                buffer.push(char);
            } else {
                if !buffer.is_empty() {
                    spans.push(Span::Text {
                        styles: active_styles.iter().copied().collect(),
                        text: buffer.iter().collect(),
                    });
                }
                buffer.clear();
                if active_styles.contains(&style) {
                    active_styles.remove(&style);
                } else {
                    active_styles.insert(style);
                }
            }
        } else {
            if is_escaped {
                buffer.push('\\');
            }
            match char {
                '\\' => {}
                _ => buffer.push(char),
            }
        }
        is_escaped = match char {
            '\\' => !is_escaped,
            _ => false,
        };
    }
    if !buffer.is_empty() {
        spans.push(Span::Text {
            styles: active_styles.iter().copied().collect(),
            text: buffer.iter().collect(),
        });
    }
    spans
}

fn as_markup(styles: &BTreeSet<BlockStyle>, spans: &[Span]) -> Result<String, SerializationError> {
    let mut markup = String::new();
    for block_style in styles.iter().rev() {
        match block_style {
            BlockStyle::Aside => markup.push(','),
            BlockStyle::Quote => markup.push('>'),
            BlockStyle::List => markup.push('-'),
            BlockStyle::Heading => markup.push('#'),
        }
    }
    if !markup.is_empty() {
        markup.push(' ');
    }

    if let Some(Span::Text { styles: _, text }) = spans.last() {
        let (_, block_styles_in_prefix) = parse_block_styles_from_prefix(text);
        if !block_styles_in_prefix.is_empty() {
            markup.push('\\');
        }
    }

    fn add_span_markup<'a>(markup: &mut String, styles: impl Iterator<Item = &'a SpanStyle>) {
        for s in styles {
            match s {
                SpanStyle::Bold => markup.push('*'),
                SpanStyle::Italic => markup.push('_'),
                SpanStyle::Struck => markup.push('~'),
                SpanStyle::Mono => markup.push('`'),
                SpanStyle::Marked => markup.push('|'),
            }
        }
    }

    let mut active_styles = Vec::new();
    for span in spans.iter() {
        match span {
            Span::Text { styles, text } => {
                let mut closed_or_opened = Vec::new();
                for i in (0..active_styles.len()).rev() {
                    let s = active_styles[i];
                    if !styles.iter().any(|next| *next == s) {
                        closed_or_opened.push(s);
                        active_styles.remove(i);
                    }
                }
                for s in styles.iter().rev() {
                    if !active_styles.iter().any(|active| active == s) {
                        closed_or_opened.push(*s);
                        active_styles.push(*s);
                    }
                }
                add_span_markup(&mut markup, closed_or_opened.iter());
                markup.push_str(
                    &text
                        .replace("\\", "\\\\")
                        .replace("*", "\\*")
                        .replace("_", "\\_")
                        .replace("~", "\\~")
                        .replace("`", "\\`")
                        .replace("|", "\\|"),
                );
            }
            _ => return Err(SerializationError::InvalidSpanType(span.clone())),
        }
    }
    if !active_styles.is_empty() {
        add_span_markup(&mut markup, active_styles.iter().rev());
    }
    Ok(markup)
}
