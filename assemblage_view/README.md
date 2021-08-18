# Linearized View Model & Bindings for AssemblageDB

Assemblage View is a relatively thin layer between frontend implementations and
AssemblageDBs, exposing the document-graphs of AssemblageDBs as a linearized
view-model that can more easily be processed by a frontend. Assemblage View can
be thought of as the Ariadne to AssemblageDB's labyrinth, providing a flat and
linear view onto a deeply nested and possibly cyclic structure, with links to
other paths that "branch off" the main path.

Assemblage View implements bindings that can be used from JS to query and update
an AssemblageDB running on wasm, with an ultra-minimal [markup
language](#markup) inspired by Markdown as a way to edit text blocks and sync
their visual representation with the DB.

**Please note that the view model is still rather experimental (even more so
than the AssemblageDB data model) and may change considerably in the future.**

## Data Model

Nodes in an AssemblageDB form a directed, possibly cylic graph and cannot be
straightforwardly displayed without first "linearizing" them into a view that
shows only a _single subtree_ and renders all connected parents or siblings as
_links that branch off_. This crate provides such a frontend-independent view
model, which can be used as a high level, linearized interface to an
AssemblageDB by frontends on different platforms.

A linearized view consists of 6 different levels of components:

  - A single _Space_, which contains one or more tiles (usually arranged
    horizontally side by side).
  - _Tiles_, a single node and its subtree (of descendants), which can contain
    zero or more sections.
  - _Sections_, a single editable subsections or an uneditable group of
    subsections that appear as children of multiple nodes.
  - _Subsections_, which contain a single block and one or more branches leading
    to other nodes before or after the block.
  - _Blocks_ descendants of the top-level node that must be displayed as blocks
    due to their layout or style.
  - _Spans_, the descendants of the block descendants that must be displayed as
    spans due to their layout or style.

As a result, frontends only have to follow a very simple document model, which
is always just 6 levels deep: A space contains tiles, which contain sections,
which contain subsections, which contain a single block, which contain spans.

By design, nested lists (as in Markdown) or multiple levels of headings are not
supported. Instead of a single, complex and hierarchical document, Assemblage
spaces favor a collection of relatively flat tiles. The richer structure of an
Assemblage space emerges as the result of the interplay of these intertwined
nodes.

## Markup

Assemblage View supports an ultra-minimal markup language inspired by Markdown,
but much simpler and deliberately focused on flat markup for a single block of
text.

### Features

  - _extremely minimal_: Only 4 block styles and 5 span styles.
  - _simple to parse_: Each style corresponds to a single character.
  - _unambiguous_: Only one way to write each style.
  - _flat_: No nesting, neither for headings nor lists.

### Markup Example

(Note that the following code block is not strictly speaking the markup language
that is parsed by the functions provided in this crate, as these functions
always parse _a single line of markup into a single AssemblageDB block_.)

```text
# Headings start with "#".
> Block quotes start with ">".
- Lists...
- ...start...
- ...with...
- ..."-"!
, Oh and by the way, asides start with ",".

The above 4 block styles are all there is to block styling.
They can be combined in any order:

#>, A block quote heading aside.
,>#> Also a block quote heading aside.

But " " is needed to separate the block markers from the text:

#This is just regular text, as block styles need to end with a " ".
#>-This is also just regular text...

There are also 5 different span styles:

*These three words* are bold.
And _this_ is italic.
Words can be ~struck from a sentence~.
Code can be displayed with a `monospaced typeface`!
Some |parts of a sentence| can be marked and thus highlighted.

Each span style can be escaped, for example in: 2 \* 2 = 4.

And that's it!
```

### Why not Markdown?

Markdown is relatively easy to write, but is far from simple to parse and
process, with many different implementations that do not always follow the same
specification. More importantly however, Markdown provides markup capabilities
for _full documents_ including multiple (nested) hierarchy levels and the
ability to include arbitrary HTML, which ties Markdown to the web.

Instead, the ultra-minimal markup language implemented here provides markup only
for text blocks (not full documents) and does not support any nesting, neither
of headings nor of lists or other structures. This is deliberate, as nested
structure and rich hierarchies arise from the _graph structure and interplay of
different AssemblageDB nodes_, not as the result of a single and complex markup
block.

Minimal markup encourages structure through the combination of different
documents, whereas sophisticated markup encourages siloization into fewer less
richly connected documents.

### Specification (as ABNF)

```abnf
markup       = [block-markup] span-markup
block-markup = 1*(heading / quote / list / aside) " "
heading      = "#"
quote        = ">"
list         = "-"
aside        = ","
span-markup  = normal / bold / italic / struck / mono / marked
normal       = *(unescaped / escaped)
unescaped    = ; all characters except "\", "*", "_", "~", "`", "|" and newline
escaped      = "\\" / "\*" / "\_" / "\~" / "\`" / "|"
bold         = "*" span-markup "*"
italic       = "_" span-markup "_"
struck       = "~" span-markup "~"
mono         = "`" span-markup "`"
marked       = "|" span-markup "|"
```
