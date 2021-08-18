# Distributed Document/Graph DB for Connected Pages

AssemblageDB is a transactional high-level database for connected webs of pages,
notes, texts and other media. Think of it like a _personal web_, but easily
editable, with more connections and better navigation than the web. It is
high-level in the sense that it defines a document model similar to HTML but
vastly simpler and with graph-like 2-way links instead of tree-like 1-way jump
links. The data model is both:

  - _document-oriented:_ supports nested documents without a fixed schema
  - _graph-based:_ documents can have multiple parents and form a directed,
    possibly cyclic graph

## Features

  - _versioned:_ old values remain accessible until merged
  - _transactional:_ snapshots are isolated through
    [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)
  - _storage-agnostic:_ supports native and wasm targets
  - _indexed:_ maintains an automatic index for similarity/overlap search
  - _distributed:_ nodes can be published/subscribed as remote broadcasts

## Components

[AssemblageDB](assemblage_db) is the core of the project, a document-graph DB
built on top of the [AssemblageKV](assemblage_kv) store and exposed to a web
frontend (not in this repo) using the [Assemblage View](assemblage_view)
linearized view model. AssemblageDB nodes can be broadcast and persisted in the
cloud using the [Assemblage Broadcast](assemblage_broadcast) service.

The best place to get an understanding of the whole system is probably
[AssemblageDB](assemblage_db) and its document-graph data model, followed by
[Assemblage View's](assemblage_view) view model. The former is deliberately open
and graph-like at the cost of being quite abstract, with the latter as a
concrete view that displays a single linear path in the DB graph and injects all
connections to other nodes as branches into the view.

```text
+=================+
| Web Frontend    | (Typescript/React)
+========+========+
         |
+========+========+
| Assemblage View |
+-----------------+          +======================+
| AssemblageDB    | <~~~~~~> | Assemblage Broadcast |
+-----------------+          +======================+
| AssemblageKV    |
+========+========+
         |
+========+========+
| Native | WASM   |
+=================+
```

## Why?

AssemblageDB is an attempt to reimagine and prototype a different vision of the
web in general and of connected and overlapping documents/pages/media in
particular. It works similar to a wiki, shares a few similarities with Ted
Nelson's [Xanadu](https://en.wikipedia.org/wiki/Project_Xanadu) and
[ZigZag](https://en.wikipedia.org/wiki/ZigZag_(software)), but more importantly
grew out of a hypertext system for Wittgenstein's [philosophical
Nachlass](http://wab.uib.no/wab_nachlass.page/), a philosophical corpus of
20.000 pages with a strikingly non-linear structure and way of thinking. It is
also vaguely inspired by Deleuze & Guattari's concept of the
[Rhizome](https://en.wikipedia.org/wiki/Rhizome_(philosophy)) and
[McLuhan's](https://en.wikipedia.org/wiki/Marshall_McLuhan) media theory with
its notion of _acoustic space_. At the risk of rambling on pretentiously and
pseudo-philosophically, one of the aims of this project is to explore how
certain theoretical ideas about non-linear structure can be cast into software
and used practically.
