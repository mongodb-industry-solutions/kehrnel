# ContextObjects runtime overview

The kernel-side ContextObjects runtime is implemented in:

- [models.py](/Users/francesc.mateu/Documents/GitHub/kehrnel/src/kehrnel/contextobjects/models.py)
- [catalog.py](/Users/francesc.mateu/Documents/GitHub/kehrnel/src/kehrnel/contextobjects/catalog.py)
- [resolver.py](/Users/francesc.mateu/Documents/GitHub/kehrnel/src/kehrnel/contextobjects/resolver.py)
- [con2l.py](/Users/francesc.mateu/Documents/GitHub/kehrnel/src/kehrnel/contextobjects/con2l.py)
- [object_maps.py](/Users/francesc.mateu/Documents/GitHub/kehrnel/src/kehrnel/contextobjects/object_maps.py)
- [strategy_support.py](/Users/francesc.mateu/Documents/GitHub/kehrnel/src/kehrnel/contextobjects/strategy_support.py)

The runtime responsibilities are:

- normalize published ContextObject definitions
- load tenant catalogs
- resolve a natural-language-like draft against the modeled universe
- build an executable Con2L contract when the draft is clear enough
- compile that executable contract into a deterministic Mongo plan
- summarize Context Maps against the current definition catalog

This is the shared middle layer between:

- direct retrieval over ContextObjects
- and the higher copilot stack that goes on to semantic products and answer models
