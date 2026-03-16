# Queries as Function Arguments

In all other sections, we have used queries with the method [`query_nodes()`](graphrecords.graphrecord.GraphRecord.query_nodes){target="_blank"} for representation purposes of its capacities. However, queries can also be used as function arguments to other methods or indexers from the [`GraphRecord`](graphrecords.graphrecord.GraphRecord){target="_blank"} that take edge/node indices or the queries that result on those indices as arguments. Here are some examples of those functions:

- Using the [`add_group()`](graphrecords.graphrecord.GraphRecord.add_group){target="_blank"} to create groups in the GraphRecord out of a chosen subset of users. We need to [`unfreeze_schema()`](graphrecords.graphrecord.GraphRecord.unfreeze_schema){target="_blank"} first, since this new group does not exist in the schema and we have a provided schema in the example dataset.

```{exec-literalinclude} scripts/query_engine.py
---
language: python
setup-lines: 1-53, 66-74
lines: 216-219
---
```

- Using the [`node[]`](graphrecords.graphrecord.GraphRecord.node){target="_blank"} indexer, which retrieves the attributes for the given node indices.

```{exec-literalinclude} scripts/query_engine.py
---
language: python
setup-lines: 1-53, 154-173
lines: 221
---
```

- Using [`groups_of_node()`](graphrecords.graphrecord.GraphRecord.groups_of_node){target="_blank"}, a method that retrieves the groups to which a specific node index belongs to.

```{exec-literalinclude} scripts/query_engine.py
---
language: python
setup-lines: 1-74
lines: 222
---
```

- Using [`edge_endpoints()`](graphrecords.graphrecord.GraphRecord.edge_endpoints){target="_blank"}, a method that retrieves the source and target nodes of the specified edge(s) in the GraphRecord.

```{exec-literalinclude} scripts/query_engine.py
---
language: python
setup-lines: 1-53, 118-128
lines: 223
---
```

:::{dropdown} Methods used in the snippet

- [`unfreeze_schema()`](graphrecords.graphrecord.GraphRecord.unfreeze_schema){target="_blank"} : Unfreezes the schema. Changes are automatically inferred.
- [`add_group()`](graphrecords.graphrecord.GraphRecord.add_group){target="_blank"} : Adds a group to the GraphRecord, optionally with node and edge indices.
- [`groups`](graphrecords.graphrecord.GraphRecord.groups){target="_blank"} : Lists the groups in the GraphRecord instance.
- [`node[]`](graphrecords.graphrecord.GraphRecord.node){target="_blank"} : Provides access to node information within the GraphRecord instance via an indexer, returning a dictionary with node indices as keys and node attributes as values.
- [`groups_of_node()`](graphrecords.graphrecord.GraphRecord.groups_of_node){target="_blank"} : Retrieves the groups associated with the specified node(s) in the GraphRecord.
- [`edge_endpoints()`](graphrecords.graphrecord.GraphRecord.edge_endpoints){target="_blank"} : Retrieves the source and target nodes of the specified edge(s) in the GraphRecord.

:::



## Full example Code

The full code examples for this chapter can be found here:

```{literalinclude} scripts/query_engine.py
---
language: python
lines: 2-55, 66-76, 154-175, 118-127, 216-223
---
```
