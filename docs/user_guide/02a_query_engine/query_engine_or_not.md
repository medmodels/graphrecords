# OR & NOT operations

The inherent structure of the query engine works with logical **AND** operations. However, a complete query engine should also include **OR** and **NOT** operations to be able to address all scenarios. For that the methods [`exclude()`](graphrecords.querying.NodeOperand.exclude){target="_blank"} and [`either_or()`](graphrecords.querying.NodeOperand.either_or){target="_blank"} are included.

```{exec-literalinclude} scripts/query_engine.py
---
language: python
setup-lines: 2-53
lines: 154-176
---
```

:::{dropdown} Methods used in the snippet

- [`in_group()`](graphrecords.querying.EdgeOperand.in_group){target="_blank"} : Query edges that belong to that group.
- [`attribute()`](graphrecords.querying.EdgeOperand.attribute){target="_blank"} : Returns a [`EdgeMultipleValuesWithIndexOperand()`](graphrecords.querying.EdgeMultipleValuesWithIndexOperand){target="_blank"} to query on the values of the edges for that attribute.
- [`less_than()`](graphrecords.querying.EdgeMultipleValuesWithIndexOperand.less_than){target="_blank"} : Query values that are less than that value.
- [`equal_to()`](graphrecords.querying.EdgeMultipleValuesWithIndexOperand.equal_to){target="_blank"} : Query values that are equal to that value.
- [`in_group()`](graphrecords.querying.NodeOperand.in_group){target="_blank"} : Query nodes that belong to that group.
- [`attribute()`](graphrecords.querying.NodeOperand.attribute){target="_blank"} : Returns a [`NodeMultipleValuesWithIndexOperand()`](graphrecords.querying.NodeMultipleValuesWithIndexOperand){target="_blank"} to query on the values of the nodes for that attribute.
- [`greater_than()`](graphrecords.querying.NodeMultipleValuesWithIndexOperand.greater_than){target="_blank"} : Query values that are greater than that value.
- [`edges()`](graphrecords.querying.NodeOperand.edges){target="_blank"} : Returns a [`EdgeOperand()`](graphrecords.querying.EdgeOperand){target="_blank"} to query on the edges of those nodes.
- [`either_or()`](graphrecords.querying.NodeOperand.either_or){target="_blank"} : Queries edges that match either one or the other given queries.
- [`index()`](graphrecords.querying.NodeOperand.index){target="_blank"}: Returns a [`NodeIndicesOperand`](graphrecords.querying.NodeIndicesOperand){target="_blank"} representing the indices of the nodes queried.
- [`query_nodes()`](graphrecords.graphrecord.GraphRecord.query_nodes){target="_blank"} : Retrieves information on the nodes from the GraphRecord given the query.

:::

This includes also _"pat_3"_, that was not included in the previous section because none of its edges was included in the `query_edge_either()`, but it can be found in the `query_edge_or()` now.

```{exec-literalinclude} scripts/query_engine.py
---
language: python
setup-lines: 1-53, 154-173
lines: 179-194
---
```

:::{dropdown} Methods used in the snippet

- [`in_group()`](graphrecords.querying.EdgeOperand.in_group){target="_blank"} : Query edges that belong to that group.
- [`exclude()`](graphrecords.querying.NodeOperand.exclude){target="_blank"} : Exclude the nodes that belong to the given query.
- [`index()`](graphrecords.querying.NodeOperand.index){target="_blank"}: Returns a [`NodeIndicesOperand`](graphrecords.querying.NodeIndicesOperand){target="_blank"} representing the indices of the nodes queried.
- [`query_nodes()`](graphrecords.graphrecord.GraphRecord.query_nodes){target="_blank"} : Retrieves information on the nodes from the GraphRecord given the query.
:::

So this gives us all the user nodes that were not selected with the previous query (logical **NOT** applied).

## Full example Code

The full code examples for this chapter can be found here:

```{literalinclude} scripts/query_engine.py
---
language: python
lines: 2-55, 154-194
---
```
