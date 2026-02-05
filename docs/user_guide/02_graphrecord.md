# GraphRecord

```{toctree}
:maxdepth: 2
:caption: Contents:
:hidden:
02a_query_engine/index
02b_schema
```

## Preface

Every major library has a central object that constitutes its core. For [PyTorch](https://pytorch.org/), it is the `torch.Tensor`, whereas for [Numpy](https://numpy.org/), it is the `np.array`. In our case, GraphRecords centres around the `GraphRecord` as its foundational structure.

GraphRecords delivers advanced data analytics methods out-of-the-box by utilizing a structured approach to data storage. This is enabled by the [`GraphRecord`](graphrecords.graphrecord.GraphRecord){target="\_blank"} class, which organizes data of any complexity within a graph structure. With its Rust backend implementation, GraphRecord guarantees high performance, even when working with extremely large datasets.

```{literalinclude} scripts/02_graphrecord_intro.py
---
language: python
lines: 5
---
```

## Adding Nodes to a GraphRecord

Let's begin by introducing some sample data:

:::{list-table} Users
:widths: 15 15 15 15
:header-rows: 1

- - ID
  - Age
  - Type
  - Region
- - User 01
  - 72
  - M
  - USA
- - User 02
  - 74
  - M
  - USA
- - User 03
  - 64
  - F
  - GER
:::

This data, stored for example in a Pandas DataFrame, looks like this:

```{literalinclude} scripts/02_graphrecord_intro.py
---
language: python
lines: 8-15
---
```

In the example below, we create a new GraphRecord using the builder pattern. We instantiate a [`GraphRecordBuilder`](graphrecords.builder.GraphRecordBuilder){target="_blank"} and instruct it to add the Pandas DataFrame as nodes, using the _'ID'_ column for indexing. Additionally, we assign these nodes to the group 'Users'.
The Builder Pattern simplifies creating complex objects by constructing them step by step. It improves flexibility, readability, and consistency, making it easier to manage and configure objects in a controlled way.

```{literalinclude} scripts/02_graphrecord_intro.py
---
language: python
lines: 32
---
```

:::{dropdown} Methods used in the snippet

- [`builder()`](graphrecords.graphrecord.GraphRecord.builder){target="\_blank"} : Creates a new [`GraphRecordBuilder`](graphrecords.builder.GraphRecordBuilder){target="\_blank"} instance to build a [`GraphRecord`](graphrecords.graphrecord.GraphRecord){target="\_blank"}.
- [`add_nodes()`](graphrecords.builder.GraphRecordBuilder.add_nodes){target="\_blank"} : Adds nodes to the GraphRecord from different data formats and optionally assigns them to a group.
- [`build()`](graphrecords.builder.GraphRecordBuilder.build){target="\_blank"} : Constructs a GraphRecord instance from the builder's configuration.
  :::

The GraphRecords GraphRecord object, `record`, now contains three users. Each user is identified by a unique index and has specific attributes, such as age, type, and region. These users serve as the initial nodes in the graph structure of our GraphRecord.

We can now proceed by adding additional data, such as the following products.

```{literalinclude} scripts/02_graphrecord_intro.py
---
language: python
lines: 18-20
---
```

Using the builder pattern to construct the GraphRecord allows us to pass as many nodes and edges as needed. If nodes are not added during the initial graph construction, they can easily be added later to an existing GraphRecord by calling [`add_nodes()`](graphrecords.builder.GraphRecordBuilder.add_nodes){target="\_blank"}, where you provide the DataFrame and specify the column containing the node indices.

```{literalinclude} scripts/02_graphrecord_intro.py
---
language: python
lines: 34
---
```

:::{dropdown} Methods used in the snippet

- [`add_nodes()`](graphrecords.graphrecord.GraphRecord.add_nodes){target="\_blank"} : Adds nodes to the GraphRecord from different data formats and optionally assigns them to a group.
  :::

This will expand the GraphRecord, adding several new nodes to the graph. However, these nodes are not yet connected, so let's establish relationships between them!

:::{note}
Nodes can be added to the GraphRecord in a lot of different formats, such as a Pandas DataFrame (as previously shown), but also from a Polars DataFrame:

```{literalinclude} scripts/02_graphrecord_intro.py
---
language: python
lines: 36-40
---
```

Or from a [`NodeTuple`](graphrecords.types.NodeTuple){target="\_blank"}:

```{literalinclude} scripts/02_graphrecord_intro.py
---
language: python
lines: 42-51
---
```

:::

## Adding Edges to a GraphRecord

To capture meaningful relationships between nodes, such as linking users to purchased products, we add edges to the GraphRecord. These edges must be specified in a relation table, such as the one shown below:

:::{list-table} User-Product Relation
:widths: 15 15 15
:header-rows: 1

- - User_ID
  - Product_ID
  - time
- - User 02
  - Product 01
  - 2020/06/07
- - User 02
  - Product 02
  - 2018/02/02
- - User 03
  - Product 02
  - 2019/03/02
:::

We can add these edges then to our GraphRecord:

```{literalinclude} scripts/02_graphrecord_intro.py
---
language: python
lines: 53
---
```

:::{dropdown} Methods used in the snippet

- [`add_edges()`](graphrecords.graphrecord.GraphRecord.add_edges){target="\_blank"} : Adds edges to the GraphRecord from different data formats and optionally assigns them to a group.

:::

This results in an enlarged Graph with more information.

## Adding Groups to a GraphRecord

For certain analyses, we may want to define specific subcohorts within our GraphRecord for easier access. We can do this by defining named groups withing our GraphRecord.

```{literalinclude} scripts/02_graphrecord_intro.py
---
language: python
lines: 55
---
```

:::{dropdown} Methods used in the snippet

- [`add_group()`](graphrecords.graphrecord.GraphRecord.add_group){target="\_blank"} : Adds a group to the GraphRecord instance with an optional list of node and/or edge indices.

:::

This group will include all the defined nodes, allowing for easier access during complex analyses. Both nodes and edges can be added to a group, with no limitations on group size. Additionally, nodes and edges can belong to multiple groups without restriction.

## Saving and Loading GraphRecords

When building a GraphRecord, you may want to save it to create a persistent version. This can be done by storing it as a RON (Rusty Object Notation) file. The GraphRecord can then be reloaded, allowing you to create a new instance from the saved RON file.

```{literalinclude} scripts/02_graphrecord_intro.py
---
language: python
lines: 86-87
---
```

:::{dropdown} Methods used in the snippet

- [`to_ron()`](graphrecords.graphrecord.GraphRecord.to_ron){target="\_blank"} : Writes the GraphRecord instance to a RON file.
- [`from_ron()`](graphrecords.graphrecord.GraphRecord.from_ron){target="\_blank"} : Creates a GraphRecord instance from a RON file.
  :::

## Overview Tables

The GraphRecord class is designed to efficiently handle large datasets while maintaining a standardized data structure that supports complex analysis methods. As a result, the structure within the GraphRecord can become intricate and difficult to manage. To address this, GraphRecords offers tools to help keep track of the graph-based data. One such tool is the [`overview()`](graphrecords.graphrecord.GraphRecord.overview){target="\_blank"} method, which prints an overview over all nodes and edges in the GraphRecord.

```{exec-literalinclude} scripts/02_graphrecord_intro.py
---
language: python
setup-lines: 1-64
lines: 66
---
```

:::{dropdown} Methods used in the snippet

- [`overview()`](graphrecords.graphrecord.GraphRecord.overview){target="\_blank"} : Gets a summary for all nodes and edges in groups and their attributes.
  :::

## Accessing Elements in a GraphRecord

Now that we have stored some structured data in our GraphRecord, we might want to access certain elements of it. The main way to do this is by either selecting the data with their indices or via groups that they are in.

We can, for example, get all available nodes:

```{exec-literalinclude} scripts/02_graphrecord_intro.py
---
language: python
setup-lines: 1-64
lines: 72
---
```

Or access the attributes of a specific node:

```{exec-literalinclude} scripts/02_graphrecord_intro.py
---
language: python
setup-lines: 1-32
lines: 75
---
```

Or a specific edge:

```{exec-literalinclude} scripts/02_graphrecord_intro.py
---
language: python
setup-lines: 1-54
lines: 78
---
```

Or get all available groups:

```{exec-literalinclude} scripts/02_graphrecord_intro.py
---
language: python
setup-lines: 1-71
lines: 81
---
```

Or get all that nodes belong to a certain group:

```{exec-literalinclude} scripts/02_graphrecord_intro.py
---
language: python
setup-lines: 1-34
lines: 84
---
```

:::{dropdown} Methods used in the snippet

- [`nodes`](graphrecords.graphrecord.GraphRecord.nodes){target="\_blank"} : Lists the node indices in the GraphRecord instance.
- [`node[]`](graphrecords.graphrecord.GraphRecord.node){target="\_blank"} : Provides access to node information within the GraphRecord instance via an indexer, returning a dictionary with node indices as keys and node attributes as values.
- [`edge[]`](graphrecords.graphrecord.GraphRecord.edge){target="\_blank"} : Provides access to edge attributes within the GraphRecord via an indexer, returning a dictionary with edge indices and edge attributes as values.
- [`groups()`](graphrecords.graphrecord.GraphRecord.groups){target="\_blank"} : Lists the groups in the GraphRecord instance.
- [`nodes_in_group()`](graphrecords.graphrecord.GraphRecord.nodes_in_group){target="\_blank"} : Retrieves the node indices associated with the specified group(s) in the GraphRecord.
  :::

The GraphRecord can be queried in very advanced ways in order to find very specific nodes based on time, relations, neighbors or other. These advanced querying methods are covered in one of the next sections of the user guide, [Query Engine](02a_query_engine/index.md).

## Full example Code

The full code examples for this chapter can be found here:

```{literalinclude} scripts/02_graphrecord_intro.py
---
language: python
lines: 2-87
---
```
