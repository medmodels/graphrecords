<div align="center">
    <img src="https://raw.githubusercontent.com/medmodels/graphrecords-static/refs/heads/main/logos/logo_with_background.svg" alt="GraphRecords Logo">
</div>

<br>

<div align="center">
  <img alt="Python Versions" src="https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue">
  <a href="https://github.com/medmodels/graphrecords/blob/main/LICENSE">
    <img alt="GraphRecords License" src="https://img.shields.io/github/license/medmodels/graphrecords.svg">
  </a>
  <a href="https://github.com/medmodels/graphrecords/actions/workflows/testing.yml">
    <img src="https://github.com/medmodels/graphrecords/actions/workflows/testing.yml/badge.svg?branch=main" alt="Tests">
  </a>
  <img alt="Coverage percentage" src="https://raw.githubusercontent.com/medmodels/graphrecords-static/refs/heads/main/icons/coverage-badge.svg">
  <a href="https://pypi.org/project/graphrecords/">
    <img src="https://img.shields.io/pypi/v/graphrecords" alt="PyPI Version">
  </a>
  <a href="https://github.com/astral-sh/ruff">
    <img alt="Code Style" src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json">
  </a>
</div>

# GraphRecords

GraphRecords stores entities and their relationships as a graph. Nodes hold attributes. Edges connect nodes and can also hold attributes. Groups organize subsets of nodes and edges.

## When to Use GraphRecords

GraphRecords fits problems where:

- Data has natural relationships (users and products, documents and citations, components and dependencies)
- You need to query based on relationships ("find all users connected to products over $100")
- Different entity types have different attributes (users have age, products have price)

## Installation

```bash
pip install graphrecords
```

## Building a Graph

```python
import graphrecords as gr

# Add nodes as tuples: (id, {attributes})
record = gr.GraphRecord()
record.add_nodes(
    [
        ("alice", {"age": 30}),
        ("bob", {"age": 25}),
        ("carol", {"age": 35}),
    ],
    group="users",
)

record.add_nodes(
    [
        ("widget", {"price": 10.0}),
        ("gadget", {"price": 25.0}),
    ],
    group="products",
)

# Add edges as tuples: (source, target, {attributes})
record.add_edges(
    [
        ("alice", "widget", {"quantity": 1}),
        ("bob", "gadget", {"quantity": 2}),
        ("alice", "gadget", {"quantity": 1}),
    ],
    group="purchases",
)
```

You can also use Pandas or Polars DataFrames:

```python
import pandas as pd

users_df = pd.DataFrame({"id": ["alice", "bob"], "age": [30, 25]})
record.add_nodes((users_df, "id"), group="users")

purchases_df = pd.DataFrame(
    {"user": ["alice"], "product": ["widget"], "qty": [1]}
)
record.add_edges((purchases_df, "user", "product"), group="purchases")
```

## Accessing Data

```python
# Get all nodes
record.nodes  # ['alice', 'bob', 'carol', 'widget', 'gadget']

# Get attributes of a node
record.node["alice"]  # {'age': 30}

# Get nodes in a group
record.nodes_in_group("users")  # ['alice', 'bob', 'carol']

# Get edges connected to a node
record.outgoing_edges("alice")  # [0, 2]

# Get edge attributes
record.edge[0]  # {'quantity': 1}
```

## Query Engine

The query engine finds nodes and edges based on their attributes and relationships.

Queries are functions that receive an operand, apply conditions, and return results:

```python
from graphrecords.querying import NodeOperand, NodeIndicesOperand

def users_over_25(node: NodeOperand) -> NodeIndicesOperand:
    node.in_group("users")
    node.attribute("age").greater_than(25)
    return node.index()

record.query_nodes(users_over_25)  # ['alice', 'carol']
```

Queries can follow relationships:

```python
def users_who_bought_expensive_items(node: NodeOperand) -> NodeIndicesOperand:
    node.in_group("users")
    # Follow edges to products, check price
    node.neighbors().attribute("price").greater_than(20)
    return node.index()

record.query_nodes(users_who_bought_expensive_items)  # ['alice', 'bob']
```

Queries can aggregate:

```python
from graphrecords.querying import NodeSingleValueWithoutIndexOperand

def average_user_age(node: NodeOperand) -> NodeSingleValueWithoutIndexOperand:
    node.in_group("users")
    return node.attribute("age").mean()

record.query_nodes(average_user_age)  # 30.0
```

See the [Query Engine Guide](https://www.medmodels.de/docs/graphrecords/latest/user_guide/05_query_engine/index.html) for the full API.

## Schema

Schemas define what attributes are allowed and their types.

**Inferred mode** (default): The schema learns from data as you add it. Any attribute is allowed.

**Provided mode**: The schema is fixed. Data that doesn't match is rejected.

```python
from graphrecords.schema import Schema, GroupSchema
from graphrecords.datatype import Int, String

schema = Schema(
    groups={"users": GroupSchema(nodes={"age": Int, "name": String})}
)

record = gr.GraphRecord.builder().with_schema(schema).build()
record.freeze_schema()  # Switch to provided mode

# Now adding a user without 'age' or 'name' raises an error
```

See the [Schema Guide](https://www.medmodels.de/docs/graphrecords/latest/user_guide/06_schema.html) for details.

## Serialization

Save and load graphs using RON format:

```python
record.to_ron("graph.ron")
loaded = gr.GraphRecord.from_ron("graph.ron")
```

Export to DataFrames:

```python
dataframes = record.to_pandas()  # or record.to_polars()
```

## Documentation

- [User Guide](https://www.medmodels.de/docs/graphrecords/latest/user_guide/index.html)
- [API Reference](https://www.medmodels.de/docs/graphrecords/latest/api/index.html)

## Background

GraphRecords started as `MedRecord` in the [medmodels](https://github.com/limebit/medmodels) library. We realized it has applications beyond the medical domain and published it as a standalone library.

## License

MIT. See [LICENSE](LICENSE).
