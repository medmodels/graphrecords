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

A GraphRecord is immutable. Every method that changes data returns a new GraphRecord and leaves the old one untouched. This makes records safe to share, cheap to snapshot, and free of locks.

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

Since every call returns a new record, building is a chain:

```python
import graphrecords as gr

# Nodes are tuples: (index, {attributes})
record = (
    gr.GraphRecord()
    .add_nodes_in_group(
        [
            ("alice", {"age": 30}),
            ("bob", {"age": 25}),
            ("carol", {"age": 35}),
        ],
        "users",
    )
    .add_nodes_in_group(
        [
            ("widget", {"price": 10.0}),
            ("gadget", {"price": 25.0}),
        ],
        "products",
    )
    # Edges are tuples: (source, target, {attributes})
    .add_edges_in_group(
        [
            ("alice", "widget", {"quantity": 1}),
            ("bob", "gadget", {"quantity": 2}),
            ("alice", "gadget", {"quantity": 1}),
        ],
        "purchases",
    )
)
```

`add_nodes` and `add_edges` do the same without a group.

You can also load Polars DataFrames, naming the index columns:

```python
import polars as pl

users = pl.DataFrame({"id": ["alice", "bob"], "age": [30, 25]})
record = record.add_nodes((users, "id"))

purchases = pl.DataFrame({"user": ["alice"], "product": ["widget"], "qty": [1]})
record = record.add_edges((purchases, "user", "product"))
```

Anything that exports an Arrow stream works the same way.

## Accessing Data

```python
record.node_indices()  # ['alice', 'bob', 'carol', 'widget', 'gadget']

# node() returns a view of one node
alice = record.node("alice")
alice.attribute("age")  # 30
alice.attributes()  # {'age': 30}
alice.neighbors()  # ['widget', 'gadget']
alice.degree()  # 2

# group() returns a view of one group
record.group("users").nodes()  # ['alice', 'bob', 'carol']

# Edge indices are opaque values, not integers
edge_index = record.edge_indices()[0]
record.edge(edge_index).attributes()  # {'quantity': 1}
```

## Querying

Queries are expressions. `gr.nodes()` and `gr.edges()` start free expressions that are not tied to any record; `record.nodes()` binds one to a record, which makes it a series. A series only runs when you call `evaluate()`.

```python
adult_users = record.nodes().filter(
    gr.nodes().in_group("users") & (gr.nodes().attribute("age") > 25)
)
list(adult_users.evaluate())  # ['alice', 'carol']
```

Expressions traverse the graph themselves — the chain reads as the question
it asks:

```python
bulk_buyers = (
    record.edges()
    .filter(gr.edges().in_group("purchases") & (gr.edges().attribute("quantity") >= 2))
    .source_node()
)
list(bulk_buyers.evaluate())  # ['bob']
```

And they aggregate:

```python
record.nodes().filter(gr.nodes().in_group("users")).attribute("age").mean().evaluate()
# 30.0
```

Every operation is checked twice: the type checker rejects operations that do not fit the current shape of the expression while you write, and the engine optimizes the query before running it. `explain()` shows the optimized plan, `explain_unoptimized()` the raw one.

The result of `evaluate()` is consumed by iterating it once. Evaluate again for a fresh result, or collect into a list first.

## Schema

Schemas define what attributes are allowed and their types.

**Inferred mode** (default): The schema learns from data as you add it. Any attribute is allowed.

**Provided mode**: The schema is fixed. Data that doesn't match is rejected.

```python
from graphrecords.schema import AttributeDataType, AttributeType, GroupSchema, Schema
from graphrecords.datatype import Int, String

schema = Schema(
    groups={
        "users": GroupSchema(
            nodes={
                "age": AttributeDataType(Int(), AttributeType.Continuous),
                "name": AttributeDataType(String(), AttributeType.Unstructured),
            }
        )
    }
)

record = gr.GraphRecord.with_schema(schema)
record = record.freeze_schema()  # Switch to provided mode

# Now adding a user without 'age' or 'name' raises an error
```

Schemas are immutable like records: `set_node_attribute`, `add_group`, `freeze`, and the other schema methods all return a new schema.

## Plugins

A plugin hooks into every change made to a record. A hook sees the pending change and can let it through, or return a modified one:

```python
class Audit:
    def on_add_nodes(self, record, payload):
        print(f"adding {len(payload.batch)} nodes")


record = record.add_plugin("audit", Audit())
```

Hooks exist for every change (`on_add_nodes`, `post_remove_edges`, ...). A plugin only pays for the hooks it defines. Returning `None` from a hook applies the change unchanged; returning a payload replaces it, and returning a list of payloads replaces it with several (an empty list drops it). The `post_*` hooks observe the applied result and can veto by raising.

## Serialization

Save and load graphs using RON format:

```python
record.to_ron("graph.ron")
loaded = gr.GraphRecord.from_ron("graph.ron")
```

Export to DataFrames, one pair of tables per group plus the ungrouped rest:

```python
export = record.to_polars()
export["ungrouped"]["nodes"]  # a Polars DataFrame
export["groups"]["users"]["nodes"]

record.to_arrow()  # the same shape, as Arrow tables
```

## Documentation

- [User Guide](https://www.medmodels.de/docs/graphrecords/latest/user_guide/index.html)
- [API Reference](https://www.medmodels.de/docs/graphrecords/latest/api/index.html)

## Background

GraphRecords started as `MedRecord` in the [medmodels](https://github.com/limebit/medmodels) library. We realized it has applications beyond the medical domain and published it as a standalone library.

## License

MIT. See [LICENSE](LICENSE).
