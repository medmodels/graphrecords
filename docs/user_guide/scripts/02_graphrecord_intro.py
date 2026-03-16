# ruff: noqa: D100, B018
import pandas as pd
import polars as pl

import graphrecords as gr

# Users DataFrame (Nodes)
users = pd.DataFrame(
    [
        ["User 01", 72, "M", "USA"],
        ["User 02", 74, "M", "USA"],
        ["User 03", 64, "F", "GER"],
    ],
    columns=["ID", "Age", "Type", "Region"],
)

# Products DataFrame (Nodes)
products = pd.DataFrame(
    [["Product 01", "Item A"], ["Product 02", "Item B"]], columns=["ID", "Name"]
)

# User-Product Relation (Edges)
user_product = pd.DataFrame(
    [
        ["User 02", "Product 01", pd.Timestamp("20200607")],
        ["User 02", "Product 02", pd.Timestamp("20180202")],
        ["User 03", "Product 02", pd.Timestamp("20190302")],
    ],
    columns=["User_ID", "Product_ID", "Date"],
)

record = gr.GraphRecord.builder().add_nodes((users, "ID"), group="Users").build()

record.add_nodes((products, "ID"), group="Products")

user_tuples = [
    ("User 04", {"Age": 45, "Type": "F", "Region": "CHI"}),
    ("User 05", {"Age": 26, "Type": "M", "Region": "SPA"}),
]
record.add_nodes(user_tuples, group="Users")

user_polars = pl.DataFrame(
    [
        ["User 06", 55, "F", "GER"],
        ["User 07", 61, "F", "USA"],
        ["User 08", 73, "M", "CHI"],
    ],
    schema=["ID", "Age", "Type", "Region"],
    orient="row",
)
record.add_nodes((user_polars, "ID"), group="Users")

record.add_edges((user_product, "User_ID", "Product_ID"))

record.add_group("US-Users", nodes=["User 01", "User 02"])

record.add_nodes(
    (
        pd.DataFrame(
            [["User 09", 65, "M", "USA"]], columns=["ID", "Age", "Type", "Region"]
        ),
        "ID",
    ),
)

record.overview()

# Adding edges to a certain group
record.add_group("User-Product", edges=record.edges)

# Getting all available nodes
record.nodes

# Accessing a certain node
record.node["User 01"]

# Accessing a certain edge
record.edge[0]

# Getting all available groups
record.groups

# Getting the nodes that are within a certain group
record.nodes_in_group("Products")

record.to_ron("record.ron")
new_record = gr.GraphRecord.from_ron("record.ron")
