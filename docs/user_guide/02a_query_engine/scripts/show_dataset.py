# ruff: noqa: D100, D103
import pandas as pd

import graphrecords as gr

# Create example dataset manually
users = pd.DataFrame(
    [
        ["pat_0", 20, "M"],
        ["pat_1", 30, "F"],
        ["pat_2", 40, "M"],
        ["pat_3", 50, "F"],
        ["pat_4", 60, "M"],
    ],
    columns=["index", "age", "gender"],
)

products = pd.DataFrame(
    [
        ["drug_0", "fentanyl injection"],
        ["drug_1", "aspirin tablet"],
        ["drug_2", "insulin pen"],
    ],
    columns=["index", "description"],
)

user_product = pd.DataFrame(
    [
        ["pat_0", "drug_0", 100, 1, "2020-01-01"],
        ["pat_1", "drug_0", 150, 2, "2020-02-15"],
        ["pat_1", "drug_1", 50, 1, "2020-03-10"],
        ["pat_2", "drug_1", 75, 12, "2020-04-20"],
        ["pat_2", "drug_2", 200, 1, "2020-05-05"],
        ["pat_3", "drug_2", 180, 12, "2020-06-30"],
        ["pat_4", "drug_0", 120, 1, "2020-07-15"],
        ["pat_4", "drug_1", 60, 2, "2020-08-01"],
    ],
    columns=["source", "target", "cost", "quantity", "time"],
)

graphrecord = (
    gr.GraphRecord.builder()
    .add_nodes((users, "index"), group="user")
    .add_nodes((products, "index"), group="product")
    .add_edges((user_product, "source", "target"), group="user_product")
    .build()
)


# Showing example dataset
def retrieve_example_dataset(
    graphrecord: gr.GraphRecord,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    users_df = pd.DataFrame(
        graphrecord.node[graphrecord.nodes_in_group("user")]
    ).T.sort_index()
    products_df = pd.DataFrame(
        graphrecord.node[graphrecord.nodes_in_group("product")]
    ).T.sort_index()

    user_product_edges = graphrecord.edge[graphrecord.edges_in_group("user_product")]
    for edge in user_product_edges:
        user_product_edges[edge]["source"], user_product_edges[edge]["target"] = (
            graphrecord.edge_endpoints(edge)
        )

    user_product_df = pd.DataFrame(user_product_edges).T.sort_index()
    user_product_df = user_product_df[
        ["source", "target"]
        + [col for col in user_product_df.columns if col not in ["source", "target"]]
    ]

    return users_df, products_df, user_product_df


users_df, products_df, user_product_edges = retrieve_example_dataset(graphrecord)

users_df.head(10)
products_df.head(10)
user_product_edges.head(10)
