# ruff: noqa: D100, D103
import pandas as pd

import graphrecords as gr
from graphrecords.querying import (
    EdgeIndexGroupOperand,
    EdgeMultipleValuesWithIndexGroupOperand,
    EdgeOperand,
    EdgeOperandGroupDiscriminator,
    NodeMultipleValuesWithIndexGroupOperand,
    NodeOperand,
    NodeOperandGroupDiscriminator,
    NodeSingleValueWithoutIndexGroupOperand,
)

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


def query_node_group_by_gender(
    node: NodeOperand,
) -> NodeMultipleValuesWithIndexGroupOperand:
    grouped_nodes = node.group_by(NodeOperandGroupDiscriminator.Attribute("gender"))

    return grouped_nodes.attribute("age")


graphrecord.query_nodes(query_node_group_by_gender)


def query_node_group_by_gender_mean(
    node: NodeOperand,
) -> NodeSingleValueWithoutIndexGroupOperand:
    grouped_nodes = node.group_by(NodeOperandGroupDiscriminator.Attribute("gender"))
    age_groups = grouped_nodes.attribute("age")

    return age_groups.mean()


graphrecord.query_nodes(query_node_group_by_gender_mean)


def query_edge_group_by_source_node(
    edge: EdgeOperand,
) -> EdgeMultipleValuesWithIndexGroupOperand:
    edge.index().less_than(20)
    grouped_edges = edge.group_by(EdgeOperandGroupDiscriminator.SourceNode())

    return grouped_edges.attribute("time")


graphrecord.query_edges(query_edge_group_by_source_node)


def query_edge_group_by_count_edges(edge: EdgeOperand) -> EdgeIndexGroupOperand:
    grouped_edges = edge.group_by(EdgeOperandGroupDiscriminator.SourceNode())

    return grouped_edges.index().count()


graphrecord.query_edges(query_edge_group_by_count_edges)
