# pyright: reportAttributeAccessIssue=false
# ruff: noqa: D100, D103
import pandas as pd

import graphrecords as gr
from graphrecords.querying import (
    NodeAttributesTreeOperand,
    NodeIndicesOperand,
    NodeMultipleAttributesWithIndexOperand,
    NodeMultipleValuesWithIndexOperand,
    NodeOperand,
    NodeSingleValueWithIndexOperand,
    NodeSingleValueWithoutIndexOperand,
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


def query_node_attribute_names(node: NodeOperand) -> NodeAttributesTreeOperand:
    node.in_group("user")

    return node.attributes()


graphrecord.query_nodes(query_node_attribute_names)


def query_node_attributes_count(
    node: NodeOperand,
) -> NodeMultipleAttributesWithIndexOperand:
    node.in_group("user")
    attributes = node.attributes()

    return attributes.count()


graphrecord.query_nodes(query_node_attributes_count)


def query_node_max_age(
    node: NodeOperand,
) -> NodeSingleValueWithIndexOperand:
    age = node.attribute("age")

    return age.max()


graphrecord.query_nodes(query_node_max_age)


# Advanced node query
def query_node_male_user_under_mean(
    node: NodeOperand,
) -> tuple[NodeMultipleValuesWithIndexOperand, NodeSingleValueWithoutIndexOperand]:
    node.in_group("user")
    node.index().contains("pat")

    gender = node.attribute("gender")
    gender.lowercase()  # Converts the string to lowercase
    gender.trim()  # Removes leading and trailing whitespaces
    gender.equal_to("m")

    node.has_attribute("age")
    mean_age = node.attribute("age").mean()
    mean_age.subtract(5)  # Subtract 5 from the mean age
    node.attribute("age").less_than(mean_age)

    return node.attribute("age"), mean_age


graphrecord.query_nodes(query_node_male_user_under_mean)


# Incorrect implementation because the querying methods are assigned to a variable
def query_operand_assigned(node: NodeOperand) -> NodeIndicesOperand:
    gender_lowercase = node.attribute(
        "gender"
    ).lowercase()  # Assigning the querying method to a variable
    gender_lowercase.equal_to("m")

    return node.index()


graphrecord.query_nodes(query_operand_assigned)


# Incorrect implementation because the querying methods are concatenated
def query_operands_concatenated(node: NodeOperand) -> NodeIndicesOperand:
    gender = node.attribute("gender")
    gender.lowercase().trim()  # Concatenating the querying methods
    gender.equal_to("m")

    return node.index()


graphrecord.query_nodes(query_operands_concatenated)


# Correct implementation
def query_correct_implementation(node: NodeOperand) -> NodeIndicesOperand:
    gender = node.attribute("gender")
    gender.lowercase()
    gender.trim()
    gender.equal_to("m")

    return node.index()


graphrecord.query_nodes(query_correct_implementation)
