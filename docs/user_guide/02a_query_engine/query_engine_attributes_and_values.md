# Attributes & Values

One of the main strengths of the query engine is the possibility of diving deeper into the GraphRecords' attributes and values. We can access them by using different return types - in the [Query Engine Introduction](index.md), we mainly used *indices* as the return type for each query.

## Inspecting Attributes Names

Each node can have a variety of different attributes, each one of the holding an assigned [`GraphRecordValue`](graphrecords.types.GraphRecordValue){target="_blank"}. We can look at the attributes of each node by using the method [`attributes()`](graphrecords.querying.NodeOperand.attributes){target="_blank"}:

```{exec-literalinclude} scripts/attributes_and_values.py
---
language: python
setup-lines: 1-57
lines: 60-66
---
```

:::{dropdown} Methods used in the snippet

- [`in_group()`](graphrecords.querying.NodeOperand.in_group){target="_blank"} : Query nodes that belong to that group.
- [`attributes()`](graphrecords.querying.NodeOperand.attributes){target="_blank"} : Query the attribute names of each node.
- [`query_nodes()`](graphrecords.graphrecord.GraphRecord.query_nodes){target="_blank"} : Retrieves information on the nodes from the GraphRecord given the query.

:::

You can also do operations on them, like checking how many attributes each node has, thanks to the [`count()`](graphrecords.querying.NodeMultipleAttributesWithIndexOperand.count){target="_blank"} method:

```{exec-literalinclude} scripts/attributes_and_values.py
---
language: python
setup-lines: 1-57
lines: 69-78
---
```

:::{dropdown} Methods used in the snippet

- [`in_group()`](graphrecords.querying.NodeOperand.in_group){target="_blank"} : Query nodes that belong to that group.
- [`attributes()`](graphrecords.querying.NodeOperand.attributes){target="_blank"} : Query the attribute names of each node.
- [`count()`](graphrecords.querying.NodeMultipleAttributesWithIndexOperand.count){target="_blank"} : Query how many attributes each node has.
- [`query_nodes()`](graphrecords.graphrecord.GraphRecord.query_nodes){target="_blank"} : Retrieves information on the nodes from the GraphRecord given the query.

:::


## Inspecting Attributes Values

As said before we can look for specific values within our GraphRecord, using the method [`attribute()`](graphrecords.querying.NodeOperand.attribute){target="_blank"}. For instance, we can search for the maximum `age` in our users, and we will get the node ID of the user with the highest age, and what that age is.

```{exec-literalinclude} scripts/attributes_and_values.py
---
language: python
setup-lines: 1-57
lines: 81-89
---
```

:::{dropdown} Methods used in the snippet

- [`attribute()`](graphrecords.querying.NodeOperand.attribute){target="_blank"} : Returns a [`NodeMultipleValuesWithIndexOperand()`](graphrecords.querying.NodeMultipleValuesWithIndexOperand){target="_blank"} to query on the values of the nodes for that attribute.
- [`max()`](graphrecords.querying.NodeMultipleValuesWithIndexOperand.max){target="_blank"}: Returns a [`NodeSingleValueWithIndexOperand()`](graphrecords.querying.NodeSingleValueWithIndexOperand){target="_blank"} holding the node index and value pair holding the maximum value for that attribute.
- [`query_nodes()`](graphrecords.graphrecord.GraphRecord.query_nodes){target="_blank"} : Retrieves information on the nodes from the GraphRecord given the query.

:::

## Advanced Query Operations

In case, for instance, that you do not know whether there are different ways to assign the `gender` attribute across the [`GraphRecord`](graphrecords.graphrecord.GraphRecord){target="_blank"} (with leading/trailing whitespaces or formatted in lower/uppercase), you can modify the value of the attributes of a node/edge inside the query.

:::{note}

It is important to note that modifying these values **does not** change the actual value of the attributes within the [`GraphRecord`](graphrecords.graphrecord.GraphRecord){target="_blank"}: it just changes the value of those variables in the query.

:::

You can also perform mathematical calculations like [`mean()`](graphrecords.querying.NodeMultipleValuesWithIndexOperand.mean){target="_blank"}, [`median()`](graphrecords.querying.NodeMultipleValuesWithIndexOperand.median){target="_blank"} or [`min()`](graphrecords.querying.NodeMultipleValuesWithIndexOperand.min){target="_blank"} and assign them to a variable. Also, you can keep manipulating the operand, like in the following example, where we are subtracting _5_ years from the `mean_age` to query on that value.

In the result, we can see the only user whose age is less than five years under the mean age and what that value is.


```{exec-literalinclude} scripts/attributes_and_values.py
---
language: python
setup-lines: 1-57
lines: 93-112
---
```

:::{dropdown} Methods used in the snippet

- [`in_group()`](graphrecords.querying.NodeOperand.in_group){target="_blank"} : Query nodes that belong to that group.
- [`index()`](graphrecords.querying.NodeOperand.index){target="_blank"}: Returns a [`NodeIndicesOperand`](graphrecords.querying.NodeIndicesOperand){target="_blank"} representing the indices of the nodes queried.
- [`contains()`](graphrecords.querying.NodeIndexOperand.contains){target="_blank"} : Query node indices containing that argument.
- [`attribute()`](graphrecords.querying.NodeOperand.attribute){target="_blank"} : Returns a [`NodeMultipleValuesWithIndexOperand`](graphrecords.querying.NodeMultipleValuesWithIndexOperand){target="_blank"} to query on the values of the nodes for that attribute.
- [`lowercase()`](graphrecords.querying.NodeMultipleValuesWithIndexOperand.lowercase){target="_blank"} : Converts the values that are strings to lowercase.
- [`trim()`](graphrecords.querying.NodeMultipleValuesWithIndexOperand.trim){target="_blank"} : Removes leading and trailing whitespacing from the values.
- [`equal_to()`](graphrecords.querying.NodeMultipleValuesWithIndexOperand.equal_to){target="_blank"} : Query values equal to that value.
- [`has_attribute()`](graphrecords.querying.NodeOperand.has_attribute){target="_blank"} : Query nodes that have that attribute.
- [`mean()`](graphrecords.querying.NodeMultipleValuesWithIndexOperand.mean){target="_blank"}: Returns a [`NodeSingleValueWithoutIndexOperand`](graphrecords.querying.NodeSingleValueWithoutIndexOperand){target="_blank"} containing the mean of those values.
- [`subtract()`](graphrecords.querying.NodeSingleValueWithoutIndexOperand.subtract){target="_blank"} : Subtract the argument from the single value operand.
- [`less_than()`](graphrecords.querying.NodeMultipleValuesWithIndexOperand.less_than){target="_blank"} : Query values that are less than that value.
- [`query_nodes()`](graphrecords.graphrecord.GraphRecord.query_nodes){target="_blank"} : Retrieves information on the nodes from the GraphRecord given the query.

:::

:::{note}
Query methods used for changing the operands cannot be assigned to variables for further querying, since their return type is `None`. The following code snippet shows an example, where the variable `gender_lowercase` evaluates to None. An `AttributeError` is thrown as a consequence when trying to further query with the `equal_to` querying method:

```{exec-literalinclude} scripts/attributes_and_values.py
---
language: python
setup-lines: 1-57
lines: 116-125
expect-error: PanicException
---
```

The concatenation of querying methods also throws an error:

```{exec-literalinclude} scripts/attributes_and_values.py
---
language: python
setup-lines: 1-57
lines: 129-137
expect-error: PanicException
---
```

**Correct implementation**:

```{exec-literalinclude} scripts/attributes_and_values.py
---
language: python
setup-lines: 1-57
lines: 141-150
---
```

:::

## Full example Code

The full code examples for this chapter can be found here:

```{literalinclude} scripts/attributes_and_values.py
---
language: python
lines: 3-150
---
```
