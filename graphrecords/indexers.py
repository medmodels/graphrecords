"""Indexers for GraphRecord nodes and edges."""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict, Tuple, Union, overload

from graphrecords.types import (
    AttributeName,
    AttributeNameInputList,
    Attributes,
    AttributesInput,
    EdgeIndex,
    EdgeIndexInputList,
    NodeIndex,
    NodeIndexInputList,
    Value,
    is_attributes,
    is_edge_index,
    is_identifier,
    is_node_index,
    is_value,
)

if TYPE_CHECKING:
    from graphrecords import GraphRecord
    from graphrecords.querying import (
        EdgeQuery,
        EdgesQuery,
        NodeQuery,
        NodesQuery,
    )


class NodeIndexer:
    """Indexer for GraphRecord nodes."""

    _graphrecord: GraphRecord

    def __init__(self, graphrecord: GraphRecord) -> None:
        """Initializes the NodeIndexer object.

        Args:
            graphrecord (GraphRecord): GraphRecord object to index.
        """
        self._graphrecord = graphrecord

    @overload
    def __getitem__(
        self,
        key: Union[
            NodeIndex,
            NodeQuery,
            Tuple[
                Union[NodeIndex, NodeQuery],
                Union[AttributeNameInputList, slice],
            ],
        ],
    ) -> Attributes: ...

    @overload
    def __getitem__(
        self, key: Tuple[Union[NodeIndex, NodeQuery], AttributeName]
    ) -> Value: ...

    @overload
    def __getitem__(
        self,
        key: Union[
            NodeIndexInputList,
            NodesQuery,
            slice,
            Tuple[
                Union[NodeIndexInputList, NodesQuery, slice],
                Union[AttributeNameInputList, slice],
            ],
        ],
    ) -> Dict[NodeIndex, Attributes]: ...

    @overload
    def __getitem__(
        self,
        key: Tuple[Union[NodeIndexInputList, NodesQuery, slice], AttributeName],
    ) -> Dict[NodeIndex, Value]: ...

    def __getitem__(  # noqa: C901
        self,
        key: Union[
            NodeIndex,
            NodeIndexInputList,
            NodeQuery,
            NodesQuery,
            slice,
            Tuple[
                Union[
                    NodeIndex,
                    NodeIndexInputList,
                    NodeQuery,
                    NodesQuery,
                    slice,
                ],
                Union[AttributeName, AttributeNameInputList, slice],
            ],
        ],
    ) -> Union[
        Value,
        Attributes,
        Dict[NodeIndex, Attributes],
        Dict[NodeIndex, Value],
    ]:
        """Gets the node attributes for the specified key.

        Args:
            key (Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery, slice, Tuple[Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery, slice], Union[AttributeName, AttributeNameInputList, slice]]):
                The nodes to get attributes for.

        Returns:
            Union[Value, Attributes, Dict[NodeIndex, Attributes], Dict[NodeIndex, Value]]:
                The node attributes to be extracted.

        Raises:
            ValueError: If the key is a slice, but not ":" is provided.
            IndexError: If the query returned no results.
        """  # noqa: W505
        if is_node_index(key):
            return self._graphrecord._graphrecord.node([key])[key]

        if isinstance(key, list):
            return self._graphrecord._graphrecord.node(key)

        if isinstance(key, Callable):
            query_result = self._graphrecord._query_node_indices(key)

            if isinstance(query_result, list):
                return self._graphrecord._graphrecord.node(query_result)
            if query_result is not None:
                return self._graphrecord._graphrecord.node([query_result])[query_result]

            msg = "The query returned no results"
            raise IndexError(msg)

        if isinstance(key, slice):
            if key.start is not None or key.stop is not None or key.step is not None:
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            return self._graphrecord._graphrecord.node(self._graphrecord.nodes)

        index_selection, attribute_selection = key

        if is_node_index(index_selection) and is_identifier(attribute_selection):
            return self._graphrecord._graphrecord.node([index_selection])[
                index_selection
            ][attribute_selection]

        if isinstance(index_selection, list) and is_identifier(attribute_selection):
            attributes = self._graphrecord._graphrecord.node(index_selection)

            return {x: attributes[x][attribute_selection] for x in attributes}

        if isinstance(index_selection, Callable) and is_identifier(attribute_selection):
            query_result = self._graphrecord._query_node_indices(index_selection)
            if isinstance(query_result, list):
                attributes = self._graphrecord._graphrecord.node(query_result)

                return {x: attributes[x][attribute_selection] for x in attributes}
            if query_result is not None:
                return self._graphrecord._graphrecord.node([query_result])[
                    query_result
                ][attribute_selection]

            msg = "The query returned no results"
            raise IndexError(msg)

        if isinstance(index_selection, slice) and is_identifier(attribute_selection):
            if (
                index_selection.start is not None
                or index_selection.stop is not None
                or index_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            attributes = self._graphrecord._graphrecord.node(self._graphrecord.nodes)

            return {x: attributes[x][attribute_selection] for x in attributes}

        if is_node_index(index_selection) and isinstance(attribute_selection, list):
            return {
                x: self._graphrecord._graphrecord.node([index_selection])[
                    index_selection
                ][x]
                for x in attribute_selection
            }

        if isinstance(index_selection, list) and isinstance(attribute_selection, list):
            attributes = self._graphrecord._graphrecord.node(index_selection)

            return {
                x: {y: attributes[x][y] for y in attribute_selection}
                for x in attributes
            }

        if isinstance(index_selection, Callable) and isinstance(
            attribute_selection, list
        ):
            query_result = self._graphrecord._query_node_indices(index_selection)

            if isinstance(query_result, list):
                attributes = self._graphrecord._graphrecord.node(query_result)

                return {
                    x: {y: attributes[x][y] for y in attribute_selection}
                    for x in attributes
                }
            if query_result is not None:
                return {
                    x: self._graphrecord._graphrecord.node([query_result])[
                        query_result
                    ][x]
                    for x in attribute_selection
                }

            msg = "The query returned no results"
            raise IndexError(msg)

        if isinstance(index_selection, slice) and isinstance(attribute_selection, list):
            if (
                index_selection.start is not None
                or index_selection.stop is not None
                or index_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            attributes = self._graphrecord._graphrecord.node(self._graphrecord.nodes)

            return {
                x: {y: attributes[x][y] for y in attribute_selection}
                for x in attributes
            }

        if is_node_index(index_selection) and isinstance(attribute_selection, slice):
            if (
                attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            return self._graphrecord._graphrecord.node([index_selection])[
                index_selection
            ]

        if isinstance(index_selection, list) and isinstance(attribute_selection, slice):
            if (
                attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            return self._graphrecord._graphrecord.node(index_selection)

        if isinstance(index_selection, Callable) and isinstance(
            attribute_selection, slice
        ):
            if (
                attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            query_result = self._graphrecord._query_node_indices(index_selection)

            if isinstance(query_result, list):
                return self._graphrecord._graphrecord.node(query_result)
            if query_result is not None:
                return self._graphrecord._graphrecord.node([query_result])[query_result]

            msg = "The query returned no results"
            raise IndexError(msg)

        if isinstance(index_selection, slice) and isinstance(
            attribute_selection, slice
        ):
            if (
                index_selection.start is not None
                or index_selection.stop is not None
                or index_selection.step is not None
                or attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            return self._graphrecord._graphrecord.node(self._graphrecord.nodes)

        msg = "Should never be reached"
        raise NotImplementedError(msg)

    @overload
    def __setitem__(
        self,
        key: Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery, slice],
        value: AttributesInput,
    ) -> None: ...

    @overload
    def __setitem__(
        self,
        key: Tuple[
            Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery, slice],
            Union[AttributeName, AttributeNameInputList, slice],
        ],
        value: Value,
    ) -> None: ...

    def __setitem__(  # noqa: C901
        self,
        key: Union[
            NodeIndex,
            NodeIndexInputList,
            NodeQuery,
            NodesQuery,
            slice,
            Tuple[
                Union[
                    NodeIndex,
                    NodeIndexInputList,
                    NodeQuery,
                    NodesQuery,
                    slice,
                ],
                Union[AttributeName, AttributeNameInputList, slice],
            ],
        ],
        value: Union[AttributesInput, Value],
    ) -> None:
        """Sets the specified node attributes.

        Args:
            key (Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery, slice, Tuple[Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery, slice], Union[AttributeName, AttributeNameInputList, slice]]):
                The nodes to set attributes for.
            value (Union[AttributesInput, Value]): The values to set.

        Raises:
            ValueError: If there is a wrong value type or the key is a slice, but no ":"
                is provided.
        """  # noqa: W505
        if is_node_index(key):
            if not is_attributes(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            return self._graphrecord._graphrecord.replace_node_attributes([key], value)

        if isinstance(key, list):
            if not is_attributes(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            return self._graphrecord._graphrecord.replace_node_attributes(key, value)

        if isinstance(key, Callable):
            if not is_attributes(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            query_result = self._graphrecord._query_node_indices(key)

            if isinstance(query_result, list):
                return self._graphrecord._graphrecord.replace_node_attributes(
                    query_result, value
                )
            if query_result is not None:
                return self._graphrecord._graphrecord.replace_node_attributes(
                    [query_result], value
                )

            return None

        if isinstance(key, slice):
            if key.start is not None or key.stop is not None or key.step is not None:
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            if not is_attributes(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            return self._graphrecord._graphrecord.replace_node_attributes(
                self._graphrecord.nodes, value
            )

        index_selection, attribute_selection = key

        if is_node_index(index_selection) and is_identifier(attribute_selection):
            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            return self._graphrecord._graphrecord.update_node_attribute(
                [index_selection], attribute_selection, value
            )

        if isinstance(index_selection, list) and is_identifier(attribute_selection):
            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            return self._graphrecord._graphrecord.update_node_attribute(
                index_selection, attribute_selection, value
            )

        if isinstance(index_selection, Callable) and is_identifier(attribute_selection):
            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            query_result = self._graphrecord._query_node_indices(index_selection)

            if isinstance(query_result, list):
                return self._graphrecord._graphrecord.update_node_attribute(
                    query_result, attribute_selection, value
                )
            if query_result is not None:
                return self._graphrecord._graphrecord.update_node_attribute(
                    [query_result], attribute_selection, value
                )

            return None

        if isinstance(index_selection, slice) and is_identifier(attribute_selection):
            if (
                index_selection.start is not None
                or index_selection.stop is not None
                or index_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            return self._graphrecord._graphrecord.update_node_attribute(
                self._graphrecord.nodes,
                attribute_selection,
                value,
            )

        if is_node_index(index_selection) and isinstance(attribute_selection, list):
            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            for attribute in attribute_selection:
                self._graphrecord._graphrecord.update_node_attribute(
                    [index_selection], attribute, value
                )

            return None

        if isinstance(index_selection, list) and isinstance(attribute_selection, list):
            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            for attribute in attribute_selection:
                self._graphrecord._graphrecord.update_node_attribute(
                    index_selection, attribute, value
                )

            return None

        if isinstance(index_selection, Callable) and isinstance(
            attribute_selection, list
        ):
            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            query_result = self._graphrecord._query_node_indices(index_selection)

            if isinstance(query_result, list):
                for attribute in attribute_selection:
                    self._graphrecord._graphrecord.update_node_attribute(
                        query_result, attribute, value
                    )
                return None
            if query_result is not None:
                for attribute in attribute_selection:
                    self._graphrecord._graphrecord.update_node_attribute(
                        [query_result], attribute, value
                    )
                return None

            return None

        if isinstance(index_selection, slice) and isinstance(attribute_selection, list):
            if (
                index_selection.start is not None
                or index_selection.stop is not None
                or index_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            for attribute in attribute_selection:
                self._graphrecord._graphrecord.update_node_attribute(
                    self._graphrecord.nodes, attribute, value
                )

            return None

        if is_node_index(index_selection) and isinstance(attribute_selection, slice):
            if (
                attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            attributes = self._graphrecord._graphrecord.node([index_selection])[
                index_selection
            ]

            for attribute in attributes:
                self._graphrecord._graphrecord.update_node_attribute(
                    [index_selection],
                    attribute,
                    value,
                )

            return None

        if isinstance(index_selection, list) and isinstance(attribute_selection, slice):
            if (
                attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            attributes = self._graphrecord._graphrecord.node(index_selection)

            for node in attributes:
                for attribute in attributes[node]:
                    self._graphrecord._graphrecord.update_node_attribute(
                        [node], attribute, value
                    )

            return None

        if isinstance(index_selection, Callable) and isinstance(
            attribute_selection, slice
        ):
            if (
                attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            query_result = self._graphrecord._query_node_indices(index_selection)

            if isinstance(query_result, list):
                attributes = self._graphrecord._graphrecord.node(query_result)

                for node in attributes:
                    for attribute in attributes[node]:
                        self._graphrecord._graphrecord.update_node_attribute(
                            [node], attribute, value
                        )
            elif query_result is not None:
                attributes = self._graphrecord._graphrecord.node([query_result])[
                    query_result
                ]

                for attribute in attributes:
                    self._graphrecord._graphrecord.update_node_attribute(
                        [query_result],
                        attribute,
                        value,
                    )

            return None

        if isinstance(index_selection, slice) and isinstance(
            attribute_selection, slice
        ):
            if (
                index_selection.start is not None
                or index_selection.stop is not None
                or index_selection.step is not None
                or attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            attributes = self._graphrecord._graphrecord.node(self._graphrecord.nodes)

            for node in attributes:
                for attribute in attributes[node]:
                    self._graphrecord._graphrecord.update_node_attribute(
                        [node], attribute, value
                    )

            return None

        msg = "Should never be reached"
        raise NotImplementedError(msg)

    def __delitem__(  # noqa: C901
        self,
        key: Tuple[
            Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery, slice],
            Union[AttributeName, AttributeNameInputList, slice],
        ],
    ) -> None:
        """Deletes the specified node attributes.

        Args:
            key (Tuple[Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery, slice], Union[AttributeName, AttributeNameInputList, slice]]):
                The key to delete.

        Raises:
            ValueError: If the key is a slice, but not ":" is provided.
        """  # noqa: W505
        index_selection, attribute_selection = key

        if is_node_index(index_selection) and is_identifier(attribute_selection):
            return self._graphrecord._graphrecord.remove_node_attribute(
                [index_selection], attribute_selection
            )

        if isinstance(index_selection, list) and is_identifier(attribute_selection):
            return self._graphrecord._graphrecord.remove_node_attribute(
                index_selection, attribute_selection
            )

        if isinstance(index_selection, Callable) and is_identifier(attribute_selection):
            query_result = self._graphrecord._query_node_indices(index_selection)

            if isinstance(query_result, list):
                return self._graphrecord._graphrecord.remove_node_attribute(
                    query_result, attribute_selection
                )
            if query_result is not None:
                return self._graphrecord._graphrecord.remove_node_attribute(
                    [query_result], attribute_selection
                )

            return None

        if isinstance(index_selection, slice) and is_identifier(attribute_selection):
            if (
                index_selection.start is not None
                or index_selection.stop is not None
                or index_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            return self._graphrecord._graphrecord.remove_node_attribute(
                self._graphrecord.nodes,
                attribute_selection,
            )

        if is_node_index(index_selection) and isinstance(attribute_selection, list):
            for attribute in attribute_selection:
                self._graphrecord._graphrecord.remove_node_attribute(
                    [index_selection], attribute
                )

            return None

        if isinstance(index_selection, list) and isinstance(attribute_selection, list):
            for attribute in attribute_selection:
                self._graphrecord._graphrecord.remove_node_attribute(
                    index_selection, attribute
                )

            return None

        if isinstance(index_selection, Callable) and isinstance(
            attribute_selection, list
        ):
            query_result = self._graphrecord._query_node_indices(index_selection)

            if isinstance(query_result, list):
                for attribute in attribute_selection:
                    self._graphrecord._graphrecord.remove_node_attribute(
                        query_result, attribute
                    )
            elif query_result is not None:
                for attribute in attribute_selection:
                    self._graphrecord._graphrecord.remove_node_attribute(
                        [query_result], attribute
                    )

            return None

        if isinstance(index_selection, slice) and isinstance(attribute_selection, list):
            if (
                index_selection.start is not None
                or index_selection.stop is not None
                or index_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            for attribute in attribute_selection:
                self._graphrecord._graphrecord.remove_node_attribute(
                    self._graphrecord.nodes, attribute
                )

            return None

        if is_node_index(index_selection) and isinstance(attribute_selection, slice):
            if (
                attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            return self._graphrecord._graphrecord.replace_node_attributes(
                [index_selection], {}
            )

        if isinstance(index_selection, list) and isinstance(attribute_selection, slice):
            if (
                attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            return self._graphrecord._graphrecord.replace_node_attributes(
                index_selection, {}
            )

        if isinstance(index_selection, Callable) and isinstance(
            attribute_selection, slice
        ):
            if (
                attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            query_result = self._graphrecord._query_node_indices(index_selection)

            if isinstance(query_result, list):
                return self._graphrecord._graphrecord.replace_node_attributes(
                    query_result, {}
                )
            if query_result is not None:
                return self._graphrecord._graphrecord.replace_node_attributes(
                    [query_result], {}
                )

            return None

        if isinstance(index_selection, slice) and isinstance(
            attribute_selection, slice
        ):
            if (
                index_selection.start is not None
                or index_selection.stop is not None
                or index_selection.step is not None
                or attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            return self._graphrecord._graphrecord.replace_node_attributes(
                self._graphrecord.nodes, {}
            )

        msg = "Should never be reached"
        raise NotImplementedError(msg)


class EdgeIndexer:
    """Indexer for GraphRecord edges."""

    _graphrecord: GraphRecord

    def __init__(self, graphrecord: GraphRecord) -> None:
        """Initializes the EdgeIndexer object.

        Args:
            graphrecord (GraphRecord): GraphRecord object to index.
        """
        self._graphrecord = graphrecord

    @overload
    def __getitem__(
        self,
        key: Union[
            EdgeIndex,
            EdgeQuery,
            Tuple[
                Union[EdgeIndex, EdgeQuery],
                Union[AttributeNameInputList, slice],
            ],
        ],
    ) -> Attributes: ...

    @overload
    def __getitem__(
        self, key: Tuple[Union[EdgeIndex, EdgeQuery], AttributeName]
    ) -> Value: ...

    @overload
    def __getitem__(
        self,
        key: Union[
            EdgeIndexInputList,
            EdgesQuery,
            slice,
            Tuple[
                Union[EdgeIndexInputList, EdgesQuery, slice],
                Union[AttributeNameInputList, slice],
            ],
        ],
    ) -> Dict[EdgeIndex, Attributes]: ...

    @overload
    def __getitem__(
        self,
        key: Tuple[Union[EdgeIndexInputList, EdgesQuery, slice], AttributeName],
    ) -> Dict[EdgeIndex, Value]: ...

    def __getitem__(  # noqa: C901
        self,
        key: Union[
            EdgeIndex,
            EdgeIndexInputList,
            EdgeQuery,
            EdgesQuery,
            slice,
            Tuple[
                Union[
                    EdgeIndex,
                    EdgeIndexInputList,
                    EdgeQuery,
                    EdgesQuery,
                    slice,
                ],
                Union[AttributeName, AttributeNameInputList, slice],
            ],
        ],
    ) -> Union[
        Value,
        Attributes,
        Dict[EdgeIndex, Attributes],
        Dict[EdgeIndex, Value],
    ]:
        """Gets the edge attributes for the specified key.

        Args:
            key (Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, slice, Tuple[Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, slice], Union[AttributeName, AttributeNameInputList, slice]]):
                The edges to get attributes for.

        Returns:
            Union[Value, Attributes, Dict[EdgeIndex, Attributes], Dict[EdgeIndex, Value]]:
                The edge attributes to be extracted.

        Raises:
            ValueError: If the key is a slice, but not ":" is provided.
            IndexError: If the query returned no results.
        """  # noqa: W505
        if is_edge_index(key):
            return self._graphrecord._graphrecord.edge([key])[key]

        if isinstance(key, list):
            return self._graphrecord._graphrecord.edge(key)

        if isinstance(key, Callable):
            query_result = self._graphrecord._query_edge_indices(key)

            if isinstance(query_result, list):
                return self._graphrecord._graphrecord.edge(query_result)
            if query_result is not None:
                return self._graphrecord._graphrecord.edge([query_result])[query_result]

            msg = "The query returned no results"
            raise IndexError(msg)

        if isinstance(key, slice):
            if key.start is not None or key.stop is not None or key.step is not None:
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            return self._graphrecord._graphrecord.edge(self._graphrecord.edges)

        index_selection, attribute_selection = key

        if is_edge_index(index_selection) and is_identifier(attribute_selection):
            return self._graphrecord._graphrecord.edge([index_selection])[
                index_selection
            ][attribute_selection]

        if isinstance(index_selection, list) and is_identifier(attribute_selection):
            attributes = self._graphrecord._graphrecord.edge(index_selection)

            return {x: attributes[x][attribute_selection] for x in attributes}

        if isinstance(index_selection, Callable) and is_identifier(attribute_selection):
            query_result = self._graphrecord._query_edge_indices(index_selection)

            if isinstance(query_result, list):
                attributes = self._graphrecord._graphrecord.edge(query_result)

                return {x: attributes[x][attribute_selection] for x in attributes}
            if query_result is not None:
                return self._graphrecord._graphrecord.edge([query_result])[
                    query_result
                ][attribute_selection]

            msg = "The query returned no results"
            raise IndexError(msg)

        if isinstance(index_selection, slice) and is_identifier(attribute_selection):
            if (
                index_selection.start is not None
                or index_selection.stop is not None
                or index_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            attributes = self._graphrecord._graphrecord.edge(self._graphrecord.edges)

            return {x: attributes[x][attribute_selection] for x in attributes}

        if is_edge_index(index_selection) and isinstance(attribute_selection, list):
            return {
                x: self._graphrecord._graphrecord.edge([index_selection])[
                    index_selection
                ][x]
                for x in attribute_selection
            }

        if isinstance(index_selection, list) and isinstance(attribute_selection, list):
            attributes = self._graphrecord._graphrecord.edge(index_selection)

            return {
                x: {y: attributes[x][y] for y in attribute_selection}
                for x in attributes
            }

        if isinstance(index_selection, Callable) and isinstance(
            attribute_selection, list
        ):
            query_result = self._graphrecord._query_edge_indices(index_selection)

            if isinstance(query_result, list):
                attributes = self._graphrecord._graphrecord.edge(query_result)

                return {
                    x: {y: attributes[x][y] for y in attribute_selection}
                    for x in attributes
                }
            if query_result is not None:
                return {
                    x: self._graphrecord._graphrecord.edge([query_result])[
                        query_result
                    ][x]
                    for x in attribute_selection
                }

            msg = "The query returned no results"
            raise IndexError(msg)

        if isinstance(index_selection, slice) and isinstance(attribute_selection, list):
            if (
                index_selection.start is not None
                or index_selection.stop is not None
                or index_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            attributes = self._graphrecord._graphrecord.edge(self._graphrecord.edges)

            return {
                x: {y: attributes[x][y] for y in attribute_selection}
                for x in attributes
            }

        if is_edge_index(index_selection) and isinstance(attribute_selection, slice):
            if (
                attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            return self._graphrecord._graphrecord.edge([index_selection])[
                index_selection
            ]

        if isinstance(index_selection, list) and isinstance(attribute_selection, slice):
            if (
                attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            return self._graphrecord._graphrecord.edge(index_selection)

        if isinstance(index_selection, Callable) and isinstance(
            attribute_selection, slice
        ):
            if (
                attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            query_result = self._graphrecord._query_edge_indices(index_selection)

            if isinstance(query_result, list):
                return self._graphrecord._graphrecord.edge(query_result)
            if query_result is not None:
                return self._graphrecord._graphrecord.edge([query_result])[query_result]

            msg = "The query returned no results"
            raise IndexError(msg)

        if isinstance(index_selection, slice) and isinstance(
            attribute_selection, slice
        ):
            if (
                index_selection.start is not None
                or index_selection.stop is not None
                or index_selection.step is not None
                or attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            return self._graphrecord._graphrecord.edge(self._graphrecord.edges)

        msg = "Should never be reached"
        raise NotImplementedError(msg)

    @overload
    def __setitem__(
        self,
        key: Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, EdgesQuery, slice],
        value: AttributesInput,
    ) -> None: ...

    @overload
    def __setitem__(
        self,
        key: Tuple[
            Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, EdgesQuery, slice],
            Union[AttributeName, AttributeNameInputList, slice],
        ],
        value: Value,
    ) -> None: ...

    def __setitem__(  # noqa: C901
        self,
        key: Union[
            EdgeIndex,
            EdgeIndexInputList,
            EdgeQuery,
            EdgesQuery,
            slice,
            Tuple[
                Union[
                    EdgeIndex,
                    EdgeIndexInputList,
                    EdgeQuery,
                    EdgesQuery,
                    slice,
                ],
                Union[AttributeName, AttributeNameInputList, slice],
            ],
        ],
        value: Union[AttributesInput, Value],
    ) -> None:
        """Sets the edge attributes for the specified key.

        Args:
            key (Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, EdgesQuery, slice, Tuple[Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, EdgesQuery, slice], Union[AttributeName, AttributeNameInputList, slice]]):
                The edges to which the attributes should be set.

            value (Union[AttributesInput, Value]):
                The values to set as attributes.

        Raises:
            ValueError: If there is a wrong value type or the key is a slice, but no ":"
                is provided.
        """  # noqa: W505
        if is_edge_index(key):
            if not is_attributes(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            return self._graphrecord._graphrecord.replace_edge_attributes([key], value)

        if isinstance(key, list):
            if not is_attributes(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            return self._graphrecord._graphrecord.replace_edge_attributes(key, value)

        if isinstance(key, Callable):
            if not is_attributes(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            query_result = self._graphrecord._query_edge_indices(key)

            if isinstance(query_result, list):
                return self._graphrecord._graphrecord.replace_edge_attributes(
                    query_result, value
                )
            if query_result is not None:
                return self._graphrecord._graphrecord.replace_edge_attributes(
                    [query_result], value
                )

            return None

        if isinstance(key, slice):
            if key.start is not None or key.stop is not None or key.step is not None:
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            if not is_attributes(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            return self._graphrecord._graphrecord.replace_edge_attributes(
                self._graphrecord.edges, value
            )

        index_selection, attribute_selection = key

        if is_edge_index(index_selection) and is_identifier(attribute_selection):
            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            return self._graphrecord._graphrecord.update_edge_attribute(
                [index_selection], attribute_selection, value
            )

        if isinstance(index_selection, list) and is_identifier(attribute_selection):
            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            return self._graphrecord._graphrecord.update_edge_attribute(
                index_selection, attribute_selection, value
            )

        if isinstance(index_selection, Callable) and is_identifier(attribute_selection):
            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            query_result = self._graphrecord._query_edge_indices(index_selection)

            if isinstance(query_result, list):
                return self._graphrecord._graphrecord.update_edge_attribute(
                    query_result, attribute_selection, value
                )
            if query_result is not None:
                return self._graphrecord._graphrecord.update_edge_attribute(
                    [query_result], attribute_selection, value
                )

            return None

        if isinstance(index_selection, slice) and is_identifier(attribute_selection):
            if (
                index_selection.start is not None
                or index_selection.stop is not None
                or index_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            return self._graphrecord._graphrecord.update_edge_attribute(
                self._graphrecord.edges,
                attribute_selection,
                value,
            )

        if is_edge_index(index_selection) and isinstance(attribute_selection, list):
            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            for attribute in attribute_selection:
                self._graphrecord._graphrecord.update_edge_attribute(
                    [index_selection], attribute, value
                )

            return None

        if isinstance(index_selection, list) and isinstance(attribute_selection, list):
            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            for attribute in attribute_selection:
                self._graphrecord._graphrecord.update_edge_attribute(
                    index_selection, attribute, value
                )

            return None

        if isinstance(index_selection, Callable) and isinstance(
            attribute_selection, list
        ):
            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            query_result = self._graphrecord._query_edge_indices(index_selection)

            if isinstance(query_result, list):
                for attribute in attribute_selection:
                    self._graphrecord._graphrecord.update_edge_attribute(
                        query_result, attribute, value
                    )
            elif query_result is not None:
                for attribute in attribute_selection:
                    self._graphrecord._graphrecord.update_edge_attribute(
                        [query_result], attribute, value
                    )

            return None

        if isinstance(index_selection, slice) and isinstance(attribute_selection, list):
            if (
                index_selection.start is not None
                or index_selection.stop is not None
                or index_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            for attribute in attribute_selection:
                self._graphrecord._graphrecord.update_edge_attribute(
                    self._graphrecord.edges, attribute, value
                )

            return None

        if is_edge_index(index_selection) and isinstance(attribute_selection, slice):
            if (
                attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            attributes = self._graphrecord._graphrecord.edge([index_selection])[
                index_selection
            ]

            for attribute in attributes:
                self._graphrecord._graphrecord.update_edge_attribute(
                    [index_selection], attribute, value
                )

            return None

        if isinstance(index_selection, list) and isinstance(attribute_selection, slice):
            if (
                attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            attributes = self._graphrecord._graphrecord.edge(index_selection)

            for edge in attributes:
                for attribute in attributes[edge]:
                    self._graphrecord._graphrecord.update_edge_attribute(
                        [edge], attribute, value
                    )

            return None

        if isinstance(index_selection, Callable) and isinstance(
            attribute_selection, slice
        ):
            if (
                attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            query_result = self._graphrecord._query_edge_indices(index_selection)

            if isinstance(query_result, list):
                attributes = self._graphrecord._graphrecord.edge(query_result)

                for edge in attributes:
                    for attribute in attributes[edge]:
                        self._graphrecord._graphrecord.update_edge_attribute(
                            query_result, attribute, value
                        )
            elif query_result is not None:
                attributes = self._graphrecord._graphrecord.edge([query_result])[
                    query_result
                ]

                for attribute in attributes:
                    self._graphrecord._graphrecord.update_edge_attribute(
                        [query_result], attribute, value
                    )

            return None

        if isinstance(index_selection, slice) and isinstance(
            attribute_selection, slice
        ):
            if (
                index_selection.start is not None
                or index_selection.stop is not None
                or index_selection.step is not None
                or attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            if not is_value(value):
                msg = "Should never be reached"
                raise NotImplementedError(msg)

            attributes = self._graphrecord._graphrecord.edge(self._graphrecord.edges)

            for edge in attributes:
                for attribute in attributes[edge]:
                    self._graphrecord._graphrecord.update_edge_attribute(
                        [edge], attribute, value
                    )

            return None

        msg = "Should never be reached"
        raise NotImplementedError(msg)

    def __delitem__(  # noqa: C901
        self,
        key: Tuple[
            Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, EdgesQuery, slice],
            Union[AttributeName, AttributeNameInputList, slice],
        ],
    ) -> None:
        """Deletes the specified edge attributes.

        Args:
            key (Tuple[Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, EdgesQuery, slice], Union[AttributeName, AttributeNameInputList, slice]]):
                The edges from which to delete the attributes.

        Raises:
            ValueError: If the key is a slice, but not ":" is provided.
        """  # noqa: W505
        index_selection, attribute_selection = key

        if is_edge_index(index_selection) and is_identifier(attribute_selection):
            return self._graphrecord._graphrecord.remove_edge_attribute(
                [index_selection], attribute_selection
            )

        if isinstance(index_selection, list) and is_identifier(attribute_selection):
            return self._graphrecord._graphrecord.remove_edge_attribute(
                index_selection, attribute_selection
            )

        if isinstance(index_selection, Callable) and is_identifier(attribute_selection):
            query_result = self._graphrecord._query_edge_indices(index_selection)

            if isinstance(query_result, list):
                return self._graphrecord._graphrecord.remove_edge_attribute(
                    query_result, attribute_selection
                )
            if query_result is not None:
                return self._graphrecord._graphrecord.remove_edge_attribute(
                    [query_result], attribute_selection
                )

            return None

        if isinstance(index_selection, slice) and is_identifier(attribute_selection):
            if (
                index_selection.start is not None
                or index_selection.stop is not None
                or index_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            return self._graphrecord._graphrecord.remove_edge_attribute(
                self._graphrecord.edges,
                attribute_selection,
            )

        if is_edge_index(index_selection) and isinstance(attribute_selection, list):
            for attribute in attribute_selection:
                self._graphrecord._graphrecord.remove_edge_attribute(
                    [index_selection], attribute
                )

            return None

        if isinstance(index_selection, list) and isinstance(attribute_selection, list):
            for attribute in attribute_selection:
                self._graphrecord._graphrecord.remove_edge_attribute(
                    index_selection, attribute
                )

            return None

        if isinstance(index_selection, Callable) and isinstance(
            attribute_selection, list
        ):
            query_result = self._graphrecord._query_edge_indices(index_selection)

            if isinstance(query_result, list):
                for attribute in attribute_selection:
                    self._graphrecord._graphrecord.remove_edge_attribute(
                        query_result, attribute
                    )
            elif query_result is not None:
                for attribute in attribute_selection:
                    self._graphrecord._graphrecord.remove_edge_attribute(
                        [query_result], attribute
                    )

            return None

        if isinstance(index_selection, slice) and isinstance(attribute_selection, list):
            if (
                index_selection.start is not None
                or index_selection.stop is not None
                or index_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            for attribute in attribute_selection:
                self._graphrecord._graphrecord.remove_edge_attribute(
                    self._graphrecord.edges, attribute
                )

            return None

        if is_edge_index(index_selection) and isinstance(attribute_selection, slice):
            if (
                attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            return self._graphrecord._graphrecord.replace_edge_attributes(
                [index_selection], {}
            )

        if isinstance(index_selection, list) and isinstance(attribute_selection, slice):
            if (
                attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            return self._graphrecord._graphrecord.replace_edge_attributes(
                index_selection, {}
            )

        if isinstance(index_selection, Callable) and isinstance(
            attribute_selection, slice
        ):
            if (
                attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            query_result = self._graphrecord._query_edge_indices(index_selection)

            if isinstance(query_result, list):
                return self._graphrecord._graphrecord.replace_edge_attributes(
                    query_result, {}
                )
            if query_result is not None:
                return self._graphrecord._graphrecord.replace_edge_attributes(
                    [query_result], {}
                )

            return None

        if isinstance(index_selection, slice) and isinstance(
            attribute_selection, slice
        ):
            if (
                index_selection.start is not None
                or index_selection.stop is not None
                or index_selection.step is not None
                or attribute_selection.start is not None
                or attribute_selection.stop is not None
                or attribute_selection.step is not None
            ):
                msg = "Invalid slice, only ':' is allowed"
                raise ValueError(msg)

            return self._graphrecord._graphrecord.replace_edge_attributes(
                self._graphrecord.edges, {}
            )

        msg = "Should never be reached"
        raise NotImplementedError(msg)
