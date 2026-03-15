"""Connector classes for ingesting and exporting data with a GraphRecord."""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, Generic, TypeVar

from graphrecords._graphrecords.graphrecord import PyGraphRecord
from graphrecords.graphrecord import GraphRecord
from graphrecords.types import _PyConnector


class _ConnectorBridge(_PyConnector):
    _connector: Connector

    def __init__(self, connector: Connector) -> None:
        self._connector = connector

    def _graphrecord(self, graphrecord: PyGraphRecord) -> GraphRecord:
        return GraphRecord._from_py_graphrecord(graphrecord)

    def initialize(self, graphrecord: PyGraphRecord) -> None:
        self._connector.initialize(self._graphrecord(graphrecord))

    def disconnect(self, graphrecord: PyGraphRecord) -> None:
        self._connector.disconnect(self._graphrecord(graphrecord))

    def ingest(self, graphrecord: PyGraphRecord, data: Any) -> None:  # noqa: ANN401
        if isinstance(self._connector, IngestConnector):
            self._connector.ingest(self._graphrecord(graphrecord), data)
            return

        msg = "Connector does not implement ingest method"
        raise NotImplementedError(msg)

    def export(self, graphrecord: PyGraphRecord) -> Any:  # noqa: ANN401
        if isinstance(self._connector, ExportConnector):
            return self._connector.export(self._graphrecord(graphrecord))

        msg = "Connector does not implement export method"
        raise NotImplementedError(msg)


class Connector:
    """Base class for connectors that attach to a GraphRecord.

    Subclass this to define custom initialization and disconnection logic.
    For data ingestion or export, subclass IngestConnector or ExportConnector
    instead.
    """

    def initialize(self, graphrecord: GraphRecord) -> None:
        """Called when the connector is attached to a GraphRecord.

        Override this method to set up schema, plugins, or initial state.

        Args:
            graphrecord (GraphRecord): The GraphRecord being initialized.
        """

    def disconnect(self, graphrecord: GraphRecord) -> None:
        """Called when the connector is detached from a GraphRecord.

        Override this method to perform cleanup.

        Args:
            graphrecord (GraphRecord): The GraphRecord being disconnected.
        """


IngestData = TypeVar("IngestData")


class IngestConnector(Connector, Generic[IngestData]):
    """A connector that can ingest data into a GraphRecord.

    The type parameter specifies the data type accepted by the ingest method.
    """

    @abstractmethod
    def ingest(self, graphrecord: GraphRecord, data: IngestData) -> None:
        """Ingests data into the GraphRecord.

        Args:
            graphrecord (GraphRecord): The GraphRecord to ingest data into.
            data (IngestData): The data to ingest.
        """
        ...


ExportData = TypeVar("ExportData")


class ExportConnector(Connector, Generic[ExportData]):
    """A connector that can export data from a GraphRecord.

    The type parameter specifies the data type returned by the export method.
    """

    @abstractmethod
    def export(self, graphrecord: GraphRecord) -> ExportData:
        """Exports data from the GraphRecord.

        Args:
            graphrecord (GraphRecord): The GraphRecord to export data from.

        Returns:
            ExportData: The exported data.
        """
        ...


ConnectorType = TypeVar("ConnectorType", bound=Connector, covariant=True)


class ConnectedGraphRecord(GraphRecord, Generic[ConnectorType]):
    """A GraphRecord with an attached connector.

    Wraps a GraphRecord and provides ingest and export methods based on the
    connector's capabilities. The connector type parameter determines which
    methods are available at the type level.
    """

    def __init__(self, connector: ConnectorType) -> None:
        """Creates a ConnectedGraphRecord with the specified connector.

        Initializes a new GraphRecord and calls the connector's initialize
        method.

        Args:
            connector (ConnectorType): The connector to attach.
        """
        self._graphrecord = PyGraphRecord.with_connector(_ConnectorBridge(connector))

    def disconnect(self) -> GraphRecord:
        """Detaches the connector and returns a plain GraphRecord.

        Calls the connector's disconnect method and returns the underlying
        GraphRecord without the connector.

        Returns:
            GraphRecord: The disconnected GraphRecord.
        """
        graphrecord = self._graphrecord.disconnect()

        return GraphRecord._from_py_graphrecord(graphrecord)

    def ingest(
        self: ConnectedGraphRecord[IngestConnector[IngestData]], data: IngestData
    ) -> None:
        """Ingests data into the GraphRecord using the attached connector.

        Only available when the connector implements IngestConnector.

        Args:
            data (IngestData): The data to ingest.
        """
        self._graphrecord.ingest(data)

    def export(
        self: ConnectedGraphRecord[ExportConnector[ExportData]],
    ) -> ExportData:
        """Exports data from the GraphRecord using the attached connector.

        Only available when the connector implements ExportConnector.

        Returns:
            ExportData: The exported data.
        """
        return self._graphrecord.export()
