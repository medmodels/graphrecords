import unittest
from typing import Dict, List

import pytest

from graphrecords import GraphRecord
from graphrecords._graphrecords.graphrecord import PyGraphRecord
from graphrecords.connectors import (
    ConnectedGraphRecord,
    Connector,
    ExportConnector,
    IngestConnector,
    _ConnectorBridge,
)
from graphrecords.types import NodeIndex


class RecordingConnector(Connector):
    def __init__(self) -> None:
        self.calls: List[str] = []

    def initialize(self, graphrecord: GraphRecord) -> None:
        self.calls.append("initialize")

    def disconnect(self, graphrecord: GraphRecord) -> None:
        self.calls.append("disconnect")


class SimpleIngestConnector(IngestConnector[List[Dict[str, str]]]):
    def __init__(self) -> None:
        self.calls: List[str] = []

    def initialize(self, graphrecord: GraphRecord) -> None:
        self.calls.append("initialize")

    def ingest(self, graphrecord: GraphRecord, data: List[Dict[str, str]]) -> None:
        self.calls.append("ingest")
        for item in data:
            graphrecord.add_nodes((item["id"], {"value": item["value"]}))


class SimpleExportConnector(ExportConnector[List[NodeIndex]]):
    def __init__(self) -> None:
        self.calls: List[str] = []

    def initialize(self, graphrecord: GraphRecord) -> None:
        self.calls.append("initialize")

    def export(self, graphrecord: GraphRecord) -> List[NodeIndex]:
        self.calls.append("export")
        return list(graphrecord.nodes)


class FullConnector(IngestConnector[List[str]], ExportConnector[List[NodeIndex]]):
    def __init__(self) -> None:
        self.calls: List[str] = []

    def initialize(self, graphrecord: GraphRecord) -> None:
        self.calls.append("initialize")

    def disconnect(self, graphrecord: GraphRecord) -> None:
        self.calls.append("disconnect")

    def ingest(self, graphrecord: GraphRecord, data: List[str]) -> None:
        self.calls.append("ingest")
        for name in data:
            graphrecord.add_nodes((name, {}))

    def export(self, graphrecord: GraphRecord) -> List[NodeIndex]:
        self.calls.append("export")
        return list(graphrecord.nodes)


class TestConnector(unittest.TestCase):
    def test_initialize_called(self) -> None:
        connector = RecordingConnector()
        ConnectedGraphRecord(connector)

        assert "initialize" in connector.calls

    def test_initialize_default_noop(self) -> None:
        connector = Connector()
        record = ConnectedGraphRecord(connector)

        assert record.node_count() == 0

    def test_disconnect_default_noop(self) -> None:
        connector = Connector()
        record = ConnectedGraphRecord(connector)
        disconnected = record.disconnect()

        assert isinstance(disconnected, GraphRecord)

    def test_disconnect_called(self) -> None:
        connector = RecordingConnector()
        record = ConnectedGraphRecord(connector)
        disconnected = record.disconnect()

        assert "disconnect" in connector.calls
        assert isinstance(disconnected, GraphRecord)
        assert not isinstance(disconnected, ConnectedGraphRecord)

    def test_graphrecord_methods_available(self) -> None:
        connector = RecordingConnector()
        record = ConnectedGraphRecord(connector)

        record.add_nodes(("node1", {"value": "1"}))

        assert record.node_count() == 1
        assert "node1" in record.nodes
        assert record.node["node1"]["value"] == "1"


class TestIngestConnector(unittest.TestCase):
    def test_ingest(self) -> None:
        connector = SimpleIngestConnector()
        record = ConnectedGraphRecord(connector)

        record.ingest([{"id": "a", "value": "1"}, {"id": "b", "value": "2"}])

        assert record.node_count() == 2
        assert record.node["a"]["value"] == "1"
        assert record.node["b"]["value"] == "2"
        assert "ingest" in connector.calls


class TestExportConnector(unittest.TestCase):
    def test_export(self) -> None:
        connector = SimpleExportConnector()
        record = ConnectedGraphRecord(connector)

        record.add_nodes(("a", {"value": "1"}))

        exported = record.export()

        assert exported == ["a"]
        assert "export" in connector.calls


class TestFullConnector(unittest.TestCase):
    def test_ingest_and_export(self) -> None:
        connector = FullConnector()
        record = ConnectedGraphRecord(connector)

        record.ingest(["a", "b"])
        exported = record.export()

        assert sorted(exported) == ["a", "b"]
        assert "ingest" in connector.calls
        assert "export" in connector.calls

    def test_disconnect(self) -> None:
        connector = FullConnector()
        record = ConnectedGraphRecord(connector)
        record.ingest(["a"])

        disconnected = record.disconnect()

        assert isinstance(disconnected, GraphRecord)
        assert not isinstance(disconnected, ConnectedGraphRecord)
        assert disconnected.node_count() == 1
        assert "disconnect" in connector.calls


class TestConnectorBridge(unittest.TestCase):
    def test_ingest_raises_for_non_ingest_connector(self) -> None:
        connector = RecordingConnector()
        bridge = _ConnectorBridge(connector)

        with pytest.raises(NotImplementedError, match="ingest"):
            bridge.ingest(PyGraphRecord(), None)

    def test_export_raises_for_non_export_connector(self) -> None:
        connector = RecordingConnector()
        bridge = _ConnectorBridge(connector)

        with pytest.raises(NotImplementedError, match="export"):
            bridge.export(PyGraphRecord())


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestConnector))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestIngestConnector))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestExportConnector))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestFullConnector))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestConnectorBridge))
    unittest.TextTestRunner(verbosity=2).run(suite)
