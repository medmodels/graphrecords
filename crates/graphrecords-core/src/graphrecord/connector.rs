use crate::{GraphRecord, errors::GraphRecordResult};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{
    fmt::Display,
    ops::{Deref, DerefMut},
};

pub trait Connector {
    fn initialize(&self, graphrecord: &mut GraphRecord) -> GraphRecordResult<()>;
    fn disconnect(&self, graphrecord: &mut GraphRecord) -> GraphRecordResult<()>;
}

pub trait IngestConnector: Connector {
    type DataSet;

    fn ingest(&self, graphrecord: &mut GraphRecord, data: Self::DataSet) -> GraphRecordResult<()>;
}

pub trait ExportConnector: Connector {
    type DataSet;

    fn export(&self, graphrecord: &GraphRecord) -> GraphRecordResult<Self::DataSet>;
}

impl GraphRecord {
    pub fn with_connector<C: Connector>(
        connector: C,
    ) -> GraphRecordResult<ConnectedGraphRecord<C>> {
        ConnectedGraphRecord::new(connector)
    }

    pub fn from_connector_with_data<D, C: IngestConnector<DataSet = D>>(
        connector: C,
        data: D,
    ) -> GraphRecordResult<ConnectedGraphRecord<C>> {
        ConnectedGraphRecord::from_connector_with_data(connector, data)
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ConnectedGraphRecord<C: Connector> {
    graphrecord: GraphRecord,
    connector: C,
}

impl<C: Connector> Display for ConnectedGraphRecord<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.graphrecord, f)
    }
}

impl<C: Connector> Deref for ConnectedGraphRecord<C> {
    type Target = GraphRecord;

    fn deref(&self) -> &Self::Target {
        &self.graphrecord
    }
}

impl<C: Connector> DerefMut for ConnectedGraphRecord<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.graphrecord
    }
}

impl<C: Connector> AsRef<GraphRecord> for ConnectedGraphRecord<C> {
    fn as_ref(&self) -> &GraphRecord {
        &self.graphrecord
    }
}

impl<C: Connector> AsMut<GraphRecord> for ConnectedGraphRecord<C> {
    fn as_mut(&mut self) -> &mut GraphRecord {
        &mut self.graphrecord
    }
}

impl<C: Connector> From<ConnectedGraphRecord<C>> for GraphRecord {
    fn from(connected: ConnectedGraphRecord<C>) -> Self {
        connected.graphrecord
    }
}

impl<C: Connector> ConnectedGraphRecord<C> {
    pub fn new(connector: C) -> GraphRecordResult<Self> {
        let mut graphrecord = GraphRecord::new();

        connector.initialize(&mut graphrecord)?;

        Ok(Self {
            graphrecord,
            connector,
        })
    }

    pub fn from_connector_with_data<D>(connector: C, data: D) -> GraphRecordResult<Self>
    where
        C: IngestConnector<DataSet = D>,
    {
        let mut graphrecord = GraphRecord::new();

        connector.initialize(&mut graphrecord)?;

        connector.ingest(&mut graphrecord, data)?;

        Ok(Self {
            graphrecord,
            connector,
        })
    }

    pub fn disconnect(mut self) -> GraphRecordResult<GraphRecord> {
        self.connector.disconnect(&mut self.graphrecord)?;

        Ok(self.graphrecord)
    }
}

impl<C: IngestConnector> ConnectedGraphRecord<C> {
    pub fn ingest(&mut self, data: C::DataSet) -> GraphRecordResult<()> {
        self.connector.ingest(&mut self.graphrecord, data)
    }
}

impl<C: ExportConnector> ConnectedGraphRecord<C> {
    pub fn export(&self) -> GraphRecordResult<C::DataSet> {
        self.connector.export(&self.graphrecord)
    }
}
