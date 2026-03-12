use crate::{GraphRecord, errors::GraphRecordResult};

pub trait Connector {
    type DataSet;

    fn initialize(&self, graphrecord: &mut GraphRecord) -> GraphRecordResult<()>;

    fn ingest(&self, graphrecord: &mut GraphRecord, data: Self::DataSet) -> GraphRecordResult<()>;

    fn export(&self, graphrecord: &GraphRecord) -> GraphRecordResult<Self::DataSet>;
}
