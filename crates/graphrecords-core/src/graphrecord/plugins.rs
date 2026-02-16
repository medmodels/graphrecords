use crate::{GraphRecord, prelude::Schema};
use std::fmt::Debug;

pub struct PreSetSchemaContext {
    pub schema: Schema,
}

#[cfg_attr(feature = "plugins_serde", typetag::serde(tag = "type"))]
pub trait Plugin: Debug + Send + Sync {
    fn initialize(&self, graphrecord: &mut GraphRecord);
    fn clone_box(&self) -> Box<dyn Plugin>;
    fn pre_set_schema(
        &self,
        context: PreSetSchemaContext,
        _graphrecord: &mut GraphRecord,
    ) -> PreSetSchemaContext {
        context
    }
    fn post_set_schema(&self, _graphrecord: &mut GraphRecord) {}
}

impl Clone for Box<dyn Plugin> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
