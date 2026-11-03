use super::{DynExpression, expression::Program};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};

impl Serialize for DynExpression {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.program().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DynExpression {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Program::deserialize(deserializer)?
            .compile()
            .map_err(D::Error::custom)
    }
}
