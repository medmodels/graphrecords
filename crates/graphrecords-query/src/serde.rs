use crate::{Failure, FailureKind, Queryable, ReturnExpression, Series};
use graphrecords_core::GraphRecord;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::Error as DeserializeError,
    ser::{Error as SerializeError, SerializeStruct},
};

impl Serialize for Failure {
    fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        Err(S::Error::custom("a failure cannot be saved"))
    }
}

impl<'de> Deserialize<'de> for Failure {
    fn deserialize<D: Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
        Err(D::Error::custom("a failure cannot be loaded"))
    }
}

impl Serialize for FailureKind {
    fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        Err(S::Error::custom("a failure kind cannot be saved"))
    }
}

impl<'de> Deserialize<'de> for FailureKind {
    fn deserialize<D: Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
        Err(D::Error::custom("a failure kind cannot be loaded"))
    }
}

#[derive(Deserialize)]
struct SeriesParts<E> {
    graphrecord: GraphRecord,
    expression: E,
}

impl<E: Serialize> Serialize for Series<E> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut series = serializer.serialize_struct("Series", 2)?;

        series.serialize_field("graphrecord", self.graphrecord())?;
        series.serialize_field("expression", self.expression())?;

        series.end()
    }
}

impl<'de, E: Deserialize<'de> + ReturnExpression> Deserialize<'de> for Series<E> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let parts = SeriesParts::deserialize(deserializer)?;

        Ok(parts.graphrecord.query(parts.expression))
    }
}
