use duckdb::core::LogicalTypeHandle;
use duckdb::core::LogicalTypeId;
use geojson::FeatureCollection;
use std::sync::atomic::AtomicBool;

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(C)]
pub enum ColumnType {
    Boolean,
    Varchar,
    Double,
    Integer,
}

// Note: NULL must be handled outside of this function
impl TryFrom<&serde_json::Value> for ColumnType {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: &serde_json::Value) -> std::result::Result<Self, Self::Error> {
        match value {
            serde_json::Value::Bool(_) => Ok(Self::Boolean),
            serde_json::Value::Number(_number) => {
                // TODO: detect integer or double
                Ok(Self::Double)
            }
            serde_json::Value::String(_) => Ok(Self::Varchar),
            _ => Err(format!("Unsupported type: {value:?}").into()),
        }
    }
}

impl From<ColumnType> for LogicalTypeHandle {
    fn from(value: ColumnType) -> Self {
        match value {
            ColumnType::Boolean => LogicalTypeId::Boolean.into(),
            ColumnType::Double => LogicalTypeId::Double.into(),
            ColumnType::Integer => LogicalTypeId::Integer.into(),
            ColumnType::Varchar => LogicalTypeId::Varchar.into(),
        }
    }
}

#[repr(C)]
pub struct ColumnSpec {
    pub name: String,
    pub column_type: ColumnType,
}

#[repr(C)]
pub struct FeatureCollectionWithSource {
    pub feature_collection: FeatureCollection,
    pub filename: String,
}

#[repr(C)]
pub struct StReadMultiBindData {
    pub sources: Vec<FeatureCollectionWithSource>,
    pub column_specs: Vec<ColumnSpec>,
}

#[repr(C)]
pub struct StReadMultiInitData {
    pub done: AtomicBool,
}
