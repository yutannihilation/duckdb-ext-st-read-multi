use duckdb::core::LogicalTypeHandle;
use duckdb::core::LogicalTypeId;
use std::sync::atomic::AtomicBool;

use crate::geojson::GeoJsonDataSource;

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
pub struct GeoJsonBindData {
    pub sources: Vec<GeoJsonDataSource>,
    pub column_specs: Vec<ColumnSpec>,
}

#[repr(C)]
pub struct GpkgBindData {
    // pub sources: Vec<GeoJsonDataSource>,
    pub column_specs: Vec<ColumnSpec>,
}

#[repr(C)]
pub enum StReadMultiBindData {
    GeoJson(GeoJsonBindData),
    Gpkg(GpkgBindData),
}

impl From<GeoJsonBindData> for StReadMultiBindData {
    fn from(value: GeoJsonBindData) -> Self {
        Self::GeoJson(value)
    }
}

impl From<GpkgBindData> for StReadMultiBindData {
    fn from(value: GpkgBindData) -> Self {
        Self::Gpkg(value)
    }
}

#[repr(C)]
pub struct StReadMultiInitData {
    pub done: AtomicBool,
}
