use duckdb::core::LogicalTypeHandle;
use duckdb::core::LogicalTypeId;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;

use crate::geojson::GeoJsonDataSource;
use crate::gpkg::GpkgDataSource;

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(C)]
pub enum ColumnType {
    Boolean,
    Varchar,
    Double,
    Integer,
    Geometry,
}

impl From<ColumnType> for LogicalTypeHandle {
    fn from(value: ColumnType) -> Self {
        match value {
            ColumnType::Boolean => LogicalTypeId::Boolean.into(),
            ColumnType::Double => LogicalTypeId::Double.into(),
            ColumnType::Integer => LogicalTypeId::Integer.into(),
            ColumnType::Varchar => LogicalTypeId::Varchar.into(),
            ColumnType::Geometry => LogicalTypeId::Blob.into(),
        }
    }
}

#[derive(Clone, Debug)]
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
    pub sources: Vec<GpkgDataSource>,
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
    pub cur_source_idx: AtomicUsize,
}
