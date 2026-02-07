use duckdb::core::LogicalTypeHandle;
use duckdb::core::LogicalTypeId;
use std::sync::Arc;
use std::sync::Mutex;

use crate::geojson::GeoJsonDataSource;
use crate::gpkg::GpkgDataSource;
use crate::shapefile::ShapefileDataSource;

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
pub struct ShapefileBindData {
    pub sources: Vec<ShapefileDataSource>,
    pub column_specs: Vec<ColumnSpec>,
}

#[repr(C)]
pub enum StReadMultiBindData {
    GeoJson(GeoJsonBindData),
    Gpkg(GpkgBindData),
    Shapefile(ShapefileBindData),
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

impl From<ShapefileBindData> for StReadMultiBindData {
    fn from(value: ShapefileBindData) -> Self {
        Self::Shapefile(value)
    }
}

pub struct Cursor {
    pub source_idx: usize,
    pub offset: usize,
}

#[repr(C)]
pub struct StReadMultiInitData {
    pub cursor: Arc<Mutex<Cursor>>,
}

impl Cursor {
    pub fn new() -> Self {
        Self {
            source_idx: 0,
            offset: 0,
        }
    }
}
