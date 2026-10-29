use duckdb_neo::{
    Parameters, Result,
    connection::FFILink,
    logical_type::{LogicalType, LogicalTypeID},
};

use crate::geojson::GeoJsonDataSource;
use crate::gpkg::GpkgDataSource;
use crate::shapefile::ShapefileDataSource;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ColumnType {
    Boolean,
    Varchar,
    Double,
    Integer,
    Date,
    Timestamp,
    Blob,
    Geometry,
}

impl ColumnType {
    pub fn logical_type_id(&self) -> LogicalTypeID {
        match self {
            ColumnType::Boolean => LogicalTypeID::Boolean,
            ColumnType::Double => LogicalTypeID::Double,
            ColumnType::Integer => LogicalTypeID::Integer,
            ColumnType::Date => LogicalTypeID::Date,
            ColumnType::Timestamp => LogicalTypeID::Timestamp,
            ColumnType::Varchar => LogicalTypeID::Varchar,
            ColumnType::Blob => LogicalTypeID::Blob,
            ColumnType::Geometry => LogicalTypeID::Geometry,
        }
    }

    pub fn logical_type<C: FFILink + ?Sized>(&self, link: &C) -> Result<LogicalType> {
        match self {
            // GEOMETRY carries a CRS parameter, so it cannot be created from the bare id.
            ColumnType::Geometry => LogicalType::from_text(link, "GEOMETRY"),
            _ => LogicalType::create_from_id(link, self.logical_type_id(), Parameters::None),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ColumnSpec {
    pub name: String,
    pub column_type: ColumnType,
}

pub struct GeoJsonBindData {
    pub sources: Vec<GeoJsonDataSource>,
    pub column_specs: Vec<ColumnSpec>,
}

pub struct GpkgBindData {
    pub sources: Vec<GpkgDataSource>,
    pub column_specs: Vec<ColumnSpec>,
}

pub struct ShapefileBindData {
    pub sources: Vec<ShapefileDataSource>,
    pub column_specs: Vec<ColumnSpec>,
}

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

/// Position of the scan: which data source is being read and the row offset within it.
pub struct Cursor {
    pub source_idx: usize,
    pub offset: usize,
}

impl Cursor {
    pub fn new() -> Self {
        Self {
            source_idx: 0,
            offset: 0,
        }
    }
}

/// One cell of an output chunk, buffered before it is written column by column.
#[derive(Debug)]
pub enum Cell {
    Null,
    Boolean(bool),
    Integer(i32),
    Double(f64),
    Varchar(String),
    /// Raw bytes; also used for WKB that is written to a GEOMETRY column.
    Blob(Vec<u8>),
    /// Days since 1970-01-01.
    Date(i32),
    /// Microseconds since 1970-01-01 00:00:00.
    Timestamp(i64),
}
