use std::path::Path;

use crate::types::{ColumnSpec, ColumnType};

use super::encoding::infer_encoding_from_cpg;

#[repr(C)]
pub struct ShapefileRow {
    pub geometry: Option<Vec<u8>>,
    pub record: ::shapefile::dbase::Record,
}

#[repr(C)]
pub struct ShapefileDataSource {
    pub rows: Vec<ShapefileRow>,
    pub filename: String,
    pub column_specs: Vec<ColumnSpec>,
    pub inferred_cpg_encoding: Option<String>,
}

impl ShapefileDataSource {
    pub(crate) fn new<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let path = path.as_ref();
        let dbf_path = path.with_extension("dbf");
        let cpg_path = path.with_extension("cpg");

        let cpg_inferred = infer_encoding_from_cpg(&cpg_path);
        let dbf_reader = open_dbf_reader(&dbf_path, cpg_inferred.as_ref().map(|v| v.encoding))?;

        let mut column_specs: Vec<ColumnSpec> = dbf_reader
            .fields()
            .iter()
            .map(|field| ColumnSpec {
                name: field.name().to_string(),
                column_type: field.field_type().into(),
            })
            .collect();
        column_specs.sort_by(|a, b| a.name.cmp(&b.name));

        let shape_reader = ::shapefile::ShapeReader::from_path(path)?;
        let mut reader = ::shapefile::Reader::new(shape_reader, dbf_reader);

        let mut rows: Vec<ShapefileRow> = Vec::new();
        for shape_record in reader.iter_shapes_and_records() {
            let (shape, record) = shape_record?;
            rows.push(ShapefileRow {
                geometry: shape_to_wkb(shape)?,
                record,
            });
        }

        Ok(ShapefileDataSource {
            rows,
            filename: path.to_string_lossy().into_owned(),
            column_specs,
            inferred_cpg_encoding: cpg_inferred.map(|v| v.name.to_string()),
        })
    }
}

fn open_dbf_reader(
    dbf_path: &Path,
    cpg_encoding: Option<::shapefile::dbase::encoding::EncodingRs>,
) -> Result<::shapefile::dbase::Reader<std::io::BufReader<std::fs::File>>, Box<dyn std::error::Error>>
{
    match cpg_encoding {
        Some(encoding) => Ok(::shapefile::dbase::Reader::from_path_with_encoding(
            dbf_path, encoding,
        )?),
        None => Ok(::shapefile::dbase::Reader::from_path(dbf_path)?),
    }
}

impl From<::shapefile::dbase::FieldType> for ColumnType {
    fn from(value: ::shapefile::dbase::FieldType) -> Self {
        use ::shapefile::dbase::FieldType;

        match value {
            FieldType::Logical => Self::Boolean,
            FieldType::Integer => Self::Integer,
            FieldType::Numeric | FieldType::Float | FieldType::Currency | FieldType::Double => {
                Self::Double
            }
            FieldType::DateTime => Self::Double, // TODO
            FieldType::Character | FieldType::Memo => Self::Varchar,
            FieldType::Date => Self::Varchar, // TODO
        }
    }
}

fn shape_to_wkb(shape: ::shapefile::Shape) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    if matches!(shape, ::shapefile::Shape::NullShape) {
        return Ok(None);
    }

    let geometry: geo_types::Geometry<f64> = shape.try_into()?;
    let mut buffer = Vec::new();
    wkb::writer::write_geometry(&mut buffer, &geometry, &Default::default())
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
    Ok(Some(buffer))
}
