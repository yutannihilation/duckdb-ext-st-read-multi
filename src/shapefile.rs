use std::path::Path;

use crate::types::{ColumnSpec, ColumnType};

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
}

impl ShapefileDataSource {
    pub(crate) fn new<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let path = path.as_ref();

        let dbf_reader = ::shapefile::dbase::Reader::from_path(path.with_extension("dbf"))?;
        let mut column_specs: Vec<ColumnSpec> = dbf_reader
            .fields()
            .iter()
            .map(|field| ColumnSpec {
                name: field.name().to_string(),
                column_type: field.field_type().into(),
            })
            .collect();
        column_specs.sort_by(|a, b| a.name.cmp(&b.name));

        let mut reader = ::shapefile::Reader::from_path(path)?;
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
        })
    }

    pub(crate) fn get_column_specs<T: AsRef<str>>(
        &self,
        _table_name: T,
    ) -> Result<Vec<ColumnSpec>, Box<dyn std::error::Error>> {
        Ok(self.column_specs.clone())
    }
}

impl From<::shapefile::dbase::FieldType> for ColumnType {
    fn from(value: ::shapefile::dbase::FieldType) -> Self {
        use ::shapefile::dbase::FieldType;

        match value {
            FieldType::Logical => Self::Boolean,
            FieldType::Integer => Self::Integer,
            FieldType::Numeric
            | FieldType::Float
            | FieldType::Currency
            | FieldType::Double
            | FieldType::DateTime => Self::Double,
            FieldType::Character | FieldType::Date | FieldType::Memo => Self::Varchar,
        }
    }
}

fn shape_to_wkb(shape: ::shapefile::Shape) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    if matches!(shape, ::shapefile::Shape::NullShape) {
        return Ok(None);
    }

    let geometry: geo_types::Geometry<f64> = shape.try_into()?;
    let mut buffer = Vec::new();
    wkb::writer::write_geometry(&mut buffer, &geometry, &Default::default()).unwrap();
    Ok(Some(buffer))
}

#[cfg(test)]
mod tests {
    use crate::types::ColumnType;

    #[test]
    fn test_get_column_specs() -> Result<(), Box<dyn std::error::Error>> {
        let source = super::ShapefileDataSource::new("./test/data/shapefile_utf8/points.shp")?;
        let specs = source.get_column_specs("points")?;

        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].column_type, ColumnType::Double);
        assert_eq!(&specs[0].name, "属性1");
        assert_eq!(specs[1].column_type, ColumnType::Varchar);
        assert_eq!(&specs[1].name, "属性2");

        Ok(())
    }

    #[test]
    fn test_get_column_specs_cp932() -> Result<(), Box<dyn std::error::Error>> {
        let source = super::ShapefileDataSource::new("./test/data/shapefile_cp932/points.shp")?;
        let specs = source.get_column_specs("points")?;

        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].column_type, ColumnType::Double);
        assert_eq!(&specs[0].name, "属性1");
        assert_eq!(specs[1].column_type, ColumnType::Varchar);
        assert_eq!(&specs[1].name, "属性2");

        Ok(())
    }
}
