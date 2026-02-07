use crate::types::ColumnType;

fn row_character(
    source: &super::ShapefileDataSource,
    row_index: usize,
    field_name: &str,
) -> Option<String> {
    use ::shapefile::dbase::FieldValue;

    match source.rows.get(row_index)?.record.get(field_name)? {
        FieldValue::Character(Some(value)) => Some(value.clone()),
        _ => None,
    }
}

#[test]
fn test_get_column_specs() -> Result<(), Box<dyn std::error::Error>> {
    let source = super::ShapefileDataSource::new("./test/data/shapefile_utf8/points.shp", None)?;
    let specs = &source.column_specs;

    assert_eq!(specs.len(), 2);
    assert_eq!(specs[0].column_type, ColumnType::Double);
    assert_eq!(&specs[0].name, "属性1");
    assert_eq!(specs[1].column_type, ColumnType::Varchar);
    assert_eq!(&specs[1].name, "属性2");
    assert_eq!(row_character(&source, 0, "属性2").as_deref(), Some("値a"));
    assert_eq!(row_character(&source, 1, "属性2").as_deref(), Some("値b"));

    Ok(())
}

#[test]
fn test_get_column_specs_cp932() -> Result<(), Box<dyn std::error::Error>> {
    let source =
        super::ShapefileDataSource::new("./test/data/shapefile_cp932_wo_cpg/points.shp", None)?;
    let specs = &source.column_specs;

    assert_eq!(specs.len(), 2);
    assert_eq!(specs[0].column_type, ColumnType::Double);
    assert_eq!(&specs[0].name, "属性1");
    assert_eq!(specs[1].column_type, ColumnType::Varchar);
    assert_eq!(&specs[1].name, "属性2");
    assert_eq!(row_character(&source, 0, "属性2").as_deref(), Some("値a"));
    assert_eq!(row_character(&source, 1, "属性2").as_deref(), Some("値b"));

    Ok(())
}

#[test]
fn test_get_column_specs_cp932_with_cpg() -> Result<(), Box<dyn std::error::Error>> {
    let source =
        super::ShapefileDataSource::new("./test/data/shapefile_cp932_w_cpg/points.shp", None)?;
    let specs = &source.column_specs;

    assert_eq!(source.inferred_cpg_encoding.as_deref(), Some("Shift_JIS"));
    assert_eq!(specs.len(), 2);
    assert_eq!(specs[0].column_type, ColumnType::Double);
    assert_eq!(&specs[0].name, "属性1");
    assert_eq!(specs[1].column_type, ColumnType::Varchar);
    assert_eq!(&specs[1].name, "属性2");
    assert_eq!(row_character(&source, 0, "属性2").as_deref(), Some("値a"));
    assert_eq!(row_character(&source, 1, "属性2").as_deref(), Some("値b"));

    Ok(())
}
