use crate::types::ColumnType;

#[test]
fn test_get_column_specs() -> Result<(), Box<dyn std::error::Error>> {
    let source = super::ShapefileDataSource::new("./test/data/shapefile_utf8/points.shp")?;
    let specs = source.get_column_specs("points")?;

    assert_eq!(specs.len(), 2);
    assert_eq!(specs[0].column_type, ColumnType::Double);
    assert_eq!(&specs[0].name, "\u{5c5e}\u{6027}1");
    assert_eq!(specs[1].column_type, ColumnType::Varchar);
    assert_eq!(&specs[1].name, "\u{5c5e}\u{6027}2");

    Ok(())
}

#[test]
fn test_get_column_specs_cp932() -> Result<(), Box<dyn std::error::Error>> {
    let source = super::ShapefileDataSource::new("./test/data/shapefile_cp932_wo_cpg/points.shp")?;
    let specs = source.get_column_specs("points")?;

    assert_eq!(specs.len(), 2);
    assert_eq!(specs[0].column_type, ColumnType::Double);
    assert_eq!(&specs[0].name, "\u{5c5e}\u{6027}1");
    assert_eq!(specs[1].column_type, ColumnType::Varchar);
    assert_eq!(&specs[1].name, "\u{5c5e}\u{6027}2");

    Ok(())
}

#[test]
fn test_get_column_specs_cp932_with_cpg() -> Result<(), Box<dyn std::error::Error>> {
    let source = super::ShapefileDataSource::new("./test/data/shapefile_cp932_w_cpg/points.shp")?;
    let specs = source.get_column_specs("points")?;

    assert_eq!(source.inferred_cpg_encoding.as_deref(), Some("Shift_JIS"));
    assert_eq!(specs.len(), 2);
    assert_eq!(specs[0].column_type, ColumnType::Double);
    assert_eq!(&specs[0].name, "\u{5c5e}\u{6027}1");
    assert_eq!(specs[1].column_type, ColumnType::Varchar);
    assert_eq!(&specs[1].name, "\u{5c5e}\u{6027}2");

    Ok(())
}
