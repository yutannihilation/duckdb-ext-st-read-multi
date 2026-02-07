extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

mod geojson;
mod gpkg;
mod shapefile;
mod types;
mod utils;

use duckdb::{
    core::{DataChunkHandle, FlatVector, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use geojson::WkbConverter;
use glob::glob;
use std::{
    error::Error,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::{
    geojson::GeoJsonDataSource,
    gpkg::{gpkg_geometry_to_wkb, Gpkg, GpkgDataSource},
    shapefile::ShapefileDataSource,
    types::{
        ColumnSpec, ColumnType, Cursor, GeoJsonBindData, GpkgBindData, ShapefileBindData,
        StReadMultiBindData, StReadMultiInitData,
    },
    utils::{expand_tilde, is_geojson, is_gpkg, is_shp, validate_schema},
};

// The data chunk size. This can be obtained via libduckdb_sys::duckdb_vector_size(),
// but use a fixed value here.
pub(crate) const VECTOR_SIZE: usize = 2048;

const COLUMN_NAME_FILENAME: &str = ".filename";
const COLUMN_NAME_LAYER: &str = ".layer";

struct StReadMultiVTab;

impl VTab for StReadMultiVTab {
    type InitData = StReadMultiInitData;
    type BindData = StReadMultiBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let path_pattern = bind.get_parameter(0).to_string();
        let expanded_pattern = expand_tilde(&path_pattern);
        let paths: Vec<PathBuf> = glob(&expanded_pattern)?.collect::<Result<_, _>>()?;

        if paths.is_empty() {
            return Err(format!("'{path_pattern}' doesn't match to any file").into());
        }

        // ==================== //
        //     GeoJSON          //
        // ==================== //

        if paths.iter().all(is_geojson) {
            let mut sources: Vec<GeoJsonDataSource> = Vec::new();
            let mut column_specs: Option<Vec<ColumnSpec>> = None;

            for path in paths {
                let (mut data_sources, column_specs_local) =
                    GeoJsonDataSource::parse_and_split(&path)?;
                sources.append(&mut data_sources);

                if let Some(existing_specs) = &column_specs {
                    // check if the schema matches
                    validate_schema(existing_specs, &column_specs_local, &path)?;
                } else {
                    // if it's the first file, use the spec as the base.
                    let _ = column_specs.insert(column_specs_local);
                }
            }

            let column_specs = column_specs.unwrap();

            bind.add_result_column("geometry", LogicalTypeId::Blob.into());
            for spec in column_specs.iter() {
                bind.add_result_column(&spec.name, spec.column_type.into());
            }

            // filename column to track source file
            bind.add_result_column(COLUMN_NAME_FILENAME, LogicalTypeId::Varchar.into());

            return Ok(GeoJsonBindData {
                sources,
                column_specs,
            }
            .into());
        }

        // ==================== //
        //     Gpkg             //
        // ==================== //

        if paths.iter().all(is_gpkg) {
            // Check if user specified a layer parameter
            let layer_name = bind.get_named_parameter("layer").map(|v| v.to_string());

            let mut sources: Vec<GpkgDataSource> = Vec::new();
            let mut column_specs: Option<Vec<ColumnSpec>> = None;

            for path in paths {
                let gpkg = Gpkg::new(&path, layer_name.clone())?;

                for source in gpkg.list_data_sources()? {
                    if let Some(existing_specs) = &column_specs {
                        // check if the schema matches
                        validate_schema(existing_specs, &source.column_specs, &path)?;
                    } else {
                        // if it's the first file, use the spec as the base.
                        let _ = column_specs.insert(source.column_specs.clone());
                    }
                    sources.push(source);
                }
            }

            let column_specs = column_specs.ok_or("No layers are found")?;

            for spec in column_specs.iter() {
                bind.add_result_column(&spec.name, spec.column_type.into());
            }

            // filename and layer column to track source
            bind.add_result_column(COLUMN_NAME_FILENAME, LogicalTypeId::Varchar.into());
            bind.add_result_column(COLUMN_NAME_LAYER, LogicalTypeId::Varchar.into());

            return Ok(GpkgBindData {
                sources,
                column_specs,
            }
            .into());
        }

        // ==================== //
        //     Shapefile        //
        // ==================== //

        if paths.iter().all(is_shp) {
            let mut sources: Vec<ShapefileDataSource> = Vec::new();
            let mut column_specs: Option<Vec<ColumnSpec>> = None;

            for path in paths {
                let source = ShapefileDataSource::new(&path)?;
                let column_specs_local = source.column_specs.clone();

                if let Some(existing_specs) = &column_specs {
                    validate_schema(existing_specs, &column_specs_local, &path)?;
                } else {
                    let _ = column_specs.insert(column_specs_local);
                }

                sources.push(source);
            }

            let column_specs = column_specs.unwrap();

            bind.add_result_column("geometry", LogicalTypeId::Blob.into());
            for spec in column_specs.iter() {
                bind.add_result_column(&spec.name, spec.column_type.into());
            }

            bind.add_result_column(COLUMN_NAME_FILENAME, LogicalTypeId::Varchar.into());

            return Ok(ShapefileBindData {
                sources,
                column_specs,
            }
            .into());
        }

        Err("All files must have extension '.geojson', '.gpkg', or '.shp'".into())
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(StReadMultiInitData {
            cursor: Arc::new(Mutex::new(Cursor::new())),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        let mut cursor = match init_data.cursor.lock() {
            Ok(cursor) => cursor,
            Err(_) => return Err("Failed to acquire the lock of the cursor".into()),
        };

        match bind_data {
            // ==================== //
            //     GeoJSON          //
            // ==================== //
            StReadMultiBindData::GeoJson(bind_data_inner) => {
                // If there's no remaining data source, tell DuckDB it's over.
                if cursor.source_idx >= bind_data_inner.sources.len() {
                    output.set_len(0);
                    return Ok(());
                }

                let geom_vector = output.flat_vector(0);
                let n_props = bind_data_inner.column_specs.len();
                let mut property_vectors: Vec<FlatVector> =
                    (0..n_props).map(|i| output.flat_vector(i + 1)).collect();
                let filename_vector = output.flat_vector(n_props + 1);

                let mut row_idx: usize = 0;
                let mut wkb_converter = WkbConverter::new();
                let source = &bind_data_inner.sources[cursor.source_idx];

                let range_end = std::cmp::min(cursor.offset + VECTOR_SIZE, source.features.len());
                let last = range_end >= source.features.len();
                let range = cursor.offset..range_end;

                for f in &source.features[range] {
                    let wkb_data = wkb_converter.convert(f)?;
                    geom_vector.insert(row_idx, wkb_data);
                    filename_vector.insert(row_idx, source.filename.as_str());

                    if let Some(properties) = &f.properties {
                        for (prop_idx, spec) in bind_data_inner.column_specs.iter().enumerate() {
                            let val = properties.get(&spec.name);

                            match val {
                                Some(v) if !v.is_null() => {
                                    match spec.column_type {
                                        // Varchar needs insert()
                                        ColumnType::Varchar => {
                                            property_vectors[prop_idx]
                                                .insert(row_idx, v.as_str().unwrap());
                                        }
                                        ColumnType::Boolean => {
                                            property_vectors[prop_idx].as_mut_slice()[row_idx] =
                                                v.as_bool().unwrap();
                                        }
                                        ColumnType::Double => {
                                            property_vectors[prop_idx].as_mut_slice()[row_idx] =
                                                v.as_f64().unwrap();
                                        }
                                        // JSON doesn't have integer type.
                                        _ => unreachable!(),
                                    }
                                }
                                _ => {
                                    // Handle NULL or missing values
                                    property_vectors[prop_idx].set_null(row_idx);
                                }
                            }
                        }
                    }

                    row_idx += 1;
                }

                if last {
                    cursor.source_idx += 1;
                    cursor.offset = 0;
                } else {
                    cursor.offset += VECTOR_SIZE;
                }

                output.set_len(row_idx);
                return Ok(());
            }

            // ==================== //
            //     Gpkg             //
            // ==================== //
            StReadMultiBindData::Gpkg(bind_data_inner) => {
                // If there's no remaining data source, tell DuckDB it's over.
                if cursor.source_idx >= bind_data_inner.sources.len() {
                    output.set_len(0);
                    return Ok(());
                }

                let n_props = bind_data_inner.column_specs.len();
                let mut property_vectors: Vec<FlatVector> =
                    (0..n_props).map(|i| output.flat_vector(i)).collect();

                let filename_vector = output.flat_vector(n_props);
                let layer_name_vector = output.flat_vector(n_props + 1);

                // Note: This for loop is a bit tricky. This is necessary to let this function
                // return non-empty result, otherwise DuckDB would assume the query is done.
                for source in &bind_data_inner.sources[cursor.source_idx..] {
                    let mut conn = source.gpkg.conn.lock().unwrap();

                    let row_count =
                        conn.fetch_rows(&source.sql, cursor.offset, |row, row_idx: usize| {
                            // Insert filename
                            filename_vector.insert(row_idx, source.gpkg.path.as_str());
                            layer_name_vector.insert(row_idx, source.layer_name.as_str());

                            for (col_idx, spec) in source.column_specs.iter().enumerate() {
                                match &spec.column_type {
                                    ColumnType::Integer => {
                                        let val: Option<i64> = row.get(col_idx)?;
                                        match val {
                                            Some(v) => {
                                                property_vectors[col_idx].as_mut_slice()[row_idx] =
                                                    v as i32
                                            }
                                            None => property_vectors[col_idx].set_null(row_idx),
                                        }
                                    }
                                    ColumnType::Double => {
                                        let val: Option<f64> = row.get(col_idx)?;
                                        match val {
                                            Some(v) => {
                                                property_vectors[col_idx].as_mut_slice()[row_idx] =
                                                    v
                                            }
                                            None => property_vectors[col_idx].set_null(row_idx),
                                        }
                                    }
                                    ColumnType::Varchar => {
                                        let val: Option<String> = row.get(col_idx)?;
                                        match val {
                                            Some(v) => property_vectors[col_idx]
                                                .insert(row_idx, v.as_str()),
                                            None => property_vectors[col_idx].set_null(row_idx),
                                        }
                                    }
                                    ColumnType::Boolean => {
                                        let val: Option<bool> = row.get(col_idx)?;
                                        match val {
                                            Some(v) => {
                                                property_vectors[col_idx].as_mut_slice()[row_idx] =
                                                    v
                                            }
                                            None => property_vectors[col_idx].set_null(row_idx),
                                        }
                                    }
                                    ColumnType::Geometry => {
                                        let val: Option<Vec<u8>> = row.get(col_idx)?;
                                        match val {
                                            Some(v) => property_vectors[col_idx]
                                                .insert(row_idx, gpkg_geometry_to_wkb(&v)),
                                            None => property_vectors[col_idx].set_null(row_idx),
                                        }
                                    }
                                }
                            }

                            Ok(())
                        })?;

                    match row_count {
                        // This is a special case. While we want to just return the result,
                        // to DuckDB, 0-row result means it's finished. But, we don't want
                        // to finish as long as there's any remaining data sources.
                        0 => {
                            cursor.source_idx += 1;
                            cursor.offset = 0;

                            if cursor.source_idx < bind_data_inner.sources.len() {
                                continue;
                            }
                        }
                        // return the current result and proceed to the next data source
                        1..VECTOR_SIZE => {
                            cursor.source_idx += 1;
                            cursor.offset = 0;
                        }
                        // return the current result and continue on the current data source
                        VECTOR_SIZE => {
                            cursor.offset += row_count;
                        }
                        _ => unreachable!(),
                    }

                    // return result and break this loop.
                    output.set_len(row_count);
                    return Ok(());
                }
            }

            // ==================== //
            //     Shapefile        //
            // ==================== //
            StReadMultiBindData::Shapefile(bind_data_inner) => {
                if cursor.source_idx >= bind_data_inner.sources.len() {
                    output.set_len(0);
                    return Ok(());
                }

                let mut geom_vector = output.flat_vector(0);
                let n_props = bind_data_inner.column_specs.len();
                let mut property_vectors: Vec<FlatVector> =
                    (0..n_props).map(|i| output.flat_vector(i + 1)).collect();
                let filename_vector = output.flat_vector(n_props + 1);

                let mut row_idx: usize = 0;
                let source = &bind_data_inner.sources[cursor.source_idx];

                let range_end = std::cmp::min(cursor.offset + VECTOR_SIZE, source.rows.len());
                let last = range_end >= source.rows.len();
                let range = cursor.offset..range_end;

                for row in &source.rows[range] {
                    match &row.geometry {
                        Some(wkb_data) => geom_vector.insert(row_idx, wkb_data.as_slice()),
                        None => geom_vector.set_null(row_idx),
                    }
                    filename_vector.insert(row_idx, source.filename.as_str());

                    for (prop_idx, spec) in bind_data_inner.column_specs.iter().enumerate() {
                        let val = row.record.get(&spec.name);

                        use ::shapefile::dbase::FieldValue;
                        match (spec.column_type, val) {
                            (ColumnType::Varchar, Some(FieldValue::Character(Some(v)))) => {
                                property_vectors[prop_idx].insert(row_idx, v.as_str());
                            }
                            (ColumnType::Varchar, Some(FieldValue::Memo(v))) => {
                                property_vectors[prop_idx].insert(row_idx, v.as_str());
                            }
                            (ColumnType::Varchar, Some(FieldValue::Date(Some(v)))) => {
                                property_vectors[prop_idx].insert(row_idx, v.to_string().as_str());
                            }
                            (ColumnType::Boolean, Some(FieldValue::Logical(Some(v)))) => {
                                property_vectors[prop_idx].as_mut_slice()[row_idx] = *v;
                            }
                            (ColumnType::Integer, Some(FieldValue::Integer(v))) => {
                                property_vectors[prop_idx].as_mut_slice()[row_idx] = *v;
                            }
                            (ColumnType::Double, Some(FieldValue::Numeric(Some(v)))) => {
                                property_vectors[prop_idx].as_mut_slice()[row_idx] = *v;
                            }
                            (ColumnType::Double, Some(FieldValue::Float(Some(v)))) => {
                                property_vectors[prop_idx].as_mut_slice()[row_idx] = *v as f64;
                            }
                            (ColumnType::Double, Some(FieldValue::Currency(v)))
                            | (ColumnType::Double, Some(FieldValue::Double(v))) => {
                                property_vectors[prop_idx].as_mut_slice()[row_idx] = *v;
                            }
                            (ColumnType::Double, Some(FieldValue::DateTime(v))) => {
                                property_vectors[prop_idx].as_mut_slice()[row_idx] =
                                    v.to_unix_timestamp() as f64;
                            }
                            _ => {
                                property_vectors[prop_idx].set_null(row_idx);
                            }
                        }
                    }

                    row_idx += 1;
                }

                if last {
                    cursor.source_idx += 1;
                    cursor.offset = 0;
                } else {
                    cursor.offset += VECTOR_SIZE;
                }

                output.set_len(row_idx);
                return Ok(());
            }
        }

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeId::Varchar.into()])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![("layer".into(), LogicalTypeId::Varchar.into())])
    }
}

const EXTENSION_NAME: &str = env!("CARGO_PKG_NAME");

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<StReadMultiVTab>(EXTENSION_NAME)
        .expect("Failed to register StReadMulti table function");
    Ok(())
}
