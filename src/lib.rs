extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

mod geojson;
mod gpkg;
mod types;
mod utils;
mod wkb;

use duckdb::{
    core::{DataChunkHandle, FlatVector, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use glob::glob;
use libduckdb_sys as ffi;
use std::{
    error::Error,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};
use wkb::WkbConverter;

use crate::{
    geojson::GeoJsonDataSource,
    gpkg::{gpkg_geometry_to_wkb, Gpkg, GpkgDataSource},
    types::{
        ColumnSpec, ColumnType, GeoJsonBindData, GpkgBindData, StReadMultiBindData,
        StReadMultiInitData,
    },
    utils::{expand_tilde, is_geojson, is_gpkg, validate_schema},
};

struct StReadMultiVTab;

impl VTab for StReadMultiVTab {
    type InitData = StReadMultiInitData;
    type BindData = StReadMultiBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        // geometry column must exist
        bind.add_result_column("geometry", LogicalTypeId::Blob.into());

        let path_pattern = bind.get_parameter(0).to_string();
        let expanded_pattern = expand_tilde(&path_pattern);
        let paths: Vec<PathBuf> = glob(&expanded_pattern)?.collect::<Result<_, _>>()?;

        if paths.is_empty() {
            return Err("Doesn't match to any file".into());
        }

        if paths.iter().all(is_geojson) {
            let mut sources: Vec<GeoJsonDataSource> = Vec::new();
            let mut column_specs: Option<Vec<ColumnSpec>> = None;

            for path in paths {
                let (data_source, column_specs_local) = GeoJsonDataSource::parse(&path)?;
                sources.push(data_source);

                if let Some(existing_specs) = &column_specs {
                    // check if the schema matches
                    validate_schema(existing_specs, &column_specs_local, &path)?;
                } else {
                    // if it's the first file, use the spec as the base.
                    let _ = column_specs.insert(column_specs_local);
                }
            }

            let column_specs = column_specs.unwrap();

            for spec in column_specs.iter() {
                bind.add_result_column(&spec.name, spec.column_type.into());
            }

            // filename column to track source file
            bind.add_result_column("filename", LogicalTypeId::Varchar.into());

            return Ok(GeoJsonBindData {
                sources,
                column_specs,
            }
            .into());
        }

        if paths.iter().all(is_gpkg) {
            // Check if user specified a layer parameter
            let layer_name = bind.get_named_parameter("layer").map(|x| x.to_string());

            let mut all_sources: Vec<GpkgDataSource> = Vec::new();
            let mut column_specs: Option<Vec<ColumnSpec>> = None;

            for path in paths {
                let sources = Gpkg::read_layer_data(&path, layer_name.as_deref())?;

                for source in sources {
                    if let Some(existing_specs) = &column_specs {
                        // check if the schema matches
                        validate_schema(existing_specs, &source.column_specs, &path)?;
                    } else {
                        // if it's the first file, use the spec as the base.
                        let _ = column_specs.insert(source.column_specs.clone());
                    }
                    all_sources.push(source);
                }
            }

            let column_specs = column_specs.unwrap();

            // Add columns excluding geometry (which is already added at the top)
            for spec in column_specs.iter() {
                if spec.column_type != ColumnType::Geometry {
                    bind.add_result_column(&spec.name, spec.column_type.into());
                }
            }

            // filename column to track source file
            bind.add_result_column("filename", LogicalTypeId::Varchar.into());

            return Ok(GpkgBindData {
                sources: all_sources,
                column_specs,
            }
            .into());
        }

        Err("All file must have extension of either '.geojson' or '.gpkg'".into())
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(StReadMultiInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
        } else {
            match bind_data {
                StReadMultiBindData::GeoJson(bind_data_inner) => {
                    let geom_vector = output.flat_vector(0);
                    let n_props = bind_data_inner.column_specs.len();
                    let mut property_vectors: Vec<FlatVector> =
                        (0..n_props).map(|i| output.flat_vector(i + 1)).collect();
                    let filename_vector = output.flat_vector(n_props + 1);

                    let mut row_idx: usize = 0;
                    let mut wkb_converter = WkbConverter::new();
                    for source in &bind_data_inner.sources {
                        let fc = &source.feature_collection;
                        for f in &fc.features {
                            let wkb_data = wkb_converter.convert(f)?;
                            geom_vector.insert(row_idx, wkb_data);
                            filename_vector.insert(row_idx, source.filename.as_str());

                            if let Some(properties) = &f.properties {
                                for (prop_idx, spec) in
                                    bind_data_inner.column_specs.iter().enumerate()
                                {
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
                                                    property_vectors[prop_idx].as_mut_slice()
                                                        [row_idx] = v.as_bool().unwrap();
                                                }
                                                ColumnType::Double => {
                                                    property_vectors[prop_idx].as_mut_slice()
                                                        [row_idx] = v.as_f64().unwrap();
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
                    }

                    output.set_len(row_idx);
                }
                StReadMultiBindData::Gpkg(bind_data_inner) => {
                    let mut geom_vector = output.flat_vector(0);
                    let mut property_vectors: Vec<FlatVector> = Vec::new();
                    let mut property_indices: Vec<usize> = Vec::new();

                    // Create vectors only for non-geometry columns
                    let mut vec_idx = 1;
                    for (idx, spec) in bind_data_inner.column_specs.iter().enumerate() {
                        if spec.column_type != ColumnType::Geometry {
                            property_vectors.push(output.flat_vector(vec_idx));
                            property_indices.push(idx);
                            vec_idx += 1;
                        }
                    }
                    let filename_vector = output.flat_vector(vec_idx);

                    let mut row_idx: usize = 0;

                    for source in &bind_data_inner.sources {
                        for row_data in &source.data {
                            // Find geometry column index
                            let geom_idx = source
                                .column_specs
                                .iter()
                                .position(|spec| spec.column_type == ColumnType::Geometry)
                                .ok_or("No geometry column found")?;

                            // Insert geometry
                            if let rusqlite::types::Value::Blob(wkb) = &row_data[geom_idx] {
                                geom_vector.insert(row_idx, gpkg_geometry_to_wkb(wkb));
                            } else {
                                geom_vector.set_null(row_idx);
                            }

                            // Insert filename
                            filename_vector.insert(row_idx, source.filename.as_str());

                            // Insert other properties
                            let mut prop_vec_idx = 0;
                            for (col_idx, spec) in source.column_specs.iter().enumerate() {
                                if spec.column_type == ColumnType::Geometry {
                                    continue;
                                }

                                match &row_data[col_idx] {
                                    rusqlite::types::Value::Null => {
                                        property_vectors[prop_vec_idx].set_null(row_idx);
                                    }
                                    rusqlite::types::Value::Integer(v) => match spec.column_type {
                                        ColumnType::Integer => {
                                            property_vectors[prop_vec_idx].as_mut_slice()
                                                [row_idx] = *v as i32;
                                        }
                                        ColumnType::Boolean => {
                                            property_vectors[prop_vec_idx].as_mut_slice()
                                                [row_idx] = *v != 0;
                                        }
                                        _ => return Err("Type mismatch".into()),
                                    },
                                    rusqlite::types::Value::Real(v) => {
                                        property_vectors[prop_vec_idx].as_mut_slice()[row_idx] = *v;
                                    }
                                    rusqlite::types::Value::Text(v) => {
                                        property_vectors[prop_vec_idx].insert(row_idx, v.as_str());
                                    }
                                    rusqlite::types::Value::Blob(_) => {
                                        // Should only be geometry, which we already handled
                                        return Err("Unexpected blob in non-geometry column".into());
                                    }
                                }
                                prop_vec_idx += 1;
                            }

                            row_idx += 1;
                        }
                    }

                    output.set_len(row_idx);
                }
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
