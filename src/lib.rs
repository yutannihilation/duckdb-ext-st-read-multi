extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use duckdb::{
    core::{DataChunkHandle, FlatVector, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use geojson::FeatureCollection;
use glob::glob;
use libduckdb_sys as ffi;
use std::{
    error::Error,
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(C)]
enum ColumnType {
    Boolean,
    Varchar,
    Double,
}

// TODO: NULL should be handled outside of this function
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
            _ => {
                return Err(format!("Unsupported type: {value:?}").into());
            }
        }
    }
}

impl From<ColumnType> for LogicalTypeHandle {
    fn from(value: ColumnType) -> Self {
        match value {
            ColumnType::Boolean => LogicalTypeId::Boolean.into(),
            ColumnType::Double => LogicalTypeId::Double.into(),
            ColumnType::Varchar => LogicalTypeId::Varchar.into(),
        }
    }
}

#[repr(C)]
struct ColumnSpec {
    name: String,
    column_type: ColumnType,
}

#[repr(C)]
struct FeatureCollectionWithSource {
    feature_collection: FeatureCollection,
    filename: String,
}

#[repr(C)]
struct StReadMultiBindData {
    sources: Vec<FeatureCollectionWithSource>,
    column_specs: Vec<ColumnSpec>,
}

#[repr(C)]
struct StReadMultiInitData {
    done: AtomicBool,
}

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

        if !(paths.iter().all(is_geojson) || paths.iter().all(is_gpkg)) {
            return Err("All file must have extension of either '.geojson' or '.gpkg'".into());
        }

        let mut sources: Vec<FeatureCollectionWithSource> = Vec::new();
        let mut column_specs: Option<Vec<ColumnSpec>> = None;

        for path in paths {
            let mut column_specs_local: Vec<ColumnSpec> = Vec::new();

            let f = File::open(&path)?;
            match geojson::GeoJson::from_reader(BufReader::new(f))? {
                geojson::GeoJson::FeatureCollection(feature_collection) => {
                    // Use first 100 features to determine schema
                    let sample_size = std::cmp::min(100, feature_collection.features.len());
                    let mut property_type_map: std::collections::HashMap<String, ColumnType> =
                        std::collections::HashMap::new();

                    for i in 0..sample_size {
                        for (key, val) in feature_collection.features[i].properties_iter() {
                            // Skip NULL values
                            if val.is_null() {
                                continue;
                            }

                            let column_type: ColumnType = val.try_into()?;

                            // If key doesn't exist yet or current type is more specific, update it
                            property_type_map
                                .entry(key.to_string())
                                .or_insert(column_type);
                        }
                    }

                    // Convert to ordered vector
                    for (name, column_type) in property_type_map {
                        column_specs_local.push(ColumnSpec { name, column_type });
                    }

                    // Sort by name for consistent ordering
                    column_specs_local.sort_by(|a, b| a.name.cmp(&b.name));

                    sources.push(FeatureCollectionWithSource {
                        feature_collection,
                        filename: path.to_string_lossy().into_owned(),
                    });
                }
                _ => {
                    return Err(format!(
                        "GeoJSON file must be FeatureCollection: {}",
                        path.to_string_lossy().replace('\\', "/"),
                    )
                    .into());
                }
            }

            if column_specs.is_none() {
                let _ = column_specs.insert(column_specs_local);
            } else {
                // Verify if the schema matches
                let existing_specs = column_specs.as_ref().unwrap();

                // Check if the number of columns matches
                if existing_specs.len() != column_specs_local.len() {
                    return Err(format!(
                        "Schema mismatch in {}: expected {} columns, found {}",
                        path.to_string_lossy().replace('\\', "/"),
                        existing_specs.len(),
                        column_specs_local.len()
                    )
                    .into());
                }

                // Since both are sorted by name, we can compare directly
                for (i, (existing, local)) in existing_specs
                    .iter()
                    .zip(column_specs_local.iter())
                    .enumerate()
                {
                    if existing.name != local.name {
                        return Err(format!(
                            "Schema mismatch in {}: column {} has name '{}', expected '{}'",
                            path.to_string_lossy().replace('\\', "/"),
                            i,
                            local.name,
                            existing.name
                        )
                        .into());
                    }

                    if &existing.column_type != &local.column_type {
                        return Err(format!(
                            "Schema mismatch in {}: column '{}' has type {:?}, expected {:?}",
                            path.to_string_lossy().replace('\\', "/"),
                            local.name,
                            local.column_type,
                            existing.column_type
                        )
                        .into());
                    }
                }
            }
        }

        let column_specs = column_specs.unwrap();

        for spec in column_specs.iter() {
            bind.add_result_column(&spec.name, spec.column_type.into());
        }

        // filename column to track source file
        bind.add_result_column("filename", LogicalTypeId::Varchar.into());

        Ok(StReadMultiBindData {
            sources,
            column_specs,
        })
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
            let geom_vector = output.flat_vector(0);
            let n_props = bind_data.column_specs.len();
            let mut property_vectors: Vec<FlatVector> =
                (0..n_props).map(|i| output.flat_vector(i + 1)).collect();
            let filename_vector = output.flat_vector(n_props + 1);

            let mut row_idx: usize = 0;
            for source in &bind_data.sources {
                let fc = &source.feature_collection;
                for f in &fc.features {
                    let b = feature_to_wkb(f)?;
                    let b_ref: &[u8] = b.as_ref();
                    geom_vector.insert(row_idx, b_ref);
                    filename_vector.insert(row_idx, source.filename.as_str());

                    if let Some(properties) = &f.properties {
                        for (prop_idx, spec) in bind_data.column_specs.iter().enumerate() {
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
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

// glob() doesn't handle tilda, so I have to.
fn expand_tilde(path: &str) -> String {
    if path.starts_with('~') {
        if let Some(home) = home::home_dir() {
            path.replacen('~', &home.to_string_lossy(), 1)
        } else {
            path.to_string()
        }
    } else {
        path.to_string()
    }
}

fn is_geojson<P: AsRef<Path>>(path: P) -> bool {
    match path.as_ref().extension() {
        Some(ext) => ext.to_string_lossy() == "geojson",
        None => false,
    }
}

fn is_gpkg<P: AsRef<Path>>(path: P) -> bool {
    match path.as_ref().extension() {
        Some(ext) => ext.to_string_lossy() == "gpkg",
        None => false,
    }
}

fn feature_to_wkb(feature: &geojson::Feature) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut buffer = Vec::new();
    match &feature.geometry {
        Some(geojson_geom) => {
            let geometry: geo_types::Geometry = geojson_geom.try_into()?;
            wkb::writer::write_geometry(&mut buffer, &geometry, &Default::default()).unwrap();
        }
        None => panic!("Geometry should exist!"),
    }

    Ok(buffer)
}

const EXTENSION_NAME: &str = env!("CARGO_PKG_NAME");

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<StReadMultiVTab>(EXTENSION_NAME)
        .expect("Failed to register StReadMulti table function");
    Ok(())
}
