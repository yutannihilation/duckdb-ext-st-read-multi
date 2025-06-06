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
    sync::atomic::{AtomicBool, Ordering},
};

#[derive(Clone, Copy, Debug)]
#[repr(C)]
enum GeoJsonColumnType {
    Boolean,
    Varchar,
    Double,
}

// TODO: NULL should be handled outside of this function
impl TryFrom<&serde_json::Value> for GeoJsonColumnType {
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

impl From<GeoJsonColumnType> for LogicalTypeHandle {
    fn from(value: GeoJsonColumnType) -> Self {
        match value {
            GeoJsonColumnType::Boolean => LogicalTypeId::Boolean.into(),
            GeoJsonColumnType::Double => LogicalTypeId::Double.into(),
            GeoJsonColumnType::Varchar => LogicalTypeId::Varchar.into(),
        }
    }
}

#[repr(C)]
struct ColumnSpec {
    name: String,
    column_type: GeoJsonColumnType,
}

#[repr(C)]
struct StReadMultiBindData {
    fc: Vec<FeatureCollection>,
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

        let mut fc: Vec<FeatureCollection> = Vec::new();
        let mut column_specs: Option<Vec<ColumnSpec>> = None;

        for entry in glob(&path_pattern)? {
            let mut column_specs_local: Vec<ColumnSpec> = Vec::new();

            let path = entry?;
            let f = File::open(&path)?;
            match geojson::GeoJson::from_reader(BufReader::new(f))? {
                geojson::GeoJson::FeatureCollection(feature_collection) => {
                    for (key, val) in feature_collection.features[0].properties_iter() {
                        let column_type = val.try_into()?;
                        column_specs_local.push(ColumnSpec {
                            name: key.to_string(),
                            column_type,
                        });
                    }

                    fc.push(feature_collection);
                }
                _ => {
                    return Err(format!(
                        "GeoJSON file must be FeatureCollection: {}",
                        path.to_string_lossy()
                    )
                    .into());
                }
            }

            if column_specs.is_none() {
                let _ = column_specs.insert(column_specs_local);
            } else {
                // TODO: verify if the schema matches
            }
        }

        let column_specs = column_specs.unwrap();

        for spec in column_specs.iter() {
            bind.add_result_column(&spec.name, spec.column_type.into());
        }

        Ok(StReadMultiBindData { fc, column_specs })
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
            let mut property_vectors: Vec<FlatVector> = (0..bind_data.column_specs.len())
                .map(|i| output.flat_vector(i + 1))
                .collect();

            let mut row_idx: usize = 0;
            for fc in &bind_data.fc {
                for f in &fc.features {
                    let b = feature_to_wkb(f)?;
                    let b_ref: &[u8] = b.as_ref();
                    geom_vector.insert(row_idx, b_ref);

                    if let Some(properties) = &f.properties {
                        for (prop_idx, spec) in bind_data.column_specs.iter().enumerate() {
                            let val = properties.get(&spec.name).unwrap();

                            match spec.column_type {
                                // Varchar needs insert()
                                GeoJsonColumnType::Varchar => {
                                    property_vectors[prop_idx]
                                        .insert(row_idx, val.as_str().unwrap());
                                }
                                GeoJsonColumnType::Boolean => {
                                    property_vectors[prop_idx].as_mut_slice()[row_idx] =
                                        val.as_bool().unwrap();
                                }
                                GeoJsonColumnType::Double => {
                                    property_vectors[prop_idx].as_mut_slice()[row_idx] =
                                        val.as_f64().unwrap();
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
