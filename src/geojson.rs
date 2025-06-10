use std::{fs::File, path::Path};

use geojson::Feature;

use crate::{
    types::{ColumnSpec, ColumnType},
    VECTOR_SIZE,
};

// Note: NULL must be handled outside of this function
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
            _ => Err(format!("Unsupported type: {value:?}").into()),
        }
    }
}

#[repr(C)]
pub struct GeoJsonDataSource {
    pub features: Vec<Feature>,
    pub filename: String,
}

impl GeoJsonDataSource {
    // For simplicty, split to the size of 2048.
    pub(crate) fn parse_and_split<P: AsRef<Path>>(
        path: P,
    ) -> Result<(Vec<Self>, Vec<ColumnSpec>), Box<dyn std::error::Error>> {
        let path = path.as_ref();
        let mut column_specs: Vec<ColumnSpec> = Vec::new();

        let f = File::open(path)?;
        match geojson::GeoJson::from_reader(std::io::BufReader::new(f))? {
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
                    column_specs.push(ColumnSpec { name, column_type });
                }

                // Sort by name for consistent ordering
                column_specs.sort_by(|a, b| a.name.cmp(&b.name));

                let filename = path.to_string_lossy().into_owned();
                let data_sources = vec![GeoJsonDataSource {
                    features: feature_collection.features,
                    filename,
                }];

                Ok((data_sources, column_specs))
            }
            _ => Err(format!(
                "GeoJSON file must be FeatureCollection: {}",
                path.to_string_lossy().replace('\\', "/"),
            )
            .into()),
        }
    }
}

pub struct WkbConverter {
    buffer: Vec<u8>,
}

impl WkbConverter {
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    pub fn convert(&mut self, feature: &Feature) -> Result<&[u8], Box<dyn std::error::Error>> {
        self.buffer.clear();
        match &feature.geometry {
            Some(geojson_geom) => {
                let geometry: geo_types::Geometry = geojson_geom.try_into()?;
                wkb::writer::write_geometry(&mut self.buffer, &geometry, &Default::default())
                    .unwrap();
            }
            None => panic!("Geometry should exist!"),
        }
        Ok(&self.buffer)
    }
}
