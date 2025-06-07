use std::{fs::File, path::Path};

use geojson::FeatureCollection;

use crate::types::{ColumnSpec, ColumnType};

#[repr(C)]
pub struct GeoJsonDataSource {
    pub feature_collection: FeatureCollection,
    pub filename: String,
}

impl GeoJsonDataSource {
    pub(crate) fn parse<P: AsRef<Path>>(
        path: P,
    ) -> Result<(Self, Vec<ColumnSpec>), Box<dyn std::error::Error>> {
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

                let data_source = GeoJsonDataSource {
                    feature_collection,
                    filename: path.to_string_lossy().into_owned(),
                };

                Ok((data_source, column_specs))
            }
            _ => Err(format!(
                "GeoJSON file must be FeatureCollection: {}",
                path.to_string_lossy().replace('\\', "/"),
            )
            .into()),
        }
    }
}
