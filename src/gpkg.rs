use crate::types::{ColumnSpec, ColumnType};
use rusqlite::{Connection, OpenFlags, Result};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

pub struct GpkgDataSource {
    // TODO: Remove this as gpkg contains the filename
    pub filename: String,
    pub layer_name: String,
    pub column_specs: Vec<ColumnSpec>,
    pub gpkg: Gpkg,
}

#[derive(Clone)]
pub struct Gpkg {
    pub conn: Arc<Mutex<Connection>>,
    path: String,
    pub layers: Vec<String>,
}

impl Gpkg {
    pub(crate) fn new<P: AsRef<Path>>(
        path: P,
        layer_name: Option<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let conn = Connection::open_with_flags(
            path.as_ref(),
            OpenFlags::SQLITE_OPEN_READ_ONLY, // open as read only
        )?;

        let mut stmt = conn.prepare("SELECT table_name FROM gpkg_contents")?;
        let layers = stmt
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        drop(stmt);

        let path = path.as_ref().to_string_lossy().to_string();
        if let Some(layer_name) = layer_name {
            // If layer is specified, the data must contain the layer of the
            // same name. Otherwise, it fails.
            if !layers.contains(&layer_name) {
                return Err(format!("No such layer '{layer_name}' in {path}",).into());
            }

            Ok(Self {
                conn: Arc::new(Mutex::new(conn)),
                path,
                layers: vec![layer_name],
            })
        } else {
            // If layer is not specified, return all the layers
            Ok(Self {
                conn: Arc::new(Mutex::new(conn)),
                path,
                layers,
            })
        }
    }

    pub(crate) fn get_column_specs<T: AsRef<str>>(
        &self,
        table_name: T,
    ) -> Result<Vec<ColumnSpec>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().unwrap();

        let query = format!(
            "SELECT name, type FROM pragma_table_info('{}') WHERE name != 'fid'",
            table_name.as_ref()
        );
        let mut stmt = conn.prepare(&query)?;

        let column_specs = stmt.query_map([], |row| {
            let name: String = row.get(0)?;
            let column_type_str: String = row.get(1)?;

            // cf. https://www.geopackage.org/spec140/index.html#_sqlite_container
            let column_type = match column_type_str.to_uppercase().as_str() {
                "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" => ColumnType::Integer,
                "DOUBLE" | "FLOAT" | "REAL" => ColumnType::Double,
                "TEXT" => ColumnType::Varchar,
                "BOOLEAN" => ColumnType::Boolean,
                // cf. https://www.geopackage.org/spec140/index.html#geometry_types
                "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT"
                | "MULTILINESTRING" | "MULTIPOLYGON" | "GEOMETRYCOLLECTION" => ColumnType::Geometry,
                _ => {
                    return Err(rusqlite::Error::InvalidColumnType(
                        1,
                        format!("Unexpected type {}", column_type_str),
                        rusqlite::types::Type::Text,
                    ))
                }
            };

            Ok(ColumnSpec { name, column_type })
        })?;

        let result: Result<Vec<ColumnSpec>, rusqlite::Error> = column_specs.collect();
        Ok(result?)
    }

    pub(crate) fn list_data_sources(
        &self,
    ) -> Result<Vec<GpkgDataSource>, Box<dyn std::error::Error>> {
        let mut sources = Vec::new();

        for layer in &self.layers {
            let column_specs = self.get_column_specs(layer)?;
            sources.push(GpkgDataSource {
                filename: self.path.clone(),
                layer_name: layer.to_string(),
                column_specs,
                gpkg: self.clone(),
            });
        }

        Ok(sources)
    }
}

// cf. https://www.geopackage.org/spec140/index.html#gpb_format
pub(crate) fn gpkg_geometry_to_wkb(b: &[u8]) -> &[u8] {
    let flags = b[3];
    let envelope_size: usize = match flags & 0b00001110 {
        0b00000000 => 0,  // no envelope
        0b00000010 => 32, // envelope is [minx, maxx, miny, maxy], 32 bytes
        0b00000100 => 48, // envelope is [minx, maxx, miny, maxy, minz, maxz], 48 bytes
        0b00000110 => 48, // envelope is [minx, maxx, miny, maxy, minm, maxm], 48 bytes
        0b00001000 => 64, // envelope is [minx, maxx, miny, maxy, minz, maxz, minm, maxm], 64 bytes
        _ => return &[],  // invalid
    };
    let offset = 8 + envelope_size;

    &b[offset..]
}

#[cfg(test)]
mod tests {
    use crate::types::ColumnType;

    #[test]
    fn test_get_column_specs() -> Result<(), Box<dyn std::error::Error>> {
        let gpkg = super::Gpkg::new("./test/data/points.gpkg", None)?;
        let layers = gpkg.get_column_specs("points")?;

        assert_eq!(layers.len(), 3);
        assert_eq!(&layers[0].name, "geom");
        assert_eq!(layers[0].column_type, ColumnType::Geometry);
        assert_eq!(&layers[1].name, "val1");
        assert_eq!(layers[1].column_type, ColumnType::Integer);
        assert_eq!(&layers[2].name, "val2");
        assert_eq!(layers[2].column_type, ColumnType::Varchar);

        Ok(())
    }
}
