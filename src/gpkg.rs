use crate::types::{ColumnSpec, ColumnType};
use rusqlite::{Connection, Result};
use std::path::Path;

pub struct GpkgDataSource {
    pub filename: String,
    pub layer_name: String,
    pub column_specs: Vec<ColumnSpec>,
    pub data: Vec<Vec<rusqlite::types::Value>>,
}

pub struct Gpkg {
    conn: Connection,
}

impl Gpkg {
    pub(crate) fn new<P: AsRef<str>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let conn = Connection::open(path.as_ref())?;
        Ok(Self { conn })
    }

    pub(crate) fn list_layers(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut stmt = self.conn.prepare("SELECT table_name FROM gpkg_contents")?;

        let layers: Result<Vec<String>, rusqlite::Error> =
            stmt.query_map([], |row| row.get(0))?.collect();

        Ok(layers?)
    }

    pub(crate) fn get_column_specs<T: AsRef<str>>(
        &self,
        table_name: T,
    ) -> Result<Vec<ColumnSpec>, Box<dyn std::error::Error>> {
        let query = format!(
            "SELECT name, type FROM pragma_table_info('{}') WHERE name != 'fid'",
            table_name.as_ref()
        );
        let mut stmt = self.conn.prepare(&query)?;

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

    pub(crate) fn read_layer_data<P: AsRef<Path>>(
        path: P,
        layer_name: Option<&str>,
    ) -> Result<Vec<GpkgDataSource>, Box<dyn std::error::Error>> {
        let gpkg = Self::new(path.as_ref().to_str().unwrap())?;
        let filename = path.as_ref().to_string_lossy().into_owned();

        let layers = if let Some(layer) = layer_name {
            vec![layer.to_string()]
        } else {
            gpkg.list_layers()?
        };

        let mut sources = Vec::new();

        for layer in layers {
            let column_specs = gpkg.get_column_specs(&layer)?;

            // Build column list for SELECT
            let column_names: Vec<String> = column_specs
                .iter()
                .map(|spec| format!("\"{}\"", spec.name))
                .collect();
            let columns_str = column_names.join(", ");

            let query = format!("SELECT {} FROM \"{}\"", columns_str, layer);
            let mut stmt = gpkg.conn.prepare(&query)?;

            let mut data = Vec::new();
            let rows = stmt.query_map([], |row| {
                let mut row_data = Vec::new();
                for (idx, spec) in column_specs.iter().enumerate() {
                    let value = match spec.column_type {
                        ColumnType::Integer => {
                            let val: Option<i64> = row.get(idx)?;
                            match val {
                                Some(v) => rusqlite::types::Value::Integer(v),
                                None => rusqlite::types::Value::Null,
                            }
                        }
                        ColumnType::Double => {
                            let val: Option<f64> = row.get(idx)?;
                            match val {
                                Some(v) => rusqlite::types::Value::Real(v),
                                None => rusqlite::types::Value::Null,
                            }
                        }
                        ColumnType::Varchar => {
                            let val: Option<String> = row.get(idx)?;
                            match val {
                                Some(v) => rusqlite::types::Value::Text(v),
                                None => rusqlite::types::Value::Null,
                            }
                        }
                        ColumnType::Boolean => {
                            let val: Option<bool> = row.get(idx)?;
                            match val {
                                Some(v) => rusqlite::types::Value::Integer(if v { 1 } else { 0 }),
                                None => rusqlite::types::Value::Null,
                            }
                        }
                        ColumnType::Geometry => {
                            let val: Option<Vec<u8>> = row.get(idx)?;
                            match val {
                                Some(v) => rusqlite::types::Value::Blob(v),
                                None => rusqlite::types::Value::Null,
                            }
                        }
                    };
                    row_data.push(value);
                }
                Ok(row_data)
            })?;

            for row in rows {
                data.push(row?);
            }

            sources.push(GpkgDataSource {
                filename: filename.clone(),
                layer_name: layer.clone(),
                column_specs,
                data,
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
    fn test_list_layers() -> Result<(), Box<dyn std::error::Error>> {
        let gpkg = super::Gpkg::new("./test/data/multi_layers.gpkg")?;
        let layers = gpkg.list_layers()?;
        assert_eq!(&layers, &["points2_point", "points_point"]);

        Ok(())
    }

    #[test]
    fn test_get_column_specs() -> Result<(), Box<dyn std::error::Error>> {
        let gpkg = super::Gpkg::new("./test/data/points.gpkg")?;
        let layers = gpkg.get_column_specs("points")?;

        assert_eq!(layers.len(), 3);
        assert_eq!(&layers[0].name, "geom");
        assert_eq!(layers[0].column_type, ColumnType::Geometry);
        assert_eq!(&layers[1].name, "val1");
        assert_eq!(layers[1].column_type, ColumnType::Double);
        assert_eq!(&layers[2].name, "val2");
        assert_eq!(layers[2].column_type, ColumnType::Varchar);

        Ok(())
    }
}
