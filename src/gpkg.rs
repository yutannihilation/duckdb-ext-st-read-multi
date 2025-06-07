use crate::types::{ColumnSpec, ColumnType};
use rusqlite::{Connection, Result};

pub struct Gpkg {
    conn: Connection,
}

impl Gpkg {
    pub(crate) fn new<P: AsRef<str>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let conn = Connection::open(path.as_ref())?;
        Ok(Self { conn })
    }

    pub(crate) fn list_layers(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut stmt = self
            .conn
            .prepare("SELECT table_name FROM gpkg_contents")?;
        
        let layers: Result<Vec<String>, rusqlite::Error> = stmt
            .query_map([], |row| row.get(0))?
            .collect();
        
        Ok(layers?)
    }

    pub(crate) fn get_column_specs<T: AsRef<str>>(
        &self,
        table_name: T,
    ) -> Result<Vec<ColumnSpec>, Box<dyn std::error::Error>> {
        let query = format!(
            "SELECT name, type FROM pragma_table_info('{}')",
            table_name.as_ref()
        );
        let mut stmt = self.conn.prepare(&query)?;
        
        let column_specs = stmt.query_map([], |row| {
            let name: String = row.get(0)?;
            let column_type_str: String = row.get(1)?;
            
            let column_type = match column_type_str.to_uppercase().as_str() {
                "INTEGER" => ColumnType::Integer,
                "REAL" => ColumnType::Double,
                "TEXT" => ColumnType::Varchar,
                "BOOLEAN" => ColumnType::Boolean,
                "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT"
                | "MULTILINESTRING" | "MULTIPOLYGON" | "GEOMETRYCOLLECTION" => {
                    ColumnType::Geometry
                }
                _ => return Err(rusqlite::Error::InvalidColumnType(
                    1, 
                    format!("Unexpected type {}", column_type_str), 
                    rusqlite::types::Type::Text
                )),
            };
            
            Ok(ColumnSpec { name, column_type })
        })?;
        
        let result: Result<Vec<ColumnSpec>, rusqlite::Error> = column_specs.collect();
        Ok(result?)
    }
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

        assert_eq!(layers.len(), 4);
        assert_eq!(&layers[0].name, "fid");
        assert_eq!(layers[0].column_type, ColumnType::Integer);
        assert_eq!(&layers[1].name, "geom");
        assert_eq!(layers[1].column_type, ColumnType::Geometry);
        assert_eq!(&layers[2].name, "val1");
        assert_eq!(layers[2].column_type, ColumnType::Double);
        assert_eq!(&layers[3].name, "val2");
        assert_eq!(layers[3].column_type, ColumnType::Varchar);

        Ok(())
    }
}