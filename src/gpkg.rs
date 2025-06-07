use crate::types::{ColumnSpec, ColumnType};

struct Gpkg(limbo::Connection);

impl Gpkg {
    pub(crate) fn new<P: AsRef<str>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        pollster::block_on(async {
            let db = limbo::Builder::new_local(path.as_ref())
                .build()
                .await
                .unwrap();

            let conn = db.connect()?;

            Ok(Self(conn))
        })
    }

    pub(crate) async fn list_layers(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut rows = self
            .0
            .query("SELECT table_name FROM gpkg_contents", ())
            .await?;

        let mut result: Vec<String> = vec![];
        while let Some(row) = rows.next().await? {
            if let Some(table_name) = row.get_value(0)?.as_text() {
                result.push(table_name.to_string());
            }
        }

        Ok(result)
    }

    pub(crate) async fn get_column_specs<T: AsRef<str>>(
        &self,
        table_name: T,
    ) -> Result<Vec<ColumnSpec>, Box<dyn std::error::Error>> {
        let mut rows = self
            .0
            .query(
                &format!(
                    "SELECT name, type FROM pragma_table_info('{}')",
                    table_name.as_ref()
                ),
                (),
            )
            .await?;

        let mut result: Vec<ColumnSpec> = vec![];
        while let Some(row) = rows.next().await? {
            let nm = row.get_value(0)?;
            let ty = row.get_value(1)?;

            let spec = match (nm.as_text(), ty.as_text()) {
                (Some(name), Some(column_type)) => ColumnSpec {
                    name: name.to_string(),
                    column_type: match column_type.as_str() {
                        "INTEGER" => ColumnType::Integer,
                        "REAL" => ColumnType::Double,
                        "TEXT" => ColumnType::Varchar,
                        "BOOLEAN" => ColumnType::Boolean,
                        "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT"
                        | "MULTILINESTRING" | "MULTIPOLYGON" | "GEOMETRYCOLLECTION" => {
                            ColumnType::Geometry
                        }
                        _ => return Err(format!("Unexpected type {column_type}").into()),
                    },
                },
                _ => return Err("Unexpected result".into()),
            };

            result.push(spec);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use crate::types::ColumnType;

    #[test]
    fn test_list_layers() -> Result<(), Box<dyn std::error::Error>> {
        let gpkg = super::Gpkg::new("./test/data/multi_layers.gpkg")?;
        let layers = pollster::block_on(gpkg.list_layers())?;
        assert_eq!(&layers, &["points2_point", "points_point"]);

        Ok(())
    }

    #[test]
    fn test_get_column_specs() -> Result<(), Box<dyn std::error::Error>> {
        let gpkg = super::Gpkg::new("./test/data/points.gpkg")?;
        let layers = pollster::block_on(gpkg.get_column_specs("points"))?;

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
