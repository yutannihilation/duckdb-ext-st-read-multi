use crate::types::{ColumnSpec, ColumnType};
use crate::VECTOR_SIZE;

use rusqlite::{Connection, OpenFlags, Result, Row};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

#[repr(C)]
pub struct GpkgDataSource {
    pub layer_name: String,
    pub column_specs: Vec<ColumnSpec>,
    pub sql: String,
    pub gpkg: Gpkg,
}

#[derive(Clone)]
pub struct Gpkg {
    pub conn: Arc<Mutex<GpkgConnection>>,
    pub path: String,
    pub layers: Vec<String>,
}

pub struct GpkgConnection {
    // TODO: probably, this should contain Statement instaed of Connection.
    // But, it seems it's not possible due to the lifetime requirement.
    pub conn: Connection,
}

impl GpkgConnection {
    fn new(conn: Connection) -> Self {
        Self { conn }
    }

    // Returns the number of rows fetched.
    pub fn fetch_rows<F>(&mut self, sql: &str, offset: usize, mut f: F) -> Result<usize>
    where
        F: FnMut(&Row<'_>, usize) -> Result<()>,
    {
        let mut row_idx: usize = 0;

        let mut stmt = self.conn.prepare_cached(sql)?;
        let result = stmt
            .query_map([offset as isize], |row| {
                let result = f(row, row_idx);
                row_idx += 1;
                result
            })?
            // result needs to be consumed, otherwise, the closure is not executed.
            .collect::<Result<Vec<()>>>()?;

        Ok(result.len())
    }
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
            let layers = if !layers.contains(&layer_name) {
                eprintln!("[WARN] No such layer '{layer_name}' in {path}",);
                vec![]
            } else {
                vec![layer_name]
            };

            Ok(Self {
                conn: Arc::new(Mutex::new(GpkgConnection::new(conn))),
                path,
                layers,
            })
        } else {
            // If layer is not specified, return all the layers
            Ok(Self {
                conn: Arc::new(Mutex::new(GpkgConnection::new(conn))),
                path,
                layers,
            })
        }
    }

    /// Get the primary key column name for a table.
    fn get_pk_column<T: AsRef<str>>(
        conn: &Connection,
        table_name: T,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let query = format!(
            "SELECT name FROM pragma_table_info('{}') WHERE pk = 1",
            table_name.as_ref()
        );
        let pk: String = conn.query_row(&query, [], |row| row.get(0))?;
        Ok(pk)
    }

    pub(crate) fn get_column_specs<T: AsRef<str>>(
        &self,
        table_name: T,
    ) -> Result<Vec<ColumnSpec>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().unwrap();

        let pk_column = Self::get_pk_column(&conn.conn, table_name.as_ref())?;
        let query = format!(
            "SELECT name, type FROM pragma_table_info('{}') WHERE name != '{}'",
            table_name.as_ref(),
            pk_column
        );
        let mut stmt = conn.conn.prepare(&query)?;

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
                "DATE" => ColumnType::Date,
                "DATETIME" => ColumnType::Timestamp,
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

            let pk_column = {
                let conn = self.conn.lock().unwrap();
                Self::get_pk_column(&conn.conn, layer)?
            };

            let sql = format!(
                r#"SELECT {} FROM "{}" ORDER BY "{}" LIMIT {VECTOR_SIZE} OFFSET ?"#,
                column_specs
                    .iter()
                    .map(|s| format!(r#""{}""#, s.name))
                    .collect::<Vec<String>>()
                    .join(","),
                layer,
                pk_column,
            );

            sources.push(GpkgDataSource {
                layer_name: layer.to_string(),
                column_specs,
                sql,
                gpkg: self.clone(),
            });
        }

        Ok(sources)
    }
}

/// Parse "YYYY-MM-DD" to days since Unix epoch (1970-01-01).
pub(crate) fn parse_date_to_unix_days(s: &str) -> i32 {
    let b = s.as_bytes();
    let year = parse_digits(b, 0, 4) as i32;
    let month = parse_digits(b, 5, 2) as u32;
    let day = parse_digits(b, 8, 2) as u32;
    days_from_civil(year, month, day)
}

/// Parse "YYYY-MM-DDTHH:MM:SS", "YYYY-MM-DDTHH:MM:SS.SSSZ", or "YYYY-MM-DD HH:MM:SS" variants
/// to microseconds since Unix epoch.
pub(crate) fn parse_datetime_to_unix_micros(s: &str) -> i64 {
    let b = s.as_bytes();
    let days = parse_date_to_unix_days(s) as i64;
    // byte 10 is 'T' or ' '
    let hour = parse_digits(b, 11, 2) as i64;
    let min = parse_digits(b, 14, 2) as i64;
    let sec = parse_digits(b, 17, 2) as i64;

    let mut micros = 0i64;
    // Optional fractional seconds: ".SSS"
    if b.len() > 19 && b[19] == b'.' {
        let frac_start = 20;
        let frac_end = b[frac_start..]
            .iter()
            .position(|&c| !c.is_ascii_digit())
            .map_or(b.len(), |p| frac_start + p);
        let frac_len = frac_end - frac_start;
        let frac_val = parse_digits(b, frac_start, frac_len) as i64;
        // Scale to microseconds (6 digits)
        micros = if frac_len <= 6 {
            frac_val * 10i64.pow(6 - frac_len as u32)
        } else {
            frac_val / 10i64.pow(frac_len as u32 - 6)
        };
    }

    days * 86_400_000_000 + hour * 3_600_000_000 + min * 60_000_000 + sec * 1_000_000 + micros
}

fn parse_digits(b: &[u8], offset: usize, len: usize) -> i64 {
    let mut val = 0i64;
    for &c in &b[offset..offset + len] {
        val = val * 10 + (c - b'0') as i64;
    }
    val
}

/// Convert a civil date to days since Unix epoch.
/// Algorithm from https://howardhinnant.github.io/date_algorithms.html
fn days_from_civil(year: i32, month: u32, day: u32) -> i32 {
    let y = if month <= 2 { year - 1 } else { year } as i32;
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let m = month;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146097 + doe as i32 - 719468) as i32
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

    #[test]
    fn test_get_column_specs_with_date() -> Result<(), Box<dyn std::error::Error>> {
        let gpkg = super::Gpkg::new("./test/data/dates.gpkg", None)?;
        let specs = gpkg.get_column_specs("dates")?;

        assert_eq!(specs.len(), 4);
        assert_eq!(&specs[0].name, "geom");
        assert_eq!(specs[0].column_type, ColumnType::Geometry);
        assert_eq!(&specs[1].name, "name");
        assert_eq!(specs[1].column_type, ColumnType::Varchar);
        assert_eq!(&specs[2].name, "event_date");
        assert_eq!(specs[2].column_type, ColumnType::Date);
        assert_eq!(&specs[3].name, "event_datetime");
        assert_eq!(specs[3].column_type, ColumnType::Timestamp);

        Ok(())
    }

    #[test]
    fn test_parse_date_to_unix_days() {
        // 1970-01-01 = day 0
        assert_eq!(super::parse_date_to_unix_days("1970-01-01"), 0);
        // 1970-01-02 = day 1
        assert_eq!(super::parse_date_to_unix_days("1970-01-02"), 1);
        // 1969-12-31 = day -1
        assert_eq!(super::parse_date_to_unix_days("1969-12-31"), -1);
        // 2024-01-15 = 19737
        assert_eq!(super::parse_date_to_unix_days("2024-01-15"), 19737);
    }

    #[test]
    fn test_parse_datetime_to_unix_micros() {
        // 1970-01-01T00:00:00Z = 0
        assert_eq!(
            super::parse_datetime_to_unix_micros("1970-01-01T00:00:00Z"),
            0
        );
        // 1970-01-01T00:00:01Z = 1_000_000
        assert_eq!(
            super::parse_datetime_to_unix_micros("1970-01-01T00:00:01Z"),
            1_000_000
        );
        // with milliseconds
        assert_eq!(
            super::parse_datetime_to_unix_micros("1970-01-01T00:00:00.500Z"),
            500_000
        );
        // space separator (common in SQLite)
        assert_eq!(
            super::parse_datetime_to_unix_micros("2024-01-15 12:30:45Z"),
            19737 * 86_400_000_000i64 + 12 * 3_600_000_000 + 30 * 60_000_000 + 45 * 1_000_000
        );
    }
}
