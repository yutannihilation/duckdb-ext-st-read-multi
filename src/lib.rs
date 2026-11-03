mod geojson;
mod gpkg;
mod shapefile;
mod types;
mod utils;

use std::{path::PathBuf, sync::Mutex};

use duckdb_loadable_macros::duckdb_entrypoint_c_api_v2;
use duckdb_neo::{
    DuckDBType, Result,
    bind_arguments::BindArgument,
    connection::Context,
    data_chunk::{DataChunk, DataChunkRef},
    error::Error,
    extension::Extension,
    signature::{
        FunctionDocumentationBuilder, FunctionParameter, FunctionParameterKind, SignatureBuilder,
    },
    table_function::{
        BindFunctionHandle, ExecColumnInfo, InitColumnData, TableFunctionBuilder,
        TableFunctionCallbacks,
    },
    types::{BlobValue, DateValue, TimestampValue},
    vector::{Unknown, WritableVectorElement},
};
use glob::glob;

use crate::{
    geojson::{GeoJsonDataSource, WkbConverter},
    gpkg::{Gpkg, GpkgDataSource, gpkg_geometry_to_wkb},
    shapefile::ShapefileDataSource,
    types::{
        Cell, ColumnSpec, ColumnType, Cursor, GeoJsonBindData, GpkgBindData, ShapefileBindData,
        StReadMultiBindData,
    },
    utils::{expand_tilde, is_geojson, is_gpkg, is_shp, validate_schema},
};

/// The number of rows read from a data source per `exec` call. DuckDB's default vector
/// size is 2048; the GeoPackage reader bakes this into its `LIMIT` clause at bind time.
pub(crate) const VECTOR_SIZE: usize = 2048;

const FUNCTION_NAME: &str = "st_read_multi";

const COLUMN_NAME_FILENAME: &str = ".filename";
const COLUMN_NAME_LAYER: &str = ".layer";

/// Positions of the declared parameters in the arguments handed to `bind`.
const ARG_PATH: usize = 0;
const ARG_LAYER: usize = 1;
const ARG_ENCODING: usize = 2;

type BoxResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Convert an error from the readers into a DuckDB error, keeping DuckDB errors as they are.
fn to_duckdb_error(error: Box<dyn std::error::Error>) -> Error {
    match error.downcast::<Error>() {
        Ok(error) => *error,
        Err(error) => Error::invalid_input(error.to_string()),
    }
}

/// Read a constant VARCHAR argument; SQL NULL and an omitted named parameter give `None`.
fn string_argument(arguments: &[BindArgument], index: usize) -> Result<Option<String>> {
    match arguments
        .get(index)
        .and_then(|argument| argument.value.as_ref())
    {
        Some(value) => value.get::<String>(),
        None => Ok(None),
    }
}

struct StReadMulti;

impl TableFunctionCallbacks for StReadMulti {
    type BindData = StReadMultiBindData;
    type GlobalState = Mutex<Cursor>;
    type LocalState = ();

    fn bind(
        &self,
        context: &Context,
        arguments: Vec<BindArgument>,
        bind_handle: BindFunctionHandle<'_>,
    ) -> Result<Self::BindData> {
        let path_pattern = string_argument(&arguments, ARG_PATH)?
            .ok_or_else(|| Error::invalid_input("The path must be a non-NULL constant"))?;
        let layer_option = string_argument(&arguments, ARG_LAYER)?;
        let encoding_option = string_argument(&arguments, ARG_ENCODING)?;

        bind_inner(
            context,
            &bind_handle,
            &path_pattern,
            layer_option,
            encoding_option,
        )
        .map_err(to_duckdb_error)
    }

    fn init_global_state(
        &self,
        _bind_data: &Self::BindData,
        _context: &Context,
        _column_data: InitColumnData<'_>,
    ) -> Result<(Option<Self::GlobalState>, Option<usize>)> {
        // The cursor is advanced sequentially, so the scan runs on one thread.
        Ok((Some(Mutex::new(Cursor::new())), Some(1)))
    }

    fn exec(
        &self,
        bind_data: &Self::BindData,
        global_state: Option<&Self::GlobalState>,
        _local_state: Option<&mut Self::LocalState>,
        context: &Context,
        mut output: DataChunkRef<'_>,
        _column_info: ExecColumnInfo<'_>,
    ) -> Result<()> {
        let cursor = global_state.ok_or_else(|| Error::invalid_input("Missing global state"))?;
        let mut cursor = cursor
            .lock()
            .map_err(|_| Error::invalid_input("Failed to acquire the lock of the cursor"))?;
        let chunk_size = VECTOR_SIZE.min(output.capacity()?);

        exec_inner(bind_data, &mut cursor, context, &mut output, chunk_size)
            .map_err(to_duckdb_error)
    }
}

fn bind_inner(
    context: &Context,
    bind_handle: &BindFunctionHandle<'_>,
    path_pattern: &str,
    layer_option: Option<String>,
    encoding_option: Option<String>,
) -> BoxResult<StReadMultiBindData> {
    let expanded_pattern = expand_tilde(path_pattern);
    let paths: Vec<PathBuf> = glob(&expanded_pattern)?.collect::<std::result::Result<_, _>>()?;

    if paths.is_empty() {
        return Err(format!("'{path_pattern}' doesn't match to any file").into());
    }

    let add_column = |name: &str, column_type: ColumnType| -> BoxResult<()> {
        bind_handle.add_result_column(name, &column_type.logical_type(context)?)?;
        Ok(())
    };

    // ==================== //
    //     GeoJSON          //
    // ==================== //

    if paths.iter().all(is_geojson) {
        if encoding_option.is_some() {
            eprintln!("[WARN] Named parameter 'encoding' is ignored for GeoJSON input");
        }

        let mut sources: Vec<GeoJsonDataSource> = Vec::new();
        let mut column_specs: Option<Vec<ColumnSpec>> = None;

        for path in paths {
            let (mut data_sources, column_specs_local) = GeoJsonDataSource::parse_and_split(&path)?;
            sources.append(&mut data_sources);

            if let Some(existing_specs) = &column_specs {
                // check if the schema matches
                validate_schema(existing_specs, &column_specs_local, &path)?;
            } else {
                // if it's the first file, use the spec as the base.
                let _ = column_specs.insert(column_specs_local);
            }
        }

        let column_specs = column_specs.unwrap();

        add_column("geometry", ColumnType::Geometry)?;
        for spec in column_specs.iter() {
            add_column(&spec.name, spec.column_type)?;
        }

        // filename column to track source file
        add_column(COLUMN_NAME_FILENAME, ColumnType::Varchar)?;

        return Ok(GeoJsonBindData {
            sources,
            column_specs,
        }
        .into());
    }

    // ==================== //
    //     Gpkg             //
    // ==================== //

    if paths.iter().all(is_gpkg) {
        if encoding_option.is_some() {
            eprintln!("[WARN] Named parameter 'encoding' is ignored for GeoPackage input");
        }

        let mut sources: Vec<GpkgDataSource> = Vec::new();
        let mut column_specs: Option<Vec<ColumnSpec>> = None;

        for path in paths {
            let gpkg = Gpkg::new(&path, layer_option.clone())?;

            for source in gpkg.list_data_sources()? {
                if let Some(existing_specs) = &column_specs {
                    // check if the schema matches
                    validate_schema(existing_specs, &source.column_specs, &path)?;
                } else {
                    // if it's the first file, use the spec as the base.
                    let _ = column_specs.insert(source.column_specs.clone());
                }
                sources.push(source);
            }
        }

        let column_specs = column_specs.ok_or("No layers are found")?;

        for spec in column_specs.iter() {
            add_column(&spec.name, spec.column_type)?;
        }

        // filename and layer column to track source
        add_column(COLUMN_NAME_FILENAME, ColumnType::Varchar)?;
        add_column(COLUMN_NAME_LAYER, ColumnType::Varchar)?;

        return Ok(GpkgBindData {
            sources,
            column_specs,
        }
        .into());
    }

    // ==================== //
    //     Shapefile        //
    // ==================== //

    if paths.iter().all(is_shp) {
        let specified_encoding = match encoding_option {
            Some(label) => Some(
                ::shapefile::dbase::encoding::DynEncoding::from_name(&label).ok_or_else(|| {
                    format!("Unknown encoding label in 'encoding' option: {label}")
                })?,
            ),
            None => None,
        };

        let mut sources: Vec<ShapefileDataSource> = Vec::new();
        let mut column_specs: Option<Vec<ColumnSpec>> = None;

        for path in paths {
            let source = ShapefileDataSource::new(&path, specified_encoding.clone())?;
            let column_specs_local = source.column_specs.clone();

            if let Some(existing_specs) = &column_specs {
                validate_schema(existing_specs, &column_specs_local, &path)?;
            } else {
                let _ = column_specs.insert(column_specs_local);
            }

            sources.push(source);
        }

        let column_specs = column_specs.unwrap();

        add_column("geometry", ColumnType::Geometry)?;
        for spec in column_specs.iter() {
            add_column(&spec.name, spec.column_type)?;
        }

        add_column(COLUMN_NAME_FILENAME, ColumnType::Varchar)?;

        return Ok(ShapefileBindData {
            sources,
            column_specs,
        }
        .into());
    }

    Err("All files must have extension '.geojson', '.gpkg', or '.shp'".into())
}

/// Rows of one output chunk, buffered column by column so that each output vector can be
/// written in one pass (DuckDB hands out one mutable vector at a time).
struct ChunkBuffer {
    columns: Vec<Vec<Cell>>,
}

impl ChunkBuffer {
    fn new(n_columns: usize) -> Self {
        Self {
            columns: (0..n_columns).map(|_| Vec::new()).collect(),
        }
    }

    fn push(&mut self, column: usize, cell: Cell) {
        self.columns[column].push(cell);
    }

    /// Write every column to `output`. `types` gives the logical type of each column.
    fn write(
        &self,
        output: &mut DataChunkRef<'_>,
        context: &Context,
        types: impl Iterator<Item = ColumnType>,
    ) -> BoxResult<()> {
        for (index, (cells, column_type)) in self.columns.iter().zip(types).enumerate() {
            write_column(output, context, index, column_type, cells)?;
        }
        Ok(())
    }
}

fn write_column(
    output: &mut DataChunkRef<'_>,
    context: &Context,
    index: usize,
    column_type: ColumnType,
    cells: &[Cell],
) -> BoxResult<()> {
    match column_type {
        ColumnType::Boolean => write_typed::<bool>(output, index, cells, |cell| match cell {
            Cell::Boolean(v) => Some(*v),
            _ => None,
        }),
        ColumnType::Integer => write_typed::<i32>(output, index, cells, |cell| match cell {
            Cell::Integer(v) => Some(*v),
            _ => None,
        }),
        ColumnType::Double => write_typed::<f64>(output, index, cells, |cell| match cell {
            Cell::Double(v) => Some(*v),
            _ => None,
        }),
        ColumnType::Varchar => write_typed::<String>(output, index, cells, |cell| match cell {
            Cell::Varchar(v) => Some(v.as_str()),
            _ => None,
        }),
        ColumnType::Blob => write_typed::<BlobValue>(output, index, cells, |cell| match cell {
            Cell::Blob(v) => Some(v.as_slice()),
            _ => None,
        }),
        ColumnType::Date => write_typed::<DateValue>(output, index, cells, |cell| match cell {
            Cell::Date(v) => Some(DateValue(*v)),
            _ => None,
        }),
        ColumnType::Timestamp => {
            write_typed::<TimestampValue>(output, index, cells, |cell| match cell {
                Cell::Timestamp(v) => Some(TimestampValue(*v)),
                _ => None,
            })
        }
        ColumnType::Geometry => {
            // duckdb-neo has no GEOMETRY vector element, so the WKB is written to a scratch
            // BLOB vector and converted with DuckDB's own BLOB -> GEOMETRY cast.
            let blob_type = BlobValue::logical_type(context)?;
            let mut scratch = DataChunk::create(context, [blob_type], true)?;
            {
                let mut vector = scratch.get_vector_at_mut::<BlobValue>(0)?;
                vector.set_size(cells.len())?;
                let mut writer = vector.writer()?;
                for (row, cell) in cells.iter().enumerate() {
                    let value = match cell {
                        Cell::Blob(v) => Some(v.as_slice()),
                        _ => None,
                    };
                    writer.set(row, value)?;
                }
            }
            let source = scratch.get_vector_at::<BlobValue>(0)?;
            output
                .get_vector_at_mut::<Unknown>(index)?
                .cast_rows(source, context)?;
            Ok(())
        }
    }
}

fn write_typed<T>(
    output: &mut DataChunkRef<'_>,
    index: usize,
    cells: &[Cell],
    to_value: impl for<'c> Fn(&'c Cell) -> Option<T::Write<'c>>,
) -> BoxResult<()>
where
    T: WritableVectorElement,
{
    let mut vector = output.get_vector_at_mut::<T>(index)?;
    vector.set_size(cells.len())?;
    let mut writer = vector.writer()?;
    for (row, cell) in cells.iter().enumerate() {
        writer.set(row, to_value(cell))?;
    }
    Ok(())
}

fn exec_inner(
    bind_data: &StReadMultiBindData,
    cursor: &mut Cursor,
    context: &Context,
    output: &mut DataChunkRef<'_>,
    chunk_size: usize,
) -> BoxResult<()> {
    match bind_data {
        // ==================== //
        //     GeoJSON          //
        // ==================== //
        StReadMultiBindData::GeoJson(bind_data_inner) => {
            // If there's no remaining data source, leave the chunk empty to tell DuckDB it's over.
            if cursor.source_idx >= bind_data_inner.sources.len() {
                return Ok(());
            }

            let n_props = bind_data_inner.column_specs.len();
            let col_geometry = 0;
            let col_filename = n_props + 1;
            let mut buffer = ChunkBuffer::new(n_props + 2);

            let mut wkb_converter = WkbConverter::new();
            let source = &bind_data_inner.sources[cursor.source_idx];

            let range_end = std::cmp::min(cursor.offset + chunk_size, source.features.len());
            let last = range_end >= source.features.len();
            let range = cursor.offset..range_end;

            for f in &source.features[range] {
                buffer.push(col_geometry, Cell::Blob(wkb_converter.convert(f)?.to_vec()));
                buffer.push(col_filename, Cell::Varchar(source.filename.clone()));

                for (prop_idx, spec) in bind_data_inner.column_specs.iter().enumerate() {
                    let val = f.properties.as_ref().and_then(|p| p.get(&spec.name));

                    let cell = match (spec.column_type, val) {
                        (ColumnType::Varchar, Some(serde_json::Value::String(v))) => {
                            Cell::Varchar(v.clone())
                        }
                        (ColumnType::Boolean, Some(serde_json::Value::Bool(v))) => {
                            Cell::Boolean(*v)
                        }
                        (ColumnType::Double, Some(serde_json::Value::Number(v))) => {
                            match v.as_f64() {
                                Some(v) => Cell::Double(v),
                                None => Cell::Null,
                            }
                        }
                        // NULL, missing, or a value of another type than the one inferred
                        _ => Cell::Null,
                    };
                    buffer.push(prop_idx + 1, cell);
                }
            }

            if last {
                cursor.source_idx += 1;
                cursor.offset = 0;
            } else {
                cursor.offset += chunk_size;
            }

            let types = std::iter::once(ColumnType::Geometry)
                .chain(bind_data_inner.column_specs.iter().map(|s| s.column_type))
                .chain(std::iter::once(ColumnType::Varchar));
            buffer.write(output, context, types)?;
            Ok(())
        }

        // ==================== //
        //     Gpkg             //
        // ==================== //
        StReadMultiBindData::Gpkg(bind_data_inner) => {
            // If there's no remaining data source, leave the chunk empty to tell DuckDB it's over.
            if cursor.source_idx >= bind_data_inner.sources.len() {
                return Ok(());
            }

            let n_props = bind_data_inner.column_specs.len();
            let col_filename = n_props;
            let col_layer = n_props + 1;

            // Note: This for loop is a bit tricky. This is necessary to let this function
            // return non-empty result, otherwise DuckDB would assume the query is done.
            for source in &bind_data_inner.sources[cursor.source_idx..] {
                let mut buffer = ChunkBuffer::new(n_props + 2);
                let mut conn = source.gpkg.conn.lock().unwrap();

                let row_count = conn.fetch_rows(&source.sql, cursor.offset, |row, _row_idx| {
                    buffer.push(col_filename, Cell::Varchar(source.gpkg.path.clone()));
                    buffer.push(col_layer, Cell::Varchar(source.layer_name.clone()));

                    for (col_idx, spec) in source.column_specs.iter().enumerate() {
                        let cell = match &spec.column_type {
                            ColumnType::Integer => {
                                let val: Option<i64> = row.get(col_idx)?;
                                val.map_or(Cell::Null, |v| Cell::Integer(v as i32))
                            }
                            ColumnType::Double => {
                                let val: Option<f64> = row.get(col_idx)?;
                                val.map_or(Cell::Null, Cell::Double)
                            }
                            ColumnType::Varchar => {
                                let val: Option<String> = row.get(col_idx)?;
                                val.map_or(Cell::Null, Cell::Varchar)
                            }
                            ColumnType::Boolean => {
                                let val: Option<bool> = row.get(col_idx)?;
                                val.map_or(Cell::Null, Cell::Boolean)
                            }
                            ColumnType::Blob => {
                                let val: Option<Vec<u8>> = row.get(col_idx)?;
                                val.map_or(Cell::Null, Cell::Blob)
                            }
                            ColumnType::Geometry => {
                                let val: Option<Vec<u8>> = row.get(col_idx)?;
                                val.map_or(Cell::Null, |v| {
                                    Cell::Blob(gpkg_geometry_to_wkb(&v).to_vec())
                                })
                            }
                            ColumnType::Date => {
                                let val: Option<String> = row.get(col_idx)?;
                                val.map_or(Cell::Null, |v| {
                                    Cell::Date(gpkg::parse_date_to_unix_days(&v))
                                })
                            }
                            ColumnType::Timestamp => {
                                let val: Option<String> = row.get(col_idx)?;
                                val.map_or(Cell::Null, |v| {
                                    Cell::Timestamp(gpkg::parse_datetime_to_unix_micros(&v))
                                })
                            }
                        };
                        buffer.push(col_idx, cell);
                    }

                    Ok(())
                })?;

                match row_count {
                    // This is a special case. While we want to just return the result,
                    // to DuckDB, 0-row result means it's finished. But, we don't want
                    // to finish as long as there's any remaining data sources.
                    0 => {
                        cursor.source_idx += 1;
                        cursor.offset = 0;

                        if cursor.source_idx < bind_data_inner.sources.len() {
                            continue;
                        }
                    }
                    // return the current result and proceed to the next data source
                    1..VECTOR_SIZE => {
                        cursor.source_idx += 1;
                        cursor.offset = 0;
                    }
                    // return the current result and continue on the current data source
                    VECTOR_SIZE => {
                        cursor.offset += row_count;
                    }
                    _ => unreachable!(),
                }

                let types = source
                    .column_specs
                    .iter()
                    .map(|s| s.column_type)
                    .chain([ColumnType::Varchar, ColumnType::Varchar]);
                buffer.write(output, context, types)?;

                // return result and break this loop.
                return Ok(());
            }

            Ok(())
        }

        // ==================== //
        //     Shapefile        //
        // ==================== //
        StReadMultiBindData::Shapefile(bind_data_inner) => {
            if cursor.source_idx >= bind_data_inner.sources.len() {
                return Ok(());
            }

            let n_props = bind_data_inner.column_specs.len();
            let col_geometry = 0;
            let col_filename = n_props + 1;
            let mut buffer = ChunkBuffer::new(n_props + 2);

            let source = &bind_data_inner.sources[cursor.source_idx];

            let range_end = std::cmp::min(cursor.offset + chunk_size, source.rows.len());
            let last = range_end >= source.rows.len();
            let range = cursor.offset..range_end;

            for row in &source.rows[range] {
                let geometry = match &row.geometry {
                    Some(wkb_data) => Cell::Blob(wkb_data.clone()),
                    None => Cell::Null,
                };
                buffer.push(col_geometry, geometry);
                buffer.push(col_filename, Cell::Varchar(source.filename.clone()));

                for (prop_idx, spec) in bind_data_inner.column_specs.iter().enumerate() {
                    let val = row.record.get(&spec.name);

                    use ::shapefile::dbase::FieldValue;
                    let cell = match (spec.column_type, val) {
                        (ColumnType::Varchar, Some(FieldValue::Character(Some(v)))) => {
                            Cell::Varchar(v.clone())
                        }
                        (ColumnType::Varchar, Some(FieldValue::Memo(v))) => {
                            Cell::Varchar(v.clone())
                        }
                        (ColumnType::Boolean, Some(FieldValue::Logical(Some(v)))) => {
                            Cell::Boolean(*v)
                        }
                        (ColumnType::Integer, Some(FieldValue::Integer(v))) => Cell::Integer(*v),
                        (ColumnType::Double, Some(FieldValue::Numeric(Some(v)))) => {
                            Cell::Double(*v)
                        }
                        (ColumnType::Double, Some(FieldValue::Float(Some(v)))) => {
                            Cell::Double(*v as f64)
                        }
                        (ColumnType::Double, Some(FieldValue::Currency(v)))
                        | (ColumnType::Double, Some(FieldValue::Double(v))) => Cell::Double(*v),
                        (ColumnType::Date, Some(FieldValue::Date(Some(v)))) => {
                            Cell::Date(v.to_unix_days())
                        }
                        (ColumnType::Timestamp, Some(FieldValue::DateTime(v))) => {
                            Cell::Timestamp(v.to_unix_timestamp() * 1_000_000)
                        }
                        _ => Cell::Null,
                    };
                    buffer.push(prop_idx + 1, cell);
                }
            }

            if last {
                cursor.source_idx += 1;
                cursor.offset = 0;
            } else {
                cursor.offset += chunk_size;
            }

            let types = std::iter::once(ColumnType::Geometry)
                .chain(bind_data_inner.column_specs.iter().map(|s| s.column_type))
                .chain(std::iter::once(ColumnType::Varchar));
            buffer.write(output, context, types)?;
            Ok(())
        }
    }
}

pub fn register(extension: &Extension) -> Result<()> {
    TableFunctionBuilder::new(
        FUNCTION_NAME,
        SignatureBuilder::new()
            .param(FunctionParameter::new::<String>("path"))
            .param(
                FunctionParameter::with_default("layer", None::<String>)
                    .kind(FunctionParameterKind::Named),
            )
            .param(
                FunctionParameter::with_default("encoding", None::<String>)
                    .kind(FunctionParameterKind::Named),
            )
            .documentation(
                FunctionDocumentationBuilder::new()
                    .description(
                        "Read all GeoJSON, GeoPackage or Shapefile files matching a glob pattern into one table. \
                         The geometry column is GEOMETRY; a '.filename' column (and '.layer' for GeoPackage) tracks the source.",
                    )
                    .add_example("GeoJSON files", "FROM ST_Read_Multi('path/to/*.geojson')")
                    .add_example(
                        "One GeoPackage layer",
                        "FROM ST_Read_Multi('path/to/*.gpkg', layer := 'points')",
                    )
                    .add_example(
                        "CP932 Shapefiles",
                        "FROM ST_Read_Multi('path/to/*.shp', encoding := 'CP932')",
                    )
                    .add_category("spatial"),
            ),
        StReadMulti,
    )
    .register(extension)
}

#[duckdb_entrypoint_c_api_v2(extension_name = "st_read_multi", api_version = "2.0.0")]
pub fn entrypoint(extension: &Extension) -> Result<()> {
    register(extension)
}
