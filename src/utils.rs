use std::path::Path;
use crate::types::ColumnSpec;

// glob() doesn't handle tilda, so I have to.
pub fn expand_tilde(path: &str) -> String {
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

pub fn is_geojson<P: AsRef<Path>>(path: P) -> bool {
    match path.as_ref().extension() {
        Some(ext) => ext.to_string_lossy() == "geojson",
        None => false,
    }
}

pub fn is_gpkg<P: AsRef<Path>>(path: P) -> bool {
    match path.as_ref().extension() {
        Some(ext) => ext.to_string_lossy() == "gpkg",
        None => false,
    }
}

pub fn validate_schema(
    existing_specs: &[ColumnSpec],
    new_specs: &[ColumnSpec],
    file_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Check if the number of columns matches
    if existing_specs.len() != new_specs.len() {
        return Err(format!(
            "Schema mismatch in {}: expected {} columns, found {}",
            file_path.to_string_lossy().replace('\\', "/"),
            existing_specs.len(),
            new_specs.len()
        )
        .into());
    }

    // Since both are sorted by name, we can compare directly
    for (i, (existing, local)) in existing_specs.iter().zip(new_specs.iter()).enumerate() {
        if existing.name != local.name {
            return Err(format!(
                "Schema mismatch in {}: column {} has name '{}', expected '{}'",
                file_path.to_string_lossy().replace('\\', "/"),
                i,
                local.name,
                existing.name
            )
            .into());
        }

        if existing.column_type != local.column_type {
            return Err(format!(
                "Schema mismatch in {}: column '{}' has type {:?}, expected {:?}",
                file_path.to_string_lossy().replace('\\', "/"),
                local.name,
                local.column_type,
                existing.column_type
            )
            .into());
        }
    }
    
    Ok(())
}
