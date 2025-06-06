use std::path::Path;

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