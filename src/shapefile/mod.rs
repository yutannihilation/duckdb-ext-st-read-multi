mod datasource;
mod encoding;

pub use datasource::ShapefileDataSource;
pub(crate) use encoding::parse_encoding_label;

#[cfg(test)]
mod tests;
