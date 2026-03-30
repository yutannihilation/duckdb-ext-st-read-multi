use std::path::Path;

use ::shapefile::dbase::encoding::DynEncoding;

pub(crate) fn infer_encoding_from_cpg(cpg_path: &Path) -> Option<DynEncoding> {
    let label = std::fs::read_to_string(cpg_path).ok()?;
    DynEncoding::from_name(&label)
}
