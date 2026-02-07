use std::path::Path;

#[derive(Clone, Copy)]
pub(crate) struct InferredEncoding {
    pub(crate) encoding: ::shapefile::dbase::encoding::EncodingRs,
    pub(crate) name: &'static str,
}

// Currently, dbase-rs doesn't parse .cpg file, so let's do it ourselves...
pub(crate) fn infer_encoding_from_cpg(cpg_path: &Path) -> Option<InferredEncoding> {
    if !cpg_path.exists() {
        return None;
    }

    let label = std::fs::read_to_string(cpg_path).ok()?;
    let upper = label
        .trim()
        .trim_start_matches('\u{feff}')
        .to_ascii_uppercase();

    // I searched to the ends of the internet, but I couldn’t find the specification of the CPG file.
    // The following list is just a best guess based on the search results on GitHub.
    let enc = match upper.as_str() {
        "UTF-8" | "65001" => ::shapefile::dbase::encoding_rs::UTF_8,
        "CP932" | "SHIFT_JIS" | "SJIS" => ::shapefile::dbase::encoding_rs::SHIFT_JIS,
        "CP936" | "GBK" => ::shapefile::dbase::encoding_rs::GBK,
        "CP949" | "EUC-KR" => ::shapefile::dbase::encoding_rs::EUC_KR,
        "BIG5" | "BIG-5" => ::shapefile::dbase::encoding_rs::BIG5,
        // For consistency with https://github.com/tmontaigu/dbase-rs/blob/master/src/encoding/encoding_rs.rs
        // I couldn't find almost no actual .cpg files on GitHub.
        "CP866" => ::shapefile::dbase::encoding_rs::IBM866,
        "CP874" => ::shapefile::dbase::encoding_rs::WINDOWS_874,
        "CP1255" => ::shapefile::dbase::encoding_rs::WINDOWS_1255,
        "CP1256" => ::shapefile::dbase::encoding_rs::WINDOWS_1256,
        "CP1250" => ::shapefile::dbase::encoding_rs::WINDOWS_1250,
        "CP1251" => ::shapefile::dbase::encoding_rs::WINDOWS_1251,
        "CP1254" => ::shapefile::dbase::encoding_rs::WINDOWS_1254,
        "CP1253" => ::shapefile::dbase::encoding_rs::WINDOWS_1253,
        _ => return None,
    };

    Some(InferredEncoding {
        encoding: ::shapefile::dbase::encoding::EncodingRs::from(enc),
        name: enc.name(),
    })
}
