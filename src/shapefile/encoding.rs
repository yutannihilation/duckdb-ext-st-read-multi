use std::path::Path;

#[derive(Clone, Copy)]
pub(crate) struct InferredEncoding {
    pub(crate) encoding: ::shapefile::dbase::encoding::EncodingRs,
    pub(crate) name: &'static str,
}

pub(crate) fn parse_encoding_label(label: &str) -> Option<InferredEncoding> {
    let upper = label
        .trim()
        .trim_start_matches('\u{feff}')
        .to_ascii_uppercase();

    // It seems ISO-8859-* encodings are stored as 8859* or 8859-*
    // - https://github.com/OSGeo/gdal/blob/12582d42366b101f75079dc832e34e4144cce62f/ogr/ogrsf_frmts/shape/ogrshapelayer.cpp#L517C38-L523
    // - https://github.com/qgis/QGIS/blob/master/tests/testdata/shapefile/iso-8859-1.cpg
    if let Some(no) = upper
        .strip_prefix("8859-")
        .or_else(|| upper.strip_prefix("8859"))
    {
        let enc = match no {
            "1" => ::shapefile::dbase::encoding_rs::WINDOWS_1252,
            "2" => ::shapefile::dbase::encoding_rs::ISO_8859_2,
            "3" => ::shapefile::dbase::encoding_rs::ISO_8859_3,
            "4" => ::shapefile::dbase::encoding_rs::ISO_8859_4,
            "5" => ::shapefile::dbase::encoding_rs::ISO_8859_5,
            "6" => ::shapefile::dbase::encoding_rs::ISO_8859_6,
            "7" => ::shapefile::dbase::encoding_rs::ISO_8859_7,
            "8" => ::shapefile::dbase::encoding_rs::ISO_8859_8,
            "9" => ::shapefile::dbase::encoding_rs::WINDOWS_1254,
            "10" => ::shapefile::dbase::encoding_rs::ISO_8859_10,
            "13" => ::shapefile::dbase::encoding_rs::ISO_8859_13,
            "14" => ::shapefile::dbase::encoding_rs::ISO_8859_14,
            "15" => ::shapefile::dbase::encoding_rs::ISO_8859_15,
            "16" => ::shapefile::dbase::encoding_rs::ISO_8859_16,
            _ => return None,
        };
        return Some(InferredEncoding {
            encoding: ::shapefile::dbase::encoding::EncodingRs::from(enc),
            name: enc.name(),
        });
    }

    // I searched to the ends of the internet, but I couldn't find the specification of the CPG file.
    // The following list is just a best guess based on the search results on GitHub.
    let enc = match upper.as_str() {
        "UTF-8" | "65001" => ::shapefile::dbase::encoding_rs::UTF_8,
        "932" | "CP932" | "SHIFT_JIS" | "SJIS" => ::shapefile::dbase::encoding_rs::SHIFT_JIS,
        "936" | "CP936" | "GBK" => ::shapefile::dbase::encoding_rs::GBK,
        "949" | "CP949" | "EUC-KR" => ::shapefile::dbase::encoding_rs::EUC_KR,
        "BIG5" | "BIG-5" => ::shapefile::dbase::encoding_rs::BIG5,
        "latin1" => ::shapefile::dbase::encoding_rs::WINDOWS_1252, // Windows-1252 is a superset of latin1
        // For consistency with https://github.com/tmontaigu/dbase-rs/blob/master/src/encoding/encoding_rs.rs
        // I found almost no actual .cpg files on GitHub.
        "866" | "CP866" => ::shapefile::dbase::encoding_rs::IBM866,
        "874" | "CP874" => ::shapefile::dbase::encoding_rs::WINDOWS_874,
        "1255" | "CP1255" => ::shapefile::dbase::encoding_rs::WINDOWS_1255,
        "1256" | "CP1256" => ::shapefile::dbase::encoding_rs::WINDOWS_1256,
        "1250" | "CP1250" => ::shapefile::dbase::encoding_rs::WINDOWS_1250,
        "1251" | "CP1251" => ::shapefile::dbase::encoding_rs::WINDOWS_1251,
        "1252" | "CP1252" => ::shapefile::dbase::encoding_rs::WINDOWS_1252,
        "1254" | "CP1254" => ::shapefile::dbase::encoding_rs::WINDOWS_1254,
        "1253" | "CP1253" => ::shapefile::dbase::encoding_rs::WINDOWS_1253,
        _ => return None,
    };

    Some(InferredEncoding {
        encoding: ::shapefile::dbase::encoding::EncodingRs::from(enc),
        name: enc.name(),
    })
}

// Currently, dbase-rs doesn't parse .cpg file, so let's do it ourselves...
pub(crate) fn infer_encoding_from_cpg(cpg_path: &Path) -> Option<InferredEncoding> {
    if !cpg_path.exists() {
        return None;
    }

    let label = std::fs::read_to_string(cpg_path).ok()?;
    parse_encoding_label(&label)
}
