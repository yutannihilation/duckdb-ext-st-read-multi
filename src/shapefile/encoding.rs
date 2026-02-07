use std::path::Path;

#[derive(Clone, Copy)]
pub(crate) struct InferredEncoding {
    pub(crate) encoding: ::shapefile::dbase::encoding::EncodingRs,
    pub(crate) name: &'static str,
}

pub(crate) fn infer_encoding_from_cpg(cpg_path: &Path) -> Option<InferredEncoding> {
    if !cpg_path.exists() {
        return None;
    }

    let raw = std::fs::read_to_string(cpg_path).ok()?;
    let label = raw.trim().trim_start_matches('\u{feff}');
    if label.is_empty() {
        return None;
    }

    if let Some(enc) = ::shapefile::dbase::encoding_rs::Encoding::for_label(label.as_bytes()) {
        return Some(InferredEncoding {
            encoding: ::shapefile::dbase::encoding::EncodingRs::from(enc),
            name: enc.name(),
        });
    }

    let upper = label.to_ascii_uppercase();
    let enc = match upper.as_str() {
        "65001" | "UTF8" | "UTF-8" => ::shapefile::dbase::encoding_rs::UTF_8,
        "932" | "CP932" | "MS932" | "SHIFT_JIS" | "SHIFT-JIS" | "SJIS" | "WINDOWS-31J" => {
            ::shapefile::dbase::encoding_rs::SHIFT_JIS
        }
        "936" | "CP936" | "GBK" => ::shapefile::dbase::encoding_rs::GBK,
        "949" | "CP949" => ::shapefile::dbase::encoding_rs::EUC_KR,
        "950" | "CP950" | "BIG5" => ::shapefile::dbase::encoding_rs::BIG5,
        _ => return None,
    };

    Some(InferredEncoding {
        encoding: ::shapefile::dbase::encoding::EncodingRs::from(enc),
        name: enc.name(),
    })
}
