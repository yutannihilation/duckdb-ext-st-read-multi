use geojson::Feature;

pub struct WkbConverter {
    buffer: Vec<u8>,
}

impl WkbConverter {
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    pub fn convert(&mut self, feature: &Feature) -> Result<&[u8], Box<dyn std::error::Error>> {
        self.buffer.clear();
        match &feature.geometry {
            Some(geojson_geom) => {
                let geometry: geo_types::Geometry = geojson_geom.try_into()?;
                wkb::writer::write_geometry(&mut self.buffer, &geometry, &Default::default())
                    .unwrap();
            }
            None => panic!("Geometry should exist!"),
        }
        Ok(&self.buffer)
    }
}
