-- Test GeoPackage where the geometry column is declared as BLOB in the SQLite
-- schema but is registered in gpkg_geometry_columns.
-- This simulates producers like plateau (CityGML) that emit BLOB-typed geom cols.

CREATE TABLE gpkg_spatial_ref_sys (
  srs_name TEXT NOT NULL,
  srs_id INTEGER NOT NULL PRIMARY KEY,
  organization TEXT NOT NULL,
  organization_coordsys_id INTEGER NOT NULL,
  definition TEXT NOT NULL,
  description TEXT
);
INSERT INTO gpkg_spatial_ref_sys VALUES
  ('Undefined cartesian SRS', -1, 'NONE', -1, 'undefined', ''),
  ('Undefined geographic SRS', 0, 'NONE', 0, 'undefined', ''),
  ('WGS 84 geographic 2D', 4326, 'EPSG', 4326, 'GEOGCS["WGS 84"]', '');

CREATE TABLE gpkg_contents (
  table_name TEXT NOT NULL PRIMARY KEY,
  data_type TEXT NOT NULL,
  identifier TEXT,
  description TEXT DEFAULT '',
  last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
  min_x REAL, min_y REAL, max_x REAL, max_y REAL,
  srs_id INTEGER REFERENCES gpkg_spatial_ref_sys(srs_id)
);
INSERT INTO gpkg_contents VALUES
  ('points', 'features', 'points', '', '2025-01-01T00:00:00Z', 1.0, 2.0, 10.0, 20.0, 4326);

-- geometry column declared as BLOB (not GEOMETRY)
CREATE TABLE "points" (
  "fid" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "geom" BLOB,
  "val1" MEDIUMINT,
  "val2" TEXT
);

CREATE TABLE gpkg_geometry_columns (
  table_name TEXT NOT NULL,
  column_name TEXT NOT NULL,
  geometry_type_name TEXT NOT NULL,
  srs_id INTEGER NOT NULL,
  z TINYINT NOT NULL,
  m TINYINT NOT NULL,
  CONSTRAINT pk_geom_cols PRIMARY KEY (table_name, column_name)
);
-- Register geom despite being declared as BLOB in the table DDL
INSERT INTO gpkg_geometry_columns VALUES ('points', 'geom', 'POINT', 4326, 0, 0);

-- GPKG blobs for Point(1.0, 2.0), Point(10.0, 20.0), Point(30.0, 40.0)
-- Layout: [GPKG header 8B] [WKB]
--   Header: 4750 00 01 E6100000 (magic 'GP', version 0, flags=0x01 LE/no-envelope, srs_id=4326)
--   WKB:    01 01000000 <x f64 LE> <y f64 LE>
-- Note: avoid || for blob concatenation in SQLite (coerces to TEXT); use full literals.
INSERT INTO "points" ("geom", "val1", "val2") VALUES
  -- Point(1.0, 2.0)
  (x'47500001E61000000101000000000000000000F03F0000000000000040', 1, 'a'),
  -- Point(10.0, 20.0)
  (x'47500001E6100000010100000000000000000024400000000000003440', 2, 'b'),
  -- Point(30.0, 40.0)
  (x'47500001E610000001010000000000000000003E400000000000004440', 3, 'c');
