# `ST_Read_Multi`

This extension is to import multiple files e.g. `ST_Read_Multi('path/to/*.geojson')`.

> [!WARNING]
> This is just a temporary, poor man's solution, until `ST_Read` officially supports multiple
> file inputs (cf. [duckdb/duckdb-spatial#191](https://github.com/duckdb/duckdb-spatial/issues/191#issuecomment-2935130507)).
> You should use this only when you have no choice but to get all the things done in DuckDB.
> Usually, you should use `gdal vector concat` (or `ogrmerge`) to merge the files before
> importing the data into DuckDB.

## Limitations

- Only GeoJSON, GeoPackages, and Shapefiles are supported.
- `ST_Read_Multi` is highly inefficient compared to `ST_Read`; this eagerly reads
  all the data and doesn't support pushdown, spatial index, etc.
- The returned geometry column is actually in WKB, but the type is `BLOB`, not
  `GEOMETRY`. This is because DuckDB doesn't allow extensions to use another
  extension's type. You need to explicitly convert it by `ST_GeomFromWkb`.
- Metadata like bbox or CRS are just ignored.

## Usages

Install the extension from community repository first.

```sql
INSTALL st_read_multi FROM community;

LOAD st_read_multi;
```

### GeoJSON

```sql
SELECT * REPLACE (ST_GeomFromWkb(geometry) as geometry)
FROM ST_Read_Multi('test/data/*.geojson');
```

```
┌─────────────────┬────────┬─────────┬───────────────────────────┐
│    geometry     │  val1  │  val2   │         .filename         │
│    geometry     │ double │ varchar │          varchar          │
├─────────────────┼────────┼─────────┼───────────────────────────┤
│ POINT (1 2)     │    1.0 │ a       │ test/data/points.geojson  │
│ POINT (10 20)   │    2.0 │ b       │ test/data/points.geojson  │
│ POINT (100 200) │    5.0 │ c       │ test/data/points2.geojson │
│ POINT (111 222) │    6.0 │ d       │ test/data/points2.geojson │
└─────────────────┴────────┴─────────┴───────────────────────────┘
```

### GeoPackage

```sql
-- load all layers
SELECT * REPLACE (ST_GeomFromWkb(geom) as geom)
FROM ST_Read_Multi('test/data/*.gpkg');
```

```
┌─────────────────┬───────┬─────────┬─────────────────────────────┬───────────────┐
│      geom       │ val1  │  val2   │          .filename          │    .layer     │
│    geometry     │ int32 │ varchar │           varchar           │    varchar    │
├─────────────────┼───────┼─────────┼─────────────────────────────┼───────────────┤
│ POINT (100 200) │     5 │ c       │ test/data/multi_layers.gpkg │ points2_point │
│ POINT (111 222) │     6 │ d       │ test/data/multi_layers.gpkg │ points2_point │
│ POINT (1 2)     │     1 │ a       │ test/data/multi_layers.gpkg │ points_point  │
│ POINT (10 20)   │     2 │ b       │ test/data/multi_layers.gpkg │ points_point  │
│ POINT (1 2)     │     1 │ a       │ test/data/points.gpkg       │ points        │
│ POINT (10 20)   │     2 │ b       │ test/data/points.gpkg       │ points        │
│ POINT (100 200) │     5 │ c       │ test/data/points2.gpkg      │ points        │
│ POINT (111 222) │     6 │ d       │ test/data/points2.gpkg      │ points        │
└─────────────────┴───────┴─────────┴─────────────────────────────┴───────────────┘
```

```sql
-- load specific layers
SELECT * REPLACE (ST_GeomFromWkb(geom) as geom)
FROM ST_Read_Multi('test/data/*.gpkg', layer='points');
```

```
[WARN] No such layer 'points' in test/data/multi_layers.gpkg
┌─────────────────┬───────┬─────────┬────────────────────────┬─────────┐
│      geom       │ val1  │  val2   │       .filename        │ .layer  │
│    geometry     │ int32 │ varchar │        varchar         │ varchar │
├─────────────────┼───────┼─────────┼────────────────────────┼─────────┤
│ POINT (1 2)     │     1 │ a       │ test/data/points.gpkg  │ points  │
│ POINT (10 20)   │     2 │ b       │ test/data/points.gpkg  │ points  │
│ POINT (100 200) │     5 │ c       │ test/data/points2.gpkg │ points  │
│ POINT (111 222) │     6 │ d       │ test/data/points2.gpkg │ points  │
└─────────────────┴───────┴─────────┴────────────────────────┴─────────┘
```

### Shapefile

```sql
SELECT * REPLACE (ST_GeomFromWkb(geometry) as geometry)
FROM ST_Read_Multi('test/data/shapefile_utf8/*.shp');
```

```
┌─────────────────┬────────┬─────────┬──────────────────────────────────────┐
│    geometry     │ 属性1  │  属性2  │              .filename               │
│    geometry     │ double │ varchar │               varchar                │
├─────────────────┼────────┼─────────┼──────────────────────────────────────┤
│ POINT (1 2)     │    1.0 │ 値a     │ test\data\shapefile_utf8\points.shp  │
│ POINT (10 20)   │    1.0 │ 値b     │ test\data\shapefile_utf8\points.shp  │
│ POINT (100 200) │    5.0 │ 値c     │ test\data\shapefile_utf8\points2.shp │
│ POINT (111 222) │    6.0 │ 値d     │ test\data\shapefile_utf8\points2.shp │
└─────────────────┴────────┴─────────┴──────────────────────────────────────┘
```

Notes:

- Input should be specified as `*.shp` (not `*.dbf`).
- Attribute encoding is read from DBF LDID when available.
- If LDID is missing, this extension also tries to infer encoding from a sidecar `.cpg` file.
- You can explicitly specify an encoding label with `encoding='...'` (parsed with the same label set as `.cpg`).
- Compared to `duckdb-spatial`'s current `ST_Read` behavior, this is useful when you need to read non-UTF-8 Shapefiles (e.g. CP932/Shift_JIS DBF attributes).
