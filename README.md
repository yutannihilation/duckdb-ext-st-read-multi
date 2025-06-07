# `ST_Read_Multi`

This extension is to import multiple files e.g. `ST_Read_Multi('path/to/*.geojson')`.

> [!WARNING]
> This is just a temporary, poor man's solution, until `ST_Read` officially supports multiple
> file inputs (cf. [duckdb/duckdb-spatial#191](https://github.com/duckdb/duckdb-spatial/issues/191#issuecomment-2935130507)).
> You should use this only when you have no choice but to get all the things done in DuckDB.
> Usually, you should use `gdal vector concat` (or `ogrmerge`) to merge the files before
> importing the data into DuckDB.

## Limitations

- `ST_Read_Multi` supports only a few numbers of file formats compared to `ST_Read`.
- `ST_Read_Multi` is highly inefficient compared to `ST_Read`; this eagerly reads
  all the data and doesn't support pushdown, spatial index, etc.
- `ST_Read_Multi` returns the geometry column as WKB, but the type is `BLOB`, not
  `GEOMETRY`. This is because DuckDB doesn't allow extensions to use another
  extension's type. You need to explicitly convert it by `ST_GeomFromWkb`.

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
│    geometry     │  val1  │  val2   │         filename          │
│    geometry     │ double │ varchar │          varchar          │
├─────────────────┼────────┼─────────┼───────────────────────────┤
│ POINT (1 2)     │    1.0 │ a       │ test/data/points.geojson  │
│ POINT (10 20)   │    2.0 │ b       │ test/data/points.geojson  │
│ POINT (100 200) │    5.0 │ c       │ test/data/points2.geojson │
│ POINT (111 222) │    6.0 │ d       │ test/data/points2.geojson │
└─────────────────┴────────┴─────────┴───────────────────────────┘
```

### GeoPackage

Not Yet!

```sql
-- load all layers
SELECT * REPLACE (ST_GeomFromWkb(geometry) as geometry)
FROM ST_Read_Multi('test/data/points*.gpkg');
```

```
┌─────────────────┬───────┬─────────┬────────────────────────┐
│    geometry     │ val1  │  val2   │        filename        │
│    geometry     │ int32 │ varchar │        varchar         │
├─────────────────┼───────┼─────────┼────────────────────────┤
│ POINT (1 2)     │     1 │ a       │ test/data/points.gpkg  │
│ POINT (10 20)   │     2 │ b       │ test/data/points.gpkg  │
│ POINT (100 200) │     5 │ c       │ test/data/points2.gpkg │
│ POINT (111 222) │     6 │ d       │ test/data/points2.gpkg │
└─────────────────┴───────┴─────────┴────────────────────────┘
```

```
-- load specific layers
SELECT * REPLACE (ST_GeomFromWkb(geometry) as geometry)
FROM ST_Read_Multi('test/data/*.gpkg', layer='layer_1');
```
