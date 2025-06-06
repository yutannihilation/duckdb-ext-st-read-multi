# `ST_Read_Multi`

> [!WARNING]
> This is just a temporary, poor man's solution, until `ST_Read` supports multiple
> file inputs (cf. [duckdb/duckdb-spatial#191](https://github.com/duckdb/duckdb-spatial/issues/191#issuecomment-2935130507)).
> You should use this only when you have to get all the things done in DuckDB.
> Usually, you should use `gdal vector concat` (or `ogrmerge`) to merge the files.

This extension is to import multiple files e.g. `ST_Read_Multi('path/to/*.geojson')`.

## Limitations

- `ST_Read_Multi` supports only a few numbers of file formats compared to `ST_Read`.
- `ST_Read_Multi` is highly inefficient compared to `ST_Read`; this eagerly reads
  all the data and doesn't support pushdown, spatial index, etc.
- `ST_Read_Multi` returns the geometry column as WKB, but the type is `BLOB`, not
  `GEOMETRY`. This is because DuckDB doesn't allow extensions to use another
  extension's type. You need to explicitly convert it by `ST_GeomFromWkb`.

## Usages

### GeoJSON

```sql
SELECT ST_GeomFromWkb(geometry),
       val1,
       val2
FROM ST_Read_Multi('test/data/*.geojson');
```

```
┌──────────────────────────┬────────┬─────────┐
│ st_geomfromwkb(geometry) │  val1  │  val2   │
│         geometry         │ double │ varchar │
├──────────────────────────┼────────┼─────────┤
│ POINT (1 2)              │    1.0 │ a       │
│ POINT (10 20)            │    2.0 │ b       │
│ POINT (100 200)          │    5.0 │ c       │
│ POINT (111 222)          │    6.0 │ d       │
└──────────────────────────┴────────┴─────────┘
```

### GeoPackage

Not Yet!
