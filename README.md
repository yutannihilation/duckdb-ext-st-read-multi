# `ST_Read_Multi`

> [!WARNING]
> This is just a temporary, poor man's tool, until `ST_Read` supports multiple file inputs (cf. [duckdb/duckdb-spatial#191](https://github.com/duckdb/duckdb-spatial/issues/191#issuecomment-2935130507))

This extension is to import multiple files e.g. `ST_Read_Multi('path/to/*.geojson')`.

## Limitations

- `ST_Read_Multi` supports only a few numbers of file formats compared to `ST_Read`.
- `ST_Read_Multi` is highly inefficient compared to `ST_Read`; this eagerly reads all the data and doesn't support pushdown, spatial index, etc.

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
