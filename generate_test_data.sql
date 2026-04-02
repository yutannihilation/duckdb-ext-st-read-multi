LOAD spatial;

CREATE TABLE t_2048 AS
    SELECT point, random() as val
    FROM ST_GeneratePoints({min_x: 0, min_y:0, max_x:10, max_y:10}::BOX_2D, 2048, 42);
CREATE TABLE t_2049 AS
    SELECT point, random() as val
    FROM ST_GeneratePoints({min_x: 0, min_y:0, max_x:10, max_y:10}::BOX_2D, 2049, 42);

COPY t_2048 TO 'test/data/many_rows/points_2048.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');
COPY t_2048 TO 'test/data/many_rows/points_2048.gpkg' WITH (FORMAT GDAL, DRIVER 'GPKG');
COPY t_2048 TO 'test/data/many_rows/points_2048.shp' WITH (FORMAT GDAL, DRIVER 'ESRI Shapefile');

COPY t_2049 TO 'test/data/many_rows/points_2049.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');
COPY t_2049 TO 'test/data/many_rows/points_2049.gpkg' WITH (FORMAT GDAL, DRIVER 'GPKG');
COPY t_2049 TO 'test/data/many_rows/points_2049.shp' WITH (FORMAT GDAL, DRIVER 'ESRI Shapefile');

CREATE TABLE t_dates AS
    SELECT
        ST_Point(0, 0) as geom,
        'event1' as name,
        '2024-01-15'::DATE as event_date,
        '2024-01-15 10:30:00'::TIMESTAMP as event_datetime
    UNION ALL SELECT
        ST_Point(1, 1),
        'event2',
        '2024-06-30'::DATE,
        '2024-06-30 23:59:59.999'::TIMESTAMP
    UNION ALL SELECT
        ST_Point(2, 2),
        'event3',
        NULL,
        NULL;

COPY t_dates TO 'test/data/dates.gpkg' WITH (FORMAT GDAL, DRIVER 'GPKG');
