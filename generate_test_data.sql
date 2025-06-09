LOAD spatial;

CREATE TABLE t_2048 AS
    SELECT point, random() as val
    FROM ST_GeneratePoints({min_x: 0, min_y:0, max_x:10, max_y:10}::BOX_2D, 2048, 42);
CREATE TABLE t_2049 AS
    SELECT point, random() as val
    FROM ST_GeneratePoints({min_x: 0, min_y:0, max_x:10, max_y:10}::BOX_2D, 2049, 42);

COPY t_2048 TO 'test/data/many_rows/points_2048.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');
COPY t_2048 TO 'test/data/many_rows/points_2048.gpkg' WITH (FORMAT GDAL, DRIVER 'GPKG');

COPY t_2049 TO 'test/data/many_rows/points_2049.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');
COPY t_2049 TO 'test/data/many_rows/points_2049.gpkg' WITH (FORMAT GDAL, DRIVER 'GPKG');
