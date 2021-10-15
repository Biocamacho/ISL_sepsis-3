DROP MATERIALIZED VIEW inicio_sepsis_nuevos IF EXISTS
CREATE MATERIALIZED VIEW datospacientes.inicio_sepsis_nuevos AS
SELECT "historia final", fecha_ingreso, GREATEST("tiempo inicio de infeccion", "inicio_disfuncion") as inicio_sepsis, 1 as sepsis
FROM inicio_sepsis_total WHERE inicio_disfuncion>'1975-01-01'
UNION ALL
SELECT hc AS "historia final", "Fecha ingreso" as fecha_ingreso, index_total_view."Fecha ingreso" + '01:00:00'::interval * (24 + floor(random() * 24::double precision + 1::double precision)::integer)::double precision AS inicio_sepsis, 0 as sepsis
FROM datospacientes.index_total_view
WHERE CONCAT(hc, "Fecha ingreso") NOT IN (SELECT CONCAT("historia final", "fecha_ingreso") FROM inicio_sepsis_total WHERE inicio_disfuncion>'1975-01-01');

