
--Creación de vistas con variables que influyen en el SOFA 
DROP IF EXISTS MATERIALIZED VIEW datospacientes.bilirrubina;
CREATE MATERIALIZED VIEW datospacientes.bilirrubina AS
SELECT numhistoria, fecharesultado, horaresultado, codigoexamen, replace(replace(resultado,'<',''), ',', '.')::float, unidad, (fecharesultado + date_trunc('hour', horaresultado) + date_part('minute', horaresultado)::int / 5 * interval '5 min') as "gtime" FROM datospacientes.laboratorio
	WHERE (CAST(numhistoria AS INTEGER) IN (SELECT index_sepsis_table."HC" FROM datospacientes.index_sepsis_table)) and (codigoexamen='B620')
	ORDER BY gtime DESC;

DROP IF EXISTS MATERIALIZED VIEW datospacientes.rplaquetas;
CREATE MATERIALIZED VIEW datospacientes.rplaquetas AS
SELECT numhistoria, fecharesultado, horaresultado, codigoexamen, resultado::float, unidad, (fecharesultado + date_trunc('hour', horaresultado) + date_part('minute', horaresultado)::int / 5 * interval '5 min') as "gtime" FROM datospacientes.laboratorio
	WHERE numhistoria ~ E'^\\d+$' and (CAST(numhistoria AS INTEGER) IN (SELECT index_sepsis_table."HC" FROM datospacientes.index_sepsis_table)) and (codigoexamen='H316') and (unidad = '10^3/uL') and (titulo LIKE '%PLAQUETAS%')
	ORDER BY gtime DESC;

DROP IF EXISTS MATERIALIZED VIEW datospacientes.creatinina;
CREATE MATERIALIZED VIEW datospacientes.creatinina AS
SELECT numhistoria, fecharesultado, horaresultado, codigoexamen, replace(resultado, ',', '.')::float, unidad, (fecharesultado + date_trunc('hour', horaresultado) + date_part('minute', horaresultado)::int / 5 * interval '5 min') as "gtime" FROM datospacientes.laboratorio
	WHERE numhistoria ~ E'^\\d+$' and (CAST(numhistoria AS INTEGER) IN (SELECT index_sepsis_table."HC" FROM datospacientes.index_sepsis_table)) and (codigoexamen='C600') and (unidad = 'mg/dl')
	ORDER BY gtime DESC;

DROP IF EXISTS MATERIALIZED VIEW datospacientes.glasgow;
CREATE MATERIALIZED VIEW datospacientes.glasgow AS
SELECT historia as numhistoria, fecharegistrohce as fecha, horaregistrohce as hora, consecutivo as codigoexamen, replace(dato,'\n','')::float as resultado, (fecharegistrohce + date_trunc('hour', horaregistrohce) + date_part('minute', horaregistrohce)::int / 5 * interval '5 min') as "gtime" FROM datospacientes.temphce115
	WHERE historia ~ E'^\\d+$' and (CAST(historia AS INTEGER) IN (SELECT index_sepsis_table."HC" FROM datospacientes.index_sepsis_table)) and (consecutivo = 6) 
	ORDER BY gtime DESC;


DROP IF EXISTS MATERIALIZED VIEW datospacientes.pafi;
CREATE MATERIALIZED VIEW datospacientes.pafi AS
(SELECT historia as numhistoria, fecharegistrohce as fecharesultado, horaregistrohce as horaresultado, consecutivo as codigoexamen, dato::float as resultado, (fecharegistrohce + date_trunc('hour', horaregistrohce) + date_part('minute', horaregistrohce)::int / 5 * interval '5 min') as "gtime" FROM datospacientes.temphce335
WHERE historia ~ E'^\\d+$' AND consecutivo = 13 AND (CAST(historia AS INTEGER) IN (SELECT index_sepsis_table."HC" FROM datospacientes.index_sepsis_table)) AND dato ~ E'^\\d+$')
	UNION ALL
(SELECT t.numhistoria, t.fecha, t.hora, 0 as codigoexamen, 100*replace(s.resultado,',','.')::float/replace(t.resultado,',','.')::float as resultado, (t.fecharesultado + date_trunc('hour', t.horaresultado) + date_part('minute', t.horaresultado)::int / 5 * interval '5 min') as "gtime" FROM
	(SELECT * FROM datospacientes.laboratorio 
	WHERE codigoexamen = 'G198' AND titulo='FIO2' AND
	(CAST(numhistoria AS INTEGER) IN (SELECT index_sepsis_table."HC" FROM datospacientes.index_sepsis_table)))t
INNER JOIN
	(SELECT * FROM datospacientes.laboratorio 
	WHERE codigoexamen = 'G198' AND titulo='PO2' AND
	(CAST(numhistoria AS INTEGER) IN (SELECT index_sepsis_table."HC" FROM datospacientes.index_sepsis_table)))s
ON t.numot = s.numot)
ORDER BY gtime DESC;


DROP IF EXISTS MATERIALIZED VIEW datospacientes.vent;
CREATE MATERIALIZED VIEW datospacientes.vent AS
SELECT historia, fecharegistrohce, MIN(horaregistrohce) FROM datospacientes.temphce116 WHERE (consecutivo = 6 OR consecutivo = 16) AND (CAST(historia AS INTEGER) IN (SELECT index_sepsis_table."HC" FROM datospacientes.index_sepsis_table))
GROUP BY historia, fecharegistrohce;


DROP IF EXISTS MATERIALIZED VIEW datospacientes.map;
CREATE MATERIALIZED VIEW datospacientes.map AS
SELECT historia as numhistoria, fecharegistrohce as fecha, horaregistrohce as hora, consecutivo as codigoexamen, replace(dato,'\n','')::float as resultado, (fecharegistrohce + date_trunc('hour', horaregistrohce) + date_part('minute', horaregistrohce)::int / 5 * interval '5 min') as "gtime"
FROM datospacientes.temphce116
WHERE historia ~ E'^\\d+$' and (CAST(historia AS INTEGER) IN (SELECT index_sepsis_table."HC" FROM datospacientes.index_sepsis_table)) and (consecutivo = 104) 
ORDER BY gtime DESC;



--Funciones para calculara el SOFA para cada variable
CREATE OR REPLACE FUNCTION sofa_bilirrubina(hc VARCHAR) RETURNS TABLE (gtime timestamp, sofa_score int) AS $$
BEGIN
	RETURN QUERY 
	SELECT bilirrubina.gtime, CASE
		WHEN resultado<1.2 THEN 0
		WHEN resultado >= 1.2 AND resultado <= 1.9 THEN 1
		WHEN resultado > 1.9 AND resultado <= 5.9 THEN 2
		WHEN resultado > 5.9 AND resultado <= 11.9 THEN 3
		WHEN resultado >= 12 THEN 4
	  END sofa_score
	FROM datospacientes.bilirrubina WHERE numhistoria =  hc;
END; $$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION sofa_rplaquetas(hc VARCHAR) RETURNS TABLE (gtime timestamp, sofa_score int) AS $$
BEGIN
	RETURN QUERY 
	SELECT rplaquetas.gtime, CASE
		WHEN resultado>=150 THEN 0
		WHEN resultado < 150 AND resultado >=100 THEN 1
		WHEN resultado < 100 AND resultado >=50 THEN 2
		WHEN resultado < 50 AND resultado >=20 THEN 3
		WHEN resultado < 20 THEN 4
	  END sofa_score
	FROM datospacientes.rplaquetas WHERE numhistoria =  hc;
END; $$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION sofa_creatinina(hc VARCHAR) RETURNS TABLE (gtime timestamp, sofa_score int) AS $$
BEGIN
	RETURN QUERY 
	SELECT creatinina.gtime, CASE
		WHEN resultado<1.2 THEN 0
		WHEN resultado >= 1.2 AND resultado <= 1.9 THEN 1
		WHEN resultado > 1.9 AND resultado <= 3.4 THEN 2
		WHEN resultado > 3.4 AND resultado <= 4.9 THEN 3
		WHEN resultado >= 5 THEN 4
	  END sofa_score
	FROM datospacientes.creatinina WHERE numhistoria =  hc;
END; $$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION sofa_glasgow(hc VARCHAR) RETURNS TABLE (gtime timestamp, sofa_score int) AS $$
BEGIN
	RETURN QUERY 
	SELECT glasgow.gtime, CASE
		WHEN glasgow.resultado = 15 THEN 0
		WHEN glasgow.resultado >= 13 AND glasgow.resultado <= 14 THEN 1
		WHEN glasgow.resultado > 9 AND glasgow.resultado <= 12 THEN 2
		WHEN glasgow.resultado > 5 AND glasgow.resultado <= 9 THEN 3
		WHEN glasgow.resultado <= 5 THEN 4
	  END sofa_score
	FROM datospacientes.glasgow WHERE numhistoria =  hc;
END; $$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION sofa_pafi(hc VARCHAR) RETURNS TABLE (gtime timestamp, sofa_score int) AS $$
BEGIN
	RETURN QUERY 
	SELECT pafi.gtime, CASE
		WHEN pafi.resultado >= 400 THEN 0
		WHEN pafi.resultado >= 300 AND pafi.resultado < 400 THEN 1
		WHEN pafi.resultado >= 200 AND pafi.resultado < 300 THEN 2
		WHEN pafi.resultado >= 100 AND pafi.resultado < 200 THEN 3
		WHEN pafi.resultado <= 100 THEN 4
	  END sofa_score
	FROM datospacientes.pafi WHERE numhistoria =  hc;
END; $$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION sofa_vent(hc VARCHAR) RETURNS TABLE (gtime timestamp, sofa_score int) AS $$
BEGIN
	RETURN QUERY 
	SELECT vent.gtime, 3 as sofa_score
	FROM datospacientes.vent WHERE numhistoria =  hc;
END; $$
LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION sofa_map(hc VARCHAR) RETURNS TABLE (gtime timestamp, sofa_score int) AS $$
BEGIN
	RETURN QUERY 
	SELECT map.gtime, CASE
		WHEN map.resultado >= 70 THEN 0
		WHEN map.resultado < 70 THEN 1
	  END sofa_score
	FROM datospacientes.map WHERE numhistoria =  hc;
END; $$
LANGUAGE 'plpgsql';



-- Funcion para calcular el SOFA para una historia clinica a una hora deteminada
CREATE OR REPLACE FUNCTION sofa_total(hc VARCHAR, hora TIMESTAMP WITHOUT TIME ZONE, fecha_ingreso DATE) RETURNS INTEGER AS $$

BEGIN
	RETURN ((SELECT sofa_score FROM sofa_glasgow(hc) WHERE (gtime <= hora AND gtime>fecha_ingreso-1) UNION ALL SELECT 0 AS sofa_score WHERE NOT EXISTS(SELECT sofa_score FROM sofa_glasgow(hc) WHERE gtime <= hora AND gtime>fecha_ingreso-1) LIMIT 1) + 
	(SELECT sofa_score FROM sofa_creatinina(hc) WHERE (gtime <= hora AND gtime>fecha_ingreso-1) UNION ALL SELECT 0 AS sofa_score WHERE NOT EXISTS(SELECT sofa_score FROM sofa_creatinina(hc) WHERE gtime <= hora AND gtime>fecha_ingreso-1) LIMIT 1) + 
	(SELECT sofa_score FROM sofa_bilirrubina(hc) WHERE (gtime <= hora AND gtime>fecha_ingreso-1) UNION ALL SELECT 0 AS sofa_score WHERE NOT EXISTS(SELECT sofa_score FROM sofa_bilirrubina(hc) WHERE gtime <= hora AND gtime>fecha_ingreso-1) LIMIT 1) + 
	(SELECT sofa_score FROM sofa_rplaquetas(hc) WHERE (gtime <= hora AND gtime>fecha_ingreso-1) UNION ALL SELECT 0 AS sofa_score WHERE NOT EXISTS(SELECT sofa_score FROM sofa_rplaquetas(hc) WHERE gtime <= hora AND gtime>fecha_ingreso-1) LIMIT 1) +
	(SELECT sofa_score FROM sofa_map(hc) WHERE (gtime <= hora AND gtime>fecha_ingreso-1) UNION ALL SELECT 0 AS sofa_score WHERE NOT EXISTS(SELECT sofa_score FROM sofa_map(hc) WHERE gtime <= hora AND gtime>fecha_ingreso-1) LIMIT 1) +
    GREATEST(
	(SELECT sofa_score FROM sofa_pafi(hc) WHERE (gtime <= hora AND gtime>fecha_ingreso-1) UNION ALL SELECT 0 AS sofa_score WHERE NOT EXISTS(SELECT sofa_score FROM sofa_pafi(hc) WHERE gtime <= hora AND gtime>fecha_ingreso-1) LIMIT 1),
	(SELECT sofa_score FROM sofa_vent(hc) WHERE (gtime <= hora AND gtime>fecha_ingreso-1) UNION ALL SELECT 0 AS sofa_score WHERE NOT EXISTS(SELECT sofa_score FROM sofa_vent(hc) WHERE gtime <= hora AND gtime>fecha_ingreso-1) LIMIT 1)));

END;$$
LANGUAGE 'plpgsql';



-- Funcion para calcular el SOFA hora a hora por una ventana de 72 horas al rededor del inicio de infección y detectar si hay un cambio de 2 o mas puntos en este indicador
CREATE OR REPLACE FUNCTION inicio_disfuncion(hc VARCHAR, inicio_infeccion TIMESTAMP WITHOUT TIME ZONE, fecha_ingreso DATE) RETURNS TIMESTAMP WITHOUT TIME ZONE AS $$	
DECLARE
	aux integer := 0;
	min integer :=0;
	desface integer := -48;
	desface_min integer :=-48;
BEGIN
    min = sofa_total(hc,inicio_infeccion + interval '1h' * desface, fecha_ingreso);
	LOOP
		aux = sofa_total(hc,inicio_infeccion + interval '1h' * desface, fecha_ingreso);
		IF ((aux >= min+2) or (desface>24)) THEN
			EXIT;
		END IF;
		
		min:=aux;
		desface := desface+1;
	END LOOP;
	IF (aux >= min+2) THEN
		RETURN inicio_infeccion + interval '1h' * desface;
	END IF;
	RETURN '1970-01-01 00:00:01';
END$$
LANGUAGE 'plpgsql';

CREATE MATERIALIZED VIEW tiempos_inicio_sepsis AS
select *, inicio_disfunction(tiempos_inicio_infeccion."historia final"::VARCHAR, tiempos_inicio_infeccion."tiempo inicio de infeccion",tiempos_inicio_infeccion."fecha_ingreso"), 
GREATEST(inicio_disfunction(tiempos_inicio_infeccion."historia final"::VARCHAR, tiempos_inicio_infeccion."tiempo inicio de infeccion",tiempos_inicio_infeccion."fecha_ingreso"), tiempos_inicio_infeccion."tiempo inicio de infeccion") as inicio_sepsis
from datospacientes.tiempos_inicio_infeccion;
