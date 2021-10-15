-- se crean las vistas de las variables que influyen en el sofa
DROP MATERIALIZED VIEW IF EXISTS datospacientes.bilirrubina_total;
CREATE MATERIALIZED VIEW datospacientes.bilirrubina_total AS
SELECT numhistoria, fecha, hora, codigoexamen, replace(replace(resultado,'<',''), ',', '.')::float as resultado, unidad, (fecharesultado + date_trunc('hour', horaresultado) + date_part('minute', horaresultado)::int / 5 * interval '5 min') as "gtime" FROM datospacientes.laboratorio
	WHERE (numhistoria IN (SELECT index_total_view."hc" FROM datospacientes.index_total_view)) and (codigoexamen='B620')
	ORDER BY gtime DESC;

DROP MATERIALIZED VIEW IF EXISTS datospacientes.rplaquetas_total;
CREATE MATERIALIZED VIEW datospacientes.rplaquetas_total AS
SELECT numhistoria, fecharesultado, horaresultado, codigoexamen, resultado::float, unidad, (fecharesultado + date_trunc('hour', horaresultado) + date_part('minute', horaresultado)::int / 5 * interval '5 min') as "gtime" FROM datospacientes.laboratorio
	WHERE (numhistoria IN (SELECT index_total_view."hc" FROM datospacientes.index_total_view)) and (codigoexamen='H316') and (unidad = '10^3/uL') and (titulo LIKE '%PLAQUETAS%')
	ORDER BY gtime DESC;

DROP MATERIALIZED VIEW IF EXISTS datospacientes.creatinina_total;
CREATE MATERIALIZED VIEW datospacientes.creatinina_total AS
SELECT numhistoria, fecharesultado, horaresultado, codigoexamen, replace(resultado, ',', '.')::float as resultado, unidad, (fecharesultado + date_trunc('hour', horaresultado) + date_part('minute', horaresultado)::int / 5 * interval '5 min') as "gtime" FROM datospacientes.laboratorio
	WHERE (numhistoria IN (SELECT index_total_view."hc" FROM datospacientes.index_total_view)) and (codigoexamen='C600') and (unidad = 'mg/dl')
	ORDER BY gtime DESC;

DROP MATERIALIZED VIEW IF EXISTS datospacientes.glasgow_total;
CREATE MATERIALIZED VIEW datospacientes.glasgow_total AS
SELECT historia as numhistoria, fecharegistrohce as fecha, horaregistrohce as hora, consecutivo as codigoexamen, replace(dato,'\n','')::float as resultado, (fecharegistrohce + date_trunc('hour', horaregistrohce) + date_part('minute', horaregistrohce)::int / 5 * interval '5 min') as "gtime" FROM datospacientes.temphce115
	WHERE (historia IN (SELECT index_total_view."hc" FROM datospacientes.index_total_view)) and (consecutivo = 6) 
	ORDER BY gtime DESC;


DROP MATERIALIZED VIEW IF EXISTS datospacientes.pafi_total;
CREATE MATERIALIZED VIEW datospacientes.pafi_total AS
(SELECT historia as numhistoria, fecharegistrohce as fecharesultado, horaregistrohce as horaresultado, consecutivo as codigoexamen, dato::float as resultado, (fecharegistrohce + date_trunc('hour', horaregistrohce) + date_part('minute', horaregistrohce)::int / 5 * interval '5 min') as "gtime" FROM datospacientes.temphce335
WHERE consecutivo = 13 AND (historia IN (SELECT index_total_view."hc" FROM datospacientes.index_total_view)) AND dato ~ E'^\\d+$')
	UNION ALL
(SELECT t.numhistoria, t.fecha, t.hora, 0 as codigoexamen, 100*replace(s.resultado,',','.')::float/replace(t.resultado,',','.')::float as resultado, (t.fecharesultado + date_trunc('hour', t.horaresultado) + date_part('minute', t.horaresultado)::int / 5 * interval '5 min') as "gtime" FROM
	(SELECT * FROM datospacientes.laboratorio 
	WHERE codigoexamen = 'G198' AND titulo='FIO2' AND resultado ~ E'^\\d+[,]\d+$' AND
	(numhistoria IN (SELECT index_total_view."hc" FROM datospacientes.index_total_view)))t
INNER JOIN
	(SELECT * FROM datospacientes.laboratorio 
	WHERE codigoexamen = 'G198' AND titulo='PO2' AND resultado ~ E'^\\d+[,]\d+$' AND
	(numhistoria IN (SELECT index_total_view."hc" FROM datospacientes.index_total_view)))s
ON t.numot = s.numot)
ORDER BY gtime DESC;


DROP MATERIALIZED VIEW IF EXISTS datospacientes.vent_total;
CREATE MATERIALIZED VIEW datospacientes.vent_total AS
SELECT historia as numhistoria, fecharegistrohce + MIN(horaregistrohce) as gtime FROM datospacientes.temphce116 WHERE (consecutivo = 6 OR consecutivo = 16) AND (historia IN (SELECT index_total_view."hc" FROM datospacientes.index_total_view))
GROUP BY historia, fecharegistrohce;


DROP MATERIALIZED VIEW IF EXISTS datospacientes.map_total;
CREATE MATERIALIZED VIEW datospacientes.map_total AS
SELECT historia as numhistoria, fecharegistrohce as fecha, horaregistrohce as hora, consecutivo as codigoexamen, replace(dato,'\n','')::float as resultado, (fecharegistrohce + date_trunc('hour', horaregistrohce) + date_part('minute', horaregistrohce)::int / 5 * interval '5 min') as "gtime"
FROM datospacientes.temphce116
WHERE (historia IN (SELECT index_total_view."hc" FROM datospacientes.index_total_view)) and (consecutivo = 104) 
ORDER BY gtime DESC;



-- se crean las funciones para calcular el puntaje del sofa de cada variable
CREATE OR REPLACE FUNCTION sofa_bilirrubina(hc VARCHAR) RETURNS TABLE (gtime timestamp, sofa_score int) AS $$
BEGIN
	RETURN QUERY 
	SELECT bilirrubina_total.gtime, CASE
		WHEN bilirrubina_total.resultado<1.2 THEN 0
		WHEN bilirrubina_total.resultado >= 1.2 AND bilirrubina_total.resultado <= 1.9 THEN 1
		WHEN bilirrubina_total.resultado > 1.9 AND bilirrubina_total.resultado <= 5.9 THEN 2
		WHEN bilirrubina_total.resultado > 5.9 AND bilirrubina_total.resultado <= 11.9 THEN 3
		WHEN bilirrubina_total.resultado >= 12 THEN 4
	  END sofa_score
	FROM datospacientes.bilirrubina_total WHERE numhistoria =  hc;
END; $$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION sofa_rplaquetas(hc VARCHAR) RETURNS TABLE (gtime timestamp, sofa_score int) AS $$
BEGIN
	RETURN QUERY 
	SELECT rplaquetas_total.gtime, CASE
		WHEN rplaquetas_total.resultado>=150 THEN 0
		WHEN rplaquetas_total.resultado < 150 AND rplaquetas_total.resultado >=100 THEN 1
		WHEN rplaquetas_total.resultado < 100 AND rplaquetas_total.resultado >=50 THEN 2
		WHEN rplaquetas_total.resultado < 50 AND rplaquetas_total.resultado >=20 THEN 3
		WHEN rplaquetas_total.resultado < 20 THEN 4
	  END sofa_score
	FROM datospacientes.rplaquetas_total WHERE numhistoria =  hc;
END; $$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION sofa_creatinina(hc VARCHAR) RETURNS TABLE (gtime timestamp, sofa_score int) AS $$
BEGIN
	RETURN QUERY 
	SELECT creatinina_total.gtime, CASE
		WHEN creatinina_total.resultado<1.2 THEN 0
		WHEN creatinina_total.resultado >= 1.2 AND creatinina_total.resultado <= 1.9 THEN 1
		WHEN creatinina_total.resultado > 1.9 AND creatinina_total.resultado <= 3.4 THEN 2
		WHEN creatinina_total.resultado > 3.4 AND creatinina_total.resultado <= 4.9 THEN 3
		WHEN creatinina_total.resultado >= 5 THEN 4
	  END sofa_score
	FROM datospacientes.creatinina_total WHERE numhistoria =  hc;
END; $$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION sofa_glasgow(hc VARCHAR) RETURNS TABLE (gtime timestamp, sofa_score int) AS $$
BEGIN
	RETURN QUERY 
	SELECT glasgow_total.gtime, CASE
		WHEN glasgow_total.resultado = 15 THEN 0
		WHEN glasgow_total.resultado >= 13 AND glasgow_total.resultado <= 14 THEN 1
		WHEN glasgow_total.resultado > 9 AND glasgow_total.resultado <= 12 THEN 2
		WHEN glasgow_total.resultado > 5 AND glasgow_total.resultado <= 9 THEN 3
		WHEN glasgow_total.resultado <= 5 THEN 4
	  END sofa_score
	FROM datospacientes.glasgow_total WHERE numhistoria =  hc;
END; $$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION sofa_pafi(hc VARCHAR) RETURNS TABLE (gtime timestamp, sofa_score int) AS $$
BEGIN
	RETURN QUERY 
	SELECT pafi_total.gtime, CASE
		WHEN pafi_total.resultado >= 400 THEN 0
		WHEN pafi_total.resultado >= 300 AND pafi_total.resultado < 400 THEN 1
		WHEN pafi_total.resultado >= 200 AND pafi_total.resultado < 300 THEN 2
		WHEN pafi_total.resultado >= 100 AND pafi_total.resultado < 200 THEN 3
		WHEN pafi_total.resultado <= 100 THEN 4
	  END sofa_score
	FROM datospacientes.pafi_total WHERE numhistoria =  hc;
END; $$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION sofa_vent(hc VARCHAR) RETURNS TABLE (gtime timestamp, sofa_score int) AS $$
BEGIN
	RETURN QUERY 
	SELECT vent_total.gtime, 3 as sofa_score
	FROM datospacientes.vent_total WHERE numhistoria =  hc;
END; $$
LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION sofa_map(hc VARCHAR) RETURNS TABLE (gtime timestamp, sofa_score int) AS $$
BEGIN
	RETURN QUERY 
	SELECT map_total.gtime, CASE
		WHEN map_total.resultado >= 70 THEN 0
		WHEN map_total.resultado < 70 THEN 1
	  END sofa_score
	FROM datospacientes.map_total WHERE numhistoria =  hc;
END; $$
LANGUAGE 'plpgsql';




--funcion para calcular el sofa total
CREATE OR REPLACE FUNCTION sofa_total(hc VARCHAR, hora TIMESTAMP WITHOUT TIME ZONE, fecha_ingreso DATE) RETURNS INTEGER AS $$

BEGIN
	RETURN ((SELECT sofa_score FROM sofa_glasgow(hc) WHERE (gtime <= hora AND gtime>fecha_ingreso-1) UNION ALL SELECT 0 AS sofa_score WHERE NOT EXISTS(SELECT sofa_score FROM sofa_glasgow(hc) WHERE gtime <= hora AND gtime>fecha_ingreso-1) LIMIT 1) + 
	(SELECT sofa_score FROM sofa_creatinina(hc) WHERE (gtime <= hora AND gtime>fecha_ingreso-1) UNION ALL SELECT 0 AS sofa_score WHERE NOT EXISTS(SELECT sofa_score FROM sofa_creatinina(hc) WHERE gtime <= hora AND gtime>fecha_ingreso-1) LIMIT 1) + 
	(SELECT sofa_score FROM sofa_bilirrubina(hc) WHERE (gtime <= hora AND gtime>fecha_ingreso-1) UNION ALL SELECT 0 AS sofa_score WHERE NOT EXISTS(SELECT sofa_score FROM sofa_bilirrubina(hc) WHERE gtime <= hora AND gtime>fecha_ingreso-1) LIMIT 1) + 
	(SELECT sofa_score FROM sofa_rplaquetas(hc) WHERE (gtime <= hora AND gtime>fecha_ingreso-1) UNION ALL SELECT 0 AS sofa_score WHERE NOT EXISTS(SELECT sofa_score FROM sofa_rplaquetas(hc) WHERE gtime <= hora AND gtime>fecha_ingreso-1) LIMIT 1) +
	GREATEST(
	(SELECT sofa_score FROM sofa_pafi(hc) WHERE (gtime <= hora AND gtime>fecha_ingreso-1) UNION ALL SELECT 0 AS sofa_score WHERE NOT EXISTS(SELECT sofa_score FROM sofa_pafi(hc) WHERE gtime <= hora AND gtime>fecha_ingreso-1) LIMIT 1),
	(SELECT sofa_score FROM sofa_vent(hc) WHERE (gtime <= hora AND gtime>fecha_ingreso-1) UNION ALL SELECT 0 AS sofa_score WHERE NOT EXISTS(SELECT sofa_score FROM sofa_vent(hc) WHERE gtime <= hora AND gtime>fecha_ingreso-1) LIMIT 1)));

END;$$
LANGUAGE 'plpgsql';


--funcion para calcular el inicio de la disfunción 
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
		--IF aux<min THEN
		--	min:=aux;
		--	desface_min:=desface;
		--END IF;
		min:=aux;
		desface := desface+1;
	END LOOP;
	IF (aux >= min+2) THEN
		RETURN inicio_infeccion + interval '1h' * desface;
	END IF;
	RETURN '1970-01-01 00:00:01';
END$$
LANGUAGE 'plpgsql';

DROP MATERIALIZED VIEW inicio_sepsis_nuevos IF EXISTS
CREATE MATERIALIZED VIEW inicio_sepsis_nuevos as
select *, inicio_disfuncion(tiempos_inicio_infeccion_total."historia final"::VARCHAR, tiempos_inicio_infeccion_total."tiempo inicio de infeccion",tiempos_inicio_infeccion_total."fecha_ingreso") as inicio_disfuncion from datospacientes.tiempos_inicio_infeccion_total ORDER BY inicio_disfuncion;
