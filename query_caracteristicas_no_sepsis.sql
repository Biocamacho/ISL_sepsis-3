COPY (
SELECT no_sepsis."hc", no_sepsis.inicio_sepsis + interval '1 year'*45 as inicio_sepsis, 
    MIN((0||t1.dato)::float) as heartrate_min, MAX((0||t1.dato)::float) as heartrate_max
FROM datospacientes.no_sepsis
LEFT JOIN 
    (SELECT historia, dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce116 WHERE consecutivo = '7') AS t1
    ON (t1.historia = no_sepsis."hc") AND (no_sepsis."inicio_sepsis" - interval '1h'*24 < t1."fecharegistrohce" + t1."horaregistrohce") AND (no_sepsis."inicio_sepsis"- interval '1h' >= t1."fecharegistrohce")
GROUP BY no_sepsis."hc", no_sepsis.inicio_sepsis
)
TO '/tmp/output1.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT no_sepsis."hc", no_sepsis.inicio_sepsis + interval '1 year'*45 as inicio_sepsis, 
    MIN(LEFT(0||replace(t2.dato,'..','.'),5)::float) as tempc_min, MAX(LEFT(0||replace(t2.dato,'..','.'),5)::float) as tempc_max
FROM datospacientes.no_sepsis
LEFT JOIN 
    (SELECT historia, dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce116 WHERE consecutivo = '80') AS t2
    ON (t2.historia = no_sepsis."hc") AND (no_sepsis."inicio_sepsis" - interval '1h'*24 < t2."fecharegistrohce" + t2."horaregistrohce") AND (no_sepsis."inicio_sepsis" - interval '1h' >= t2."fecharegistrohce")
GROUP BY no_sepsis."hc", no_sepsis.inicio_sepsis
)
TO '/tmp/output2.csv' DELIMITER ',' CSV HEADER;



COPY (
SELECT no_sepsis."hc", no_sepsis.inicio_sepsis + interval '1 year'*45 as inicio_sepsis, 
    MIN((0||t3.dato)::float) as sysbp_min, MAX((0||t3.dato)::float) as sysbp_max, MAX((0||t1.dato)::float)/MAX((0||t3.dato)::float) as si_max
FROM datospacientes.no_sepsis
LEFT JOIN 
    (SELECT historia, dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce116 WHERE consecutivo = '7') AS t1
    ON (t1.historia = no_sepsis."hc") AND (no_sepsis."inicio_sepsis" - interval '1h'*24  < t1."fecharegistrohce" + t1."horaregistrohce") AND (no_sepsis."inicio_sepsis" - interval '1h' >= t1."fecharegistrohce")
LEFT JOIN 
    (SELECT historia, dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce116 WHERE consecutivo = '8' AND dato ~ E'^\\d+$') AS t3
    ON (t3.historia = no_sepsis."hc") AND (no_sepsis."inicio_sepsis" - interval '1h'*24 < t3."fecharegistrohce" + t3."horaregistrohce") AND (no_sepsis."inicio_sepsis" - interval '1h' >= t3."fecharegistrohce")
GROUP BY no_sepsis."hc", no_sepsis.inicio_sepsis
)
TO '/tmp/output3.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT no_sepsis."hc", no_sepsis.inicio_sepsis + interval '1 year'*45 as inicio_sepsis, 
    MIN((0||t4.dato)::float) as diasbp_min, MAX((0||t4.dato)::float) as diasbp_max
FROM datospacientes.no_sepsis
LEFT JOIN 
    (SELECT historia, dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce116 WHERE consecutivo = '9' AND dato ~ E'^\\d+$') AS t4
    ON (t4.historia = no_sepsis."hc") AND (no_sepsis."inicio_sepsis" - interval '1h'*24 < t4."fecharegistrohce" + t4."horaregistrohce") AND (no_sepsis."inicio_sepsis" - interval '1h' >= t4."fecharegistrohce")
GROUP BY no_sepsis."hc", no_sepsis.inicio_sepsis
)
TO '/tmp/output4.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT no_sepsis."hc", no_sepsis.inicio_sepsis + interval '1 year'*45 as inicio_sepsis, 
    MIN((0||t5.dato)::float) as meanbp_min, MAX((0||t5.dato)::float) as meanbp_max
FROM datospacientes.no_sepsis
LEFT JOIN 
    (SELECT historia, dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce116 WHERE consecutivo = '104') AS t5
    ON (t5.historia = no_sepsis."hc") AND (no_sepsis."inicio_sepsis" - interval '1h'*24 < t5."fecharegistrohce" + t5."horaregistrohce") AND (no_sepsis."inicio_sepsis" - interval '1h' >= t5."fecharegistrohce")
GROUP BY no_sepsis."hc", no_sepsis.inicio_sepsis
)
TO '/tmp/output5.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT no_sepsis."hc", no_sepsis.inicio_sepsis + interval '1 year'*45 as inicio_sepsis, 
    MIN((0||t6.dato)::float) as resprate_min, MAX((0||t6.dato)::float) as resprate_max
FROM datospacientes.no_sepsis
LEFT JOIN 
    (SELECT historia, dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce117 WHERE consecutivo = '17' AND dato ~ E'^\\d+$') AS t6
    ON (t6.historia = no_sepsis."hc") AND (no_sepsis."inicio_sepsis" - interval '1h'*24 < t6."fecharegistrohce" + t6."horaregistrohce") AND (no_sepsis."inicio_sepsis" - interval '1h' >= t6."fecharegistrohce")
GROUP BY no_sepsis."hc", no_sepsis.inicio_sepsis
)
TO '/tmp/output6.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT no_sepsis."hc", no_sepsis.inicio_sepsis + interval '1 year'*45 as inicio_sepsis, 
    MIN((0||t7.dato)::float) as spo2_min, AVG((0||t7.dato)::float) as spo2_mean
FROM datospacientes.no_sepsis
LEFT JOIN 
    (SELECT historia, dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce117 WHERE consecutivo = '127') AS t7
    ON (t7.historia = no_sepsis."hc") AND (no_sepsis."inicio_sepsis" - interval '1h'*24 < t7."fecharegistrohce" + t7."horaregistrohce") AND (no_sepsis."inicio_sepsis" - interval '1h' >= t7."fecharegistrohce")
GROUP BY no_sepsis."hc", no_sepsis.inicio_sepsis
)
TO '/tmp/output7.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT no_sepsis."hc", no_sepsis.inicio_sepsis + interval '1 year'*45 as inicio_sepsis, 
    MIN((0||replace(t8.dato,'^',''))::float) as mingcs
FROM datospacientes.no_sepsis
LEFT JOIN 
    (SELECT historia, dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce115 WHERE consecutivo = '95') AS t8
    ON (t8.historia = no_sepsis."hc") AND (no_sepsis."inicio_sepsis" - interval '1h'*24 < t8."fecharegistrohce" + t8."horaregistrohce") AND (no_sepsis."inicio_sepsis" - interval '1h' >= t8."fecharegistrohce")
GROUP BY no_sepsis."hc", no_sepsis.inicio_sepsis
)
TO '/tmp/output8.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT no_sepsis."hc", no_sepsis.inicio_sepsis + interval '1 year'*45 as inicio_sepsis, 
    MIN((0||replace(replace(t9.dato,'..',''),' ',''))::float) as glucose_min, MAX((0||replace(replace(t9.dato,'..',''),' ',''))::float) as glucose_max
FROM datospacientes.no_sepsis
LEFT JOIN 
    (SELECT historia, dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce116 WHERE consecutivo = '100') AS t9
    ON (t9.historia = no_sepsis."hc") AND (no_sepsis."inicio_sepsis" - interval '1h'*24 < t9."fecharegistrohce" + t9."horaregistrohce") AND (no_sepsis."inicio_sepsis" - interval '1h' >= t9."fecharegistrohce")
GROUP BY no_sepsis."hc", no_sepsis.inicio_sepsis
)
TO '/tmp/output9.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT no_sepsis."hc", no_sepsis.inicio_sepsis + interval '1 year'*45 as inicio_sepsis, 
    MIN((0||replace(t10.dato,',','.'))::float) as hematocrit_min, MAX((0||replace(t10.dato,',','.'))::float) as hematocrit_max
FROM datospacientes.no_sepsis
LEFT JOIN 
    (SELECT numhistoria as historia, resultado as dato, fecha as fecharegistrohce, hora as horaregistrohce FROM datospacientes.laboratorio WHERE codigoexamen = 'H316' AND titulo = 'HEMATOCRITO') AS t10
    ON (t10.historia = no_sepsis."hc") AND (no_sepsis."inicio_sepsis" - interval '1h'*24 < t10."fecharegistrohce" + t10."horaregistrohce") AND (no_sepsis."inicio_sepsis" - interval '1h' >= t10."fecharegistrohce")
GROUP BY no_sepsis."hc", no_sepsis.inicio_sepsis
)
TO '/tmp/output10.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT no_sepsis."hc", no_sepsis.inicio_sepsis + interval '1 year'*45 as inicio_sepsis, 
    MIN((0||replace(t11.dato,',','.'))::float) as hemoglobin_min, MAX((0||replace(t11.dato,',','.'))::float) as hemoglobin_max
FROM datospacientes.no_sepsis
LEFT JOIN 
    (SELECT numhistoria as historia, resultado as dato, fecha as fecharegistrohce, hora as horaregistrohce FROM datospacientes.laboratorio WHERE codigoexamen = 'H316' AND titulo = 'HEMOGLOBINA') AS t11
    ON (t11.historia = no_sepsis."hc") AND (no_sepsis."inicio_sepsis" - interval '1h'*24 < t11."fecharegistrohce" + t11."horaregistrohce") AND (no_sepsis."inicio_sepsis" - interval '1h' >= t11."fecharegistrohce")
GROUP BY no_sepsis."hc", no_sepsis.inicio_sepsis
)
TO '/tmp/output11.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT no_sepsis."hc", no_sepsis.inicio_sepsis + interval '1 year'*45 as inicio_sepsis, 
    MIN((0||replace(t12.dato,',','.'))::float) as creatinine_min, MAX((0||replace(t12.dato,',','.'))::float) as creatinine_max
FROM datospacientes.no_sepsis
LEFT JOIN 
    (SELECT numhistoria as historia, resultado as dato, fecha as fecharegistrohce, hora as horaregistrohce FROM datospacientes.laboratorio WHERE codigoexamen = 'C600' AND unidad = 'mg/dl') as t12
    ON (t12.historia = no_sepsis."hc") AND (no_sepsis."inicio_sepsis" - interval '1h'*24 < t12."fecharegistrohce" + t12."horaregistrohce") AND (no_sepsis."inicio_sepsis" - interval '1h' >= t12."fecharegistrohce")
GROUP BY no_sepsis."hc", no_sepsis.inicio_sepsis
)
TO '/tmp/output12.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT no_sepsis."hc", no_sepsis.inicio_sepsis + interval '1 year'*45 as inicio_sepsis, 
    MIN((0||replace(t13.dato,',','.'))::float) as platelet_min, MAX((0||replace(t13.dato,',','.'))::float) as platelet_max
FROM datospacientes.no_sepsis
LEFT JOIN 
    (SELECT numhistoria as historia, resultado as dato, fecha as fecharegistrohce, hora as horaregistrohce FROM datospacientes.laboratorio WHERE codigoexamen = 'H316' AND titulo LIKE '%PLAQUETAS%' ) as t13
    ON (t13.historia = no_sepsis."hc") AND (no_sepsis."inicio_sepsis" - interval '1h'*24 < t13."fecharegistrohce" + t13."horaregistrohce") AND (no_sepsis."inicio_sepsis" - interval '1h' >= t13."fecharegistrohce")
GROUP BY no_sepsis."hc", no_sepsis.inicio_sepsis
)
TO '/tmp/output13.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT no_sepsis."hc", no_sepsis.inicio_sepsis + interval '1 year'*45 as inicio_sepsis, 
    MIN((0||replace(t14.dato,',','.'))::float) as wbc_min, MAX((0||replace(t14.dato,',','.'))::float) as wbc_max
FROM datospacientes.no_sepsis
LEFT JOIN 
    (SELECT numhistoria as historia, resultado as dato, fecha as fecharegistrohce, hora as horaregistrohce FROM datospacientes.laboratorio WHERE codigoexamen = 'H316' AND titulo LIKE '%LEUCOCITO%' ) as t14
    ON (t14.historia = no_sepsis."hc") AND (no_sepsis."inicio_sepsis" - interval '1h'*24 < t14."fecharegistrohce" + t14."horaregistrohce") AND (no_sepsis."inicio_sepsis" - interval '1h' >= t14."fecharegistrohce")
GROUP BY no_sepsis."hc", no_sepsis.inicio_sepsis
)
TO '/tmp/output14.csv' DELIMITER ',' CSV HEADER;



COPY (
SELECT no_sepsis."hc", no_sepsis.inicio_sepsis + interval '1 year'*45 as inicio_sepsis, 
    MAX(t15.dato::float) as cancer
FROM datospacientes.no_sepsis
LEFT JOIN 
    (SELECT historia, CASE WHEN dato LIKE '%Si%' THEN 1 WHEN dato LIKE '%No%' THEN 0 END as dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce360 WHERE consecutivo = '156' OR consecutivo = '26') AS t15
    ON (t15.historia = no_sepsis."hc") AND (no_sepsis."inicio_sepsis" - interval '1h'*24 < t15."fecharegistrohce" + t15."horaregistrohce") AND (no_sepsis."inicio_sepsis"- interval '1h' >= t15."fecharegistrohce")
GROUP BY no_sepsis."hc", no_sepsis.inicio_sepsis
)
TO '/tmp/output15.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT no_sepsis."hc", no_sepsis.inicio_sepsis + interval '1 year'*45 as inicio_sepsis, 
    MAX(t16.dato::float) as diabetes
FROM datospacientes.no_sepsis
LEFT JOIN 
    (SELECT historia, CASE WHEN dato LIKE '%Si%' THEN 1 WHEN dato LIKE '%No%' THEN 0 END as dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce360 WHERE consecutivo = '22') AS t16
    ON (t16.historia = no_sepsis."hc") AND (no_sepsis."inicio_sepsis" - interval '1h'*24 < t16."fecharegistrohce" + t16."horaregistrohce") AND (no_sepsis."inicio_sepsis"- interval '1h' >= t16."fecharegistrohce")
GROUP BY no_sepsis."hc", no_sepsis.inicio_sepsis
)
TO '/tmp/output16.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT no_sepsis."hc", no_sepsis.inicio_sepsis + interval '1 year'*45 as inicio_sepsis, 
    MAX(t17.dato::float) as peso
FROM datospacientes.no_sepsis
LEFT JOIN 
    (SELECT historia, dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce366 WHERE consecutivo = '134') AS t17
    ON (t17.historia = no_sepsis."hc") AND (no_sepsis."inicio_sepsis" - interval '1h'*24 < t17."fecharegistrohce" + t17."horaregistrohce") AND (no_sepsis."inicio_sepsis"- interval '1h' >= t17."fecharegistrohce")
GROUP BY no_sepsis."hc", no_sepsis.inicio_sepsis
)
TO '/tmp/output17.csv' DELIMITER ',' CSV HEADER;

COPY (
select no_sepsis.hc, no_sepsis.inicio_sepsis + interval '1 year'*45 as inicio_sepsis, 
    MIN((no_sepsis.fecha_ingreso-"pacientesUciUce".fechanacimiento)/365) AS edad
FROM 
	datospacientes.no_sepsis
LEFT JOIN
	datospacientes."pacientesUciUce"
	ON no_sepsis.hc = "pacientesUciUce".historia
GROUP BY no_sepsis.hc, no_sepsis.inicio_sepsis
)
TO '/tmp/output18.csv' DELIMITER ',' CSV HEADER;
