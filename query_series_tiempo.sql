COPY (
SELECT inicio_sepsis_nuevos."historia final", inicio_sepsis_nuevos."fecha_ingreso"+ interval '1 year'*45 as fecha_ingreso, inicio_sepsis_nuevos."inicio_sepsis"+interval '1 year'*45 as inicio_sepsis, fecharegistrohce+horaregistrohce+interval '1 year'*45 as charttime, sepsis, t1.dato as heartrate
FROM datospacientes.inicio_sepsis_nuevos
INNER JOIN 
    (SELECT historia, dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce116 WHERE consecutivo = '7') AS t1
    ON (t1.historia = inicio_sepsis_nuevos."historia final") AND (inicio_sepsis_nuevos."inicio_sepsis" - interval '1h'*24 < t1."fecharegistrohce" + t1."horaregistrohce") AND (inicio_sepsis_nuevos."inicio_sepsis"- interval '1h' >= t1."fecharegistrohce"+ t1."horaregistrohce")
)
TO '/tmp/output1.csv' DELIMITER ',' CSV HEADER;



COPY (
SELECT inicio_sepsis_nuevos."historia final", inicio_sepsis_nuevos."fecha_ingreso"+ interval '1 year'*45 as fecha_ingreso, inicio_sepsis_nuevos."inicio_sepsis"+interval '1 year'*45 as inicio_sepsis, fecharegistrohce+horaregistrohce+interval '1 year'*45 as charttime, sepsis, t2.dato as tempc
FROM datospacientes.inicio_sepsis_nuevos
INNER JOIN 
    (SELECT historia, dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce116 WHERE consecutivo = '80') AS t2
    ON (t2.historia = inicio_sepsis_nuevos."historia final") AND (inicio_sepsis_nuevos."inicio_sepsis" - interval '1h'*24 < t2."fecharegistrohce" + t2."horaregistrohce") AND (inicio_sepsis_nuevos."inicio_sepsis" - interval '1h' >= t2."fecharegistrohce"+ t2."horaregistrohce")
)
TO '/tmp/output2.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT inicio_sepsis_nuevos."historia final", inicio_sepsis_nuevos."fecha_ingreso"+ interval '1 year'*45 as fecha_ingreso, inicio_sepsis_nuevos."inicio_sepsis"+interval '1 year'*45 as inicio_sepsis, fecharegistrohce+horaregistrohce+interval '1 year'*45 as charttime, sepsis, t3.dato as sysbp
FROM datospacientes.inicio_sepsis_nuevos
INNER JOIN 
    (SELECT historia, dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce116 WHERE consecutivo = '8') AS t3
    ON (t3.historia = inicio_sepsis_nuevos."historia final") AND (inicio_sepsis_nuevos."inicio_sepsis" - interval '1h'*24 < t3."fecharegistrohce" + t3."horaregistrohce") AND (inicio_sepsis_nuevos."inicio_sepsis" - interval '1h' >= t3."fecharegistrohce"+ t3."horaregistrohce")
)
TO '/tmp/output3.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT inicio_sepsis_nuevos."historia final", inicio_sepsis_nuevos."fecha_ingreso"+ interval '1 year'*45 as fecha_ingreso, inicio_sepsis_nuevos."inicio_sepsis"+interval '1 year'*45 as inicio_sepsis, fecharegistrohce+horaregistrohce+interval '1 year'*45 as charttime, sepsis, t4.dato as diasbp
FROM datospacientes.inicio_sepsis_nuevos
INNER JOIN 
    (SELECT historia, dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce116 WHERE consecutivo = '9') AS t4
    ON (t4.historia = inicio_sepsis_nuevos."historia final") AND (inicio_sepsis_nuevos."inicio_sepsis" - interval '1h'*24 < t4."fecharegistrohce" + t4."horaregistrohce") AND (inicio_sepsis_nuevos."inicio_sepsis" - interval '1h' >= t4."fecharegistrohce"+ t4."horaregistrohce")
)
TO '/tmp/output4.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT inicio_sepsis_nuevos."historia final", inicio_sepsis_nuevos."fecha_ingreso"+ interval '1 year'*45 as fecha_ingreso, inicio_sepsis_nuevos."inicio_sepsis"+interval '1 year'*45 as inicio_sepsis, fecharegistrohce+horaregistrohce+interval '1 year'*45 as charttime, sepsis, t5.dato as meanbp
FROM datospacientes.inicio_sepsis_nuevos
INNER JOIN 
    (SELECT historia, dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce116 WHERE consecutivo = '104') AS t5
    ON (t5.historia = inicio_sepsis_nuevos."historia final") AND (inicio_sepsis_nuevos."inicio_sepsis" - interval '1h'*24 < t5."fecharegistrohce" + t5."horaregistrohce") AND (inicio_sepsis_nuevos."inicio_sepsis" - interval '1h' >= t5."fecharegistrohce"+ t5."horaregistrohce")
)
TO '/tmp/output5.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT inicio_sepsis_nuevos."historia final", inicio_sepsis_nuevos."fecha_ingreso"+ interval '1 year'*45 as fecha_ingreso, inicio_sepsis_nuevos."inicio_sepsis"+interval '1 year'*45 as inicio_sepsis, fecharegistrohce+horaregistrohce+interval '1 year'*45 as charttime, sepsis, t6.dato as resprate
FROM datospacientes.inicio_sepsis_nuevos
INNER JOIN 
    (SELECT historia, dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce117 WHERE consecutivo = '17') AS t6
    ON (t6.historia = inicio_sepsis_nuevos."historia final") AND (inicio_sepsis_nuevos."inicio_sepsis" - interval '1h'*24 < t6."fecharegistrohce" + t6."horaregistrohce") AND (inicio_sepsis_nuevos."inicio_sepsis" - interval '1h' >= t6."fecharegistrohce"+ t6."horaregistrohce")
)
TO '/tmp/output6.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT inicio_sepsis_nuevos."historia final", inicio_sepsis_nuevos."fecha_ingreso"+ interval '1 year'*45 as fecha_ingreso, inicio_sepsis_nuevos."inicio_sepsis"+interval '1 year'*45 as inicio_sepsis, fecharegistrohce+horaregistrohce+interval '1 year'*45 as charttime, sepsis, t7.dato as spo2
FROM datospacientes.inicio_sepsis_nuevos
INNER JOIN 
    (SELECT historia, dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce117 WHERE consecutivo = '127') AS t7
    ON (t7.historia = inicio_sepsis_nuevos."historia final") AND (inicio_sepsis_nuevos."inicio_sepsis" - interval '1h'*24 < t7."fecharegistrohce" + t7."horaregistrohce") AND (inicio_sepsis_nuevos."inicio_sepsis" - interval '1h' >= t7."fecharegistrohce"+ t7."horaregistrohce")
)
TO '/tmp/output7.csv' DELIMITER ',' CSV HEADER;


COPY (
SELECT inicio_sepsis_nuevos."historia final", inicio_sepsis_nuevos."fecha_ingreso"+ interval '1 year'*45 as fecha_ingreso, inicio_sepsis_nuevos."inicio_sepsis"+interval '1 year'*45 as inicio_sepsis, fecharegistrohce+horaregistrohce+interval '1 year'*45 as charttime, sepsis, t9.dato as glucose
FROM datospacientes.inicio_sepsis_nuevos
INNER JOIN 
    (SELECT historia, dato, fecharegistrohce, horaregistrohce FROM datospacientes.temphce116 WHERE consecutivo = '100' AND dato ~ E'^\\d+$') AS t9
    ON (t9.historia = inicio_sepsis_nuevos."historia final") AND (inicio_sepsis_nuevos."inicio_sepsis" - interval '1h'*24 < t9."fecharegistrohce" + t9."horaregistrohce") AND (inicio_sepsis_nuevos."inicio_sepsis" - interval '1h' >= t9."fecharegistrohce" + t9."horaregistrohce")
)
TO '/tmp/output8.csv' DELIMITER ',' CSV HEADER;