--se crea una vista que contiene los tiempos de inicio de infección para todos los pacientes marcados como sepsis
CREATE OR REPLACE VIEW datospacientes.tiempos_inicio_infeccion AS 
SELECT LEAST(s.fecha_ingreso, t.fecha_ingreso) AS fecha_ingreso,
    LEAST(s.fecha_egreso, t.fecha_egreso) AS fecha_egreso,
    LEAST(t.tiempo_orden_antibiotico, s.tiempo_orden_cultivo) AS "tiempo inicio de infeccion",
    LEAST(t."HC", s."HC") AS "historia final"
FROM (SELECT min(index_sepsis_table."Fecha ingreso") AS fecha_ingreso,
            min(index_sepsis_table."Fecha egreso") AS fecha_egreso,
            min("ordenesAntibioticosAplicaciones".fechadelaorden + "ordenesAntibioticosAplicaciones".horadelaorden) AS tiempo_orden_antibiotico,
            index_sepsis_table."HC"
           FROM datospacientes."ordenesAntibioticosAplicaciones"
             JOIN datospacientes.index_sepsis_table ON "ordenesAntibioticosAplicaciones".historia::integer = index_sepsis_table."HC" AND index_sepsis_table."Fecha ingreso" < ("ordenesAntibioticosAplicaciones".fechadelaorden + "ordenesAntibioticosAplicaciones".horadelaorden) AND index_sepsis_table."Fecha egreso" >= "ordenesAntibioticosAplicaciones".fechadelaorden
          GROUP BY index_sepsis_table."HC", index_sepsis_table."Fecha ingreso") t
    FULL JOIN ( SELECT min(index_sepsis_table."Fecha ingreso") AS fecha_ingreso,
            min(index_sepsis_table."Fecha egreso") AS fecha_egreso,
            min("ordenesMedicasCultivos".fechaorden + "ordenesMedicasCultivos".horaorden) AS tiempo_orden_cultivo,
            index_sepsis_table."HC"
           FROM datospacientes."ordenesMedicasCultivos"
             JOIN datospacientes.index_sepsis_table ON "ordenesMedicasCultivos".historia::integer = index_sepsis_table."HC" AND index_sepsis_table."Fecha ingreso" < ("ordenesMedicasCultivos".fechaorden + "ordenesMedicasCultivos".horaorden) AND index_sepsis_table."Fecha egreso" >= "ordenesMedicasCultivos".fechaorden
          GROUP BY index_sepsis_table."HC", index_sepsis_table."Fecha ingreso") s ON t."HC" = s."HC" AND s.fecha_ingreso = t.fecha_ingreso;