--se crea la vista index_total_view
Create Materialized View index_total_view as
SELECT "Fecha ingreso","HC"::VARCHAR as hc from datospacientes.index_sepsis_table
UNION ALL 
SELECT * from datospacientes.no_sepsis_index;


--se crea una vista que contiene los tiempos de inicio de infección para todos los pacientes a los que se puede calcular
CREATE MATERIALIZED VIEW datospacientes.tiempos_inicio_infeccion_total as
SELECT LEAST("s"."fecha_ingreso","t"."fecha_ingreso") as fecha_ingreso, LEAST(t.tiempo_orden_antibiotico, s.tiempo_orden_cultivo) as "tiempo inicio de infeccion", LEAST(s."hc",t,."hc") as "historia final" from
	(SELECT MIN(datospacientes.index_total_view."Fecha ingreso") as "fecha_ingreso", MIN(datospacientes."ordenesAntibioticosAplicaciones"."fechadelaorden" + datospacientes."ordenesAntibioticosAplicaciones"."horadelaorden") as "tiempo_orden_antibiotico", datospacientes.index_total_view."hc"
	    FROM datospacientes."ordenesAntibioticosAplicaciones"
	INNER JOIN 
        datospacientes.index_total_view 
        ON datospacientes."ordenesAntibioticosAplicaciones".historia = datospacientes.index_total_view."hc" 
        AND (datospacientes.index_total_view."Fecha ingreso" < datospacientes."ordenesAntibioticosAplicaciones"."fechadelaorden" + datospacientes."ordenesAntibioticosAplicaciones"."horadelaorden") 
        AND (datospacientes.index_total_view."Fecha ingreso" + interval '1day'*20  >= datospacientes."ordenesAntibioticosAplicaciones"."fechadelaorden")
	GROUP BY datospacientes.index_total_view."hc",  datospacientes.index_total_view."Fecha ingreso")t
FULL OUTER JOIN
	(SELECT MIN(datospacientes.index_total_view."Fecha ingreso") as "fecha_ingreso", MIN(datospacientes."ordenesMedicasCultivos"."fechaorden" + datospacientes."ordenesMedicasCultivos"."horaorden") as "tiempo_orden_cultivo", datospacientes.index_total_view."hc"
	    FROM datospacientes."ordenesMedicasCultivos"
	INNER JOIN 
        datospacientes.index_total_view 
        ON datospacientes."ordenesMedicasCultivos".historia = datospacientes.index_total_view."hc" 
        AND (datospacientes.index_total_view."Fecha ingreso" < datospacientes."ordenesMedicasCultivos"."fechaorden" + datospacientes."ordenesMedicasCultivos"."horaorden") 
        AND (datospacientes.index_total_view."Fecha ingreso" + interval '1day'*20 >= datospacientes."ordenesMedicasCultivos"."fechaorden")
	GROUP BY datospacientes.index_total_view."hc",  datospacientes.index_total_view."Fecha ingreso")s
ON (t."hc" = s."hc" AND s."fecha_ingreso" = t."fecha_ingreso");
