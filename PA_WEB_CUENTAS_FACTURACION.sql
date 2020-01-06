CREATE OR REPLACE FUNCTION TESTION.PA_WEB_CUENTAS_FACTURACION(
    p_query json,
    OUT datos json
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE
AS $BODY$
DECLARE 
    get_data     json;
    get_rows     integer;
    pagination   json;
BEGIN
    if(p_query->>'fechaini'='')then
        p_query=((p_query)::jsonb - 'fechaini')::json;
    end if;

    if(p_query->>'fechafin'='')then
        p_query=((p_query)::jsonb -'fechafin')::json;
    end if; 

    if(p_query->>'fechaini' is null)then
        p_query = (trim( trailing '}' from (p_query)::text) || ', "fechaini":"'||(select testsp.SP_UTI_DATE_STR((SELECT o_fecha_ini FROM testst.pa_uti_rango_fechas_v1_00('D', 'H'))::date, 'DD/MM/YYYY'))||'"}')::json;
        p_query = (trim( trailing '}' from (p_query)::text) || ', "fechafin":"'||(select testsp.SP_UTI_DATE_STR((SELECT o_fecha_ini FROM testst.pa_uti_rango_fechas_v1_00('D', 'H'))::date, 'DD/MM/YYYY'))||'"}')::json;
    end if; 
    
    --Tabla temp
    DROP TABLE IF EXISTS temp_cta_fact;
    CREATE TEMP TABLE temp_cta_fact AS (
        SELECT  
            pp.trafico              AS SP_PED_TRAFICO,
            substring( cast( extract(year from pp.fec_pago) as varchar(4)) from 3 for 2 )|| '' ||substring( pp.adu_desp from 1 for 2) || '' || pp.pat_agen || '' || pp.num_pedi AS SP_PED_PEDIMENTO,
            pp.adu_desp             AS SP_PED_ADUDESP,
            pp.pat_agen             AS SP_PED_PATENTE,
            iif(pp.imp_expo = '1','IMPORTACION'::text,'EXPORTACION'::text) AS SP_PED_TIPOOPE,
            pp.cve_pedi             AS SP_PED_CVEPED,
            pp.nom_imp              AS SP_PED_NOMCTE, 
            cg.factura              AS SP_PED_FOLIO,
            cg.fecha                AS SP_CTA_FECHACG,
            cg.id_cuentagto         AS idctagto,
            cg.cliente              AS SP_CTA_CTEFACTURA
            (select COALESCE(pedido,'')
              into sta_cta_filecta
              from testst.REFERENCIA
             where referencia = pp.trafico)

--        LEFT JOIN ( SELECT REFERENCIA, COALESCE(pedido,'') FROM TESTST.REFERENCIA) E_RE ON (E_RE.REFERENCIA = pp.trafico) 
        from testsp.pedimento pp
        inner join testcg.cuentagto cg on cg.traficoori = pp.trafico     
        where pp.trafico IS NOT NULL

    --filtro de fecha
    AND (((p_query->>'fechaini' IS NULL) AND (p_query->>'fechafin' IS NULL)) OR (to_char(PP.FEC_PAGO::date, 'dd/mm/yyyy') BETWEEN (select testsp.SP_UTI_DATE_STR((p_query->>'fechaini')::date, 'DD/MM/YYYY')) AND (select testsp.SP_UTI_DATE_STR((p_query->>'fechafin')::date, 'DD/MM/YYYY'))))
    --filtro de grupo
    AND((p_query->>'grupo' IS NULL) OR (PP.CVE_IMPO::text IN(SELECT UNNEST ((SELECT string_to_array(p_query->>'grupo'::text,'.'))))))
    --filtro de cliente
    AND((p_query->>'cliente' IS NULL) OR (PP.CVE_IMPO::text IN(SELECT UNNEST ((SELECT string_to_array(p_query->>'cliente'::text,','))))))
    --filtro de aduana
    AND((p_query->>'aduana' IS NULL) or((pp.adu_desp||coalesce(pp.cve_refis,'''')::text IN(SELECT UNNEST ((SELECT string_to_array(p_query->>'aduana'::text, ',')))) ) ))
    --filtro de operación
    AND((p_query->>'operacion' IS NULL) OR(PP.IMP_EXPO::text IN(SELECT UNNEST((SELECT string_to_array(p_query->>'operación'::text,','))))))
);

    --Paginación 
    get_rows = (SELECT COUNT(SP_PED_TRAFICO) FROM temp_cta_fact);
    pagination = (select testion.pa_web_pagination((p_query->>'pagina')::integer,(p_query->>'rows')::integer,get_rows));
	if(p_query->>'pagina' IS NULL)THEN
    get_data   = (
		SELECT array_to_json(array_agg(row_to_json(response)))
        FROM (
            SELECT * FROM temp_cta_fact)response
    );
    else
        get_data= (SELECT array_to_json(array_agg(row_to_json(response)))
        FROM (

            SELECT
                --Trafico 	
                tb.SP_PED_TRAFICO       AS "Trafico",
                --Pedimento 	
                tb.SP_PED_PEDIMENTO     AS "Pedimento",
                --Aduana 	
                tb.SP_PED_ADUDESP       AS "Aduana",
                --Patente 	
                tb.SP_PED_PATENTE       AS "Patente",
                --Operacion 	
                tb.SP_PED_TIPOOPE       AS "Operacion",
                --Clave 	
                tb.SP_PED_CVEPED        AS "Clave",
                --Cliente 	
                tb.SP_PED_NOMCTE        AS "Cliente",
                --Folio 	
                tb.Folio                AS "Folio",
                --Fecha Cta. Gto. 	
                tb.SP_CTA_FECHACG       AS "Fecha Cta. Gto.",
                --File
                tb.ST_CTA_FILECTA       AS "File"
         FROM temp_cta_fact AS tb
        OFFSET (pagination->>'offset')::integer LIMIT (pagination->>'limit')::integer
        ) response
    );
    END IF;

    datos = json_build_object(
        'data'      , (select get_data),
        'rows'      , get_rows,
        'pages'     , (pagination->>'pages')::integer,
        'filters'   , (p_query)
    );
END 
$BODY$;
ALTER FUNCTION TESTION.PA_WEB_CUENTAS_FACTURACION(json)
    OWNER TO aplicaciones; 