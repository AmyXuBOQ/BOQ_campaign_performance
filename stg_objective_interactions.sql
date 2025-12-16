/*******************************************************/ 
/*** reporting.stg_objective_interactions *************/ 
/*****************************************************/ 
DROP TABLE IF EXISTS reporting.temp_variables_obj; 
CREATE TABLE IF NOT EXISTS reporting.temp_variables_obj AS
WITH CTE AS ( 
SELECT DISTINCT
    table_name 
    ,max_updated_time 
FROM reporting.log_campaign_performance
WHERE (table_name = 'stg_campaign_interactions' OR table_name LIKE 'OBJ%')
  AND run_timestamp::date < CURRENT_DATE
) 
SELECT MASTER.table_name 
        ,CASE WHEN OBJ.table_name IS NULL OR CAM.table_name IS NULL THEN '1900-01-01'
					ELSE least (OBJ.dt, CAM.dt)
					END::TIMESTAMP 		AS last_run_max  
        ,CURRENT_TIMESTAMP           	AS today_run_max 
FROM (SELECT 'AA' AS table_name 
		UNION SELECT 'ACS' 
		UNION SELECT 'T24' 
	) MASTER
LEFT JOIN ( SELECT (string_to_array(table_name, '-'))[2]              AS table_name 
                ,MAX ( COALESCE ( max_updated_time, '1900-01-01') )   AS dt 
                FROM CTE 
                WHERE table_name LIKE 'OBJ%' 
                GROUP BY 1
                ) OBJ 
	ON MASTER.table_name = OBJ.table_name 

CROSS JOIN (
    SELECT 
        'stg_campaign_interactions' AS table_name,
        COALESCE(
            (SELECT MAX(max_updated_time)
             FROM CTE 
             WHERE table_name = 'stg_campaign_interactions'),
            TIMESTAMP '1900-01-01'
        ) AS dt
) CAM;
; 


-- delete all records from today
DELETE FROM reporting.stg_objective_interactions A 
USING reporting.temp_variables_obj b 
WHERE A.source = b.table_name AND  A._updated::DATE = B.today_run_max::DATE  ;


/************ STEP 1 - CREATING OBJECTIVE FILES STACKED FROM DIFFERENT SOURCES ****************/ 
DROP TABLE IF EXISTS reporting.temp_aa_objectives; 
CREATE TABLE IF NOT EXISTS reporting.temp_aa_objectives AS
WITH CTE_obj_aa AS (
  SELECT
    _updated
    ,date_range_day
    ,hashed_mobile_1_id_evar100 
    ,hashed_email_evar101 
    ,hashed_cif_evar103 
    ,_filename 
 FROM dw."CDP_OBJ_BOQ_v1"
  WHERE _updated::DATE > (SELECT last_run_max FROM reporting.temp_variables_obj WHERE table_name = 'AA')::DATE
  AND _updated::DATE <= (SELECT DISTINCT today_run_max FROM reporting.temp_variables_obj WHERE table_name = 'AA')::DATE

  UNION ALL

  SELECT
    _updated
    ,date_range_day
    ,hashed_mobile_1_id_evar100 
    ,hashed_email_evar101 
    ,hashed_cif_evar103 
    ,_filename 
   FROM dw."CDP_OBJ_MEB_v1"
  WHERE _updated::DATE > (SELECT last_run_max FROM reporting.temp_variables_obj WHERE table_name = 'AA')::DATE
  AND _updated::DATE <= (SELECT DISTINCT today_run_max FROM reporting.temp_variables_obj WHERE table_name = 'AA')::DATE
)
,CTE_obj_aa1 AS ( 
SELECT 
    crt._updated
    ,crt.date_range_day
    ,crt.hashed_mobile_1_id_evar100 
    ,crt.hashed_email_evar101 
    ,crt.hashed_cif_evar103 
    ,TO_DATE(
      '20' || SUBSTR(crt.date_range_day, 2, 2) ||
      LPAD((CAST(SUBSTR(crt.date_range_day, 4, 2) AS INT) + 1)::TEXT, 2, '0') ||
      SUBSTR(crt.date_range_day, 6, 2),
      'YYYYMMDD'
    )                                                                       AS objective_met_date
    ,CASE -- temp
        WHEN (string_to_array(crt._filename, '_'))[4] = 'FirstUsedApp' THEN 'OBJ5'
        WHEN (string_to_array(crt._filename, '_'))[4] LIKE '%HomeLoanEnquiry%' THEN 'OBJ6'
        ELSE NULL
        END::VARCHAR                                                        AS objective_id
    ,(string_to_array(crt._filename, '_'))[3]                               AS brand 
    FROM CTE_obj_aa crt
) 
,CTE_obj_aa2 AS ( 
SELECT 
    crt1._updated
    ,COALESCE(mobile1.customer_id, mobile2.customer_id, email.customer_id, cif.customer_id)   AS customer_id 
    ,crt1.objective_met_date
    ,crt1.objective_id
    ,crt1.brand 
    ,mobile1.contact_history_updated                                       AS mobile1_updated 
    ,mobile2.contact_history_updated                                       AS mobile2_updated 
    ,email.contact_history_updated                                         AS email_updated 
    ,cif.contact_history_updated                                           AS cif_updated
    FROM CTE_obj_aa1 crt1
    LEFT JOIN reporting.stg_campaign_interactions mobile1 
      ON crt1.hashed_mobile_1_id_evar100 = mobile1.hashed_mobile_phone_v1
      AND crt1.objective_met_date >= mobile1.communication_date  
    LEFT JOIN reporting.stg_campaign_interactions mobile2 
      ON crt1.hashed_mobile_1_id_evar100 = mobile2.hashed_mobile_phone_v2
      AND crt1.objective_met_date >= mobile2.communication_date
    LEFT JOIN reporting.stg_campaign_interactions email 
      ON crt1.hashed_email_evar101 = email.hashed_email_address
      AND crt1.objective_met_date >= email.communication_date    
    LEFT JOIN reporting.stg_campaign_interactions cif
      ON crt1.hashed_cif_evar103 = cif.hashed_cif  
      AND crt1.objective_met_date >= cif.communication_date  
    WHERE COALESCE(crt1.hashed_mobile_1_id_evar100, crt1.hashed_email_evar101, crt1.hashed_cif_evar103) IS NOT NULL
) 
,CTE_obj_aa3 AS ( 
SELECT _updated 
        ,'AA'               AS source 
        ,customer_id 
        ,objective_met_date 
        ,objective_id 
        ,brand 
        ,ROW_NUMBER () OVER (PARTITION BY objective_id, objective_met_date, customer_id, brand ORDER BY GREATEST(mobile1_updated,mobile2_updated, email_updated, cif_updated) DESC ) AS RNK 
    FROM CTE_obj_aa2 
) 
SELECT * 
FROM CTE_obj_aa3 
WHERE rnk = 1 AND customer_id IS NOT NULL AND objective_id IS NOT NULL
;

DROP TABLE IF EXISTS reporting.temp_acs_objectives; 
CREATE TABLE IF NOT EXISTS reporting.temp_acs_objectives AS
  SELECT
    _updated
    ,'ACS'                      AS source 
    ,'OBJ7'::VARCHAR            AS objective_id 
    ,customer_id 
    ,_updated                   AS objective_met_date 
  FROM reporting.stg_campaign_interactions
  WHERE _updated::DATE > (SELECT last_run_max FROM reporting.temp_variables_obj WHERE table_name = 'ACS')::DATE
    AND _updated::DATE <= (SELECT DISTINCT today_run_max FROM reporting.temp_variables_obj)::DATE
    AND unsubscribes > 0 
;

DROP TABLE IF EXISTS reporting.temp_objectives; 
CREATE TABLE IF NOT EXISTS reporting.temp_objectives AS
SELECT 
    _updated
    ,source 
    ,customer_id::VARCHAR
    ,objective_met_date::TIMESTAMP 
    ,objective_id::VARCHAR
    ,brand::VARCHAR 
FROM reporting.temp_aa_objectives
WHERE objective_id IS NOT NULL
; 

INSERT INTO reporting.temp_objectives (
_updated 
,source
,objective_id 
,customer_id 
,objective_met_date 
)
  SELECT
    _updated
    ,source
    ,objective_id 
    ,customer_id 
    ,objective_met_date  
FROM reporting.temp_acs_objectives
WHERE objective_id IS NOT NULL
; 


DROP TABLE  IF EXISTS reporting.temp_t24_objectives; 
CREATE TABLE IF NOT EXISTS reporting.temp_t24_objectives AS
  SELECT
    _updated
    ,TRIM(REPLACE(objective_id, ' ', ''))::VARCHAR  AS objective_id  
    ,customer_id
    ,CASE
	  WHEN objective_met_date IS NULL THEN NULL
	  ELSE objective_met_date::DATE
	END AS objective_met_date
  FROM dw."CDP_OBJ_IDP_BOQ"
  WHERE   _updated::DATE > (SELECT last_run_max FROM reporting.temp_variables_obj WHERE table_name = 'T24')::DATE
    AND _updated::DATE <= (SELECT DISTINCT today_run_max FROM reporting.temp_variables_obj)::DATE
    AND customer_id IS NOT NULL
    AND objective_id IS NOT NULL
    AND objective_met_date IS NOT NULL
;

INSERT INTO reporting.temp_objectives (
_updated 
,source
,objective_id 
,customer_id 
,objective_met_date 
)
  SELECT
    _updated
    ,'T24'          AS source
    ,objective_id 
    ,customer_id 
    ,objective_met_date  
FROM reporting.temp_t24_objectives
; 


/************ STEP 2 - CREATING OBJECTIVE INTERACTIONS ****************/ 
DROP TABLE IF EXISTS reporting.temp_objective_interactions; 
CREATE TABLE IF NOT EXISTS reporting.temp_objective_interactions AS
WITH CTE_ao AS ( 
SELECT
  CURRENT_TIMESTAMP AS _updated
  ,CONCAT_WS(
    '-',
    SCAMPI.campaign_name,
    AO.customer_id,
    AO.objective_id,
    TO_CHAR(AO.objective_met_date, 'YYYYMMDD'),
    SCAMPI.communication_id
  ) AS pk
  ,SCAMPI.customer_id
  ,SCAMPI.campaign_name
  ,SCAMPI.campaign_portfolio
  ,SCAMPI.control_group 
  ,SCAMPI.communication_id
  ,SCAMPI.communication_date
  ,SCAMPI.first_exposure_date
  ,AO.objective_id
  ,AO.objective_met_date
  ,COALESCE(RCO.capture_period, ROBJ.default_capture_period) AS objective_capture_period
  ,COALESCE(RCO.objective_rank, 'Unintended Outcome') AS objective_rank
  ,ROBJ.objective_is_positive
  ,AO.source      
  ,ROW_NUMBER() OVER (PARTITION BY SCAMPI.customer_id, SCAMPI.campaign_name, SCAMPI.touchpoint, AO.objective_id, AO.objective_met_date ORDER BY AO._updated DESC,SCAMPI._updated DESC) AS RNK 
FROM reporting.temp_objectives AO
INNER JOIN reporting.stg_campaign_interactions SCAMPI
  ON AO.customer_id = SCAMPI.customer_id
LEFT JOIN reporting.ref_objective_vw ROBJ
  ON ROBJ.objective_id = AO.objective_id
LEFT JOIN reporting.ref_campaign_vw RCAMP
  ON SCAMPI.campaign_name = RCAMP.campaign_name
LEFT JOIN reporting.ref_campaign_objective_vw RCO
  ON RCO.campaign_id = RCAMP.campaign_id
  AND RCO.objective_id = AO.objective_id
WHERE AO.objective_met_date >= SCAMPI.communication_date
  AND AO.objective_met_date <= (SCAMPI.communication_date + COALESCE(RCO.Capture_period, ROBJ.Default_capture_period)::interval)::date
)
SELECT * FROM CTE_ao WHERE rnk = 1 
;

INSERT INTO reporting.stg_objective_interactions (
  _updated                       
  ,customer_id                   
  ,campaign_name                 
  ,campaign_portfolio            
  ,control_group                 
  ,communication_id              
  ,communication_date            
  ,first_exposure_date           
  ,objective_met_date            
  ,objective_id                  
  ,objective_capture_period      
  ,objective_rank                
  ,objective_is_positive    
  ,source  
  ,pk   
)
SELECT   
    _updated                          
  ,customer_id                   
  ,campaign_name                 
  ,campaign_portfolio            
  ,control_group                 
  ,communication_id              
  ,communication_date            
  ,first_exposure_date           
  ,objective_met_date            
  ,objective_id                  
  ,objective_capture_period      
  ,objective_rank                
  ,objective_is_positive   
  ,source 
  ,pk
FROM reporting.temp_objective_interactions;

INSERT INTO reporting.log_campaign_performance (
  table_name
  ,session_id 
  ,run_timestamp
  ,max_updated_time
  ,rows_inserted
  ,distinct_customers
  ,distinct_communications
)
SELECT
  'stg_objective_interactions'
  ,pg_backend_pid()						AS session_id 
  ,CURRENT_TIMESTAMP
  ,MAX(_updated) 						AS max_updated_time
  ,COUNT(*)
  ,COUNT(DISTINCT customer_id)
  ,COUNT(DISTINCT communication_id)
FROM reporting.temp_objective_interactions
;

INSERT INTO reporting.log_campaign_performance (
  table_name
  ,session_id 
  ,run_timestamp
  ,max_updated_time
  ,rows_inserted
  ,distinct_customers
)
SELECT
  'OBJ-'|| COALESCE(SOURCE, '') || '-' || COALESCE(objective_id, '') AS table_name  
  ,pg_backend_pid()						AS session_id 
  ,CURRENT_TIMESTAMP
  ,MAX(_updated)
  ,COUNT(*)
  ,COUNT(DISTINCT customer_id)
FROM reporting.temp_objectives
GROUP BY 1,2,3
; 

DROP TABLE IF EXISTS reporting.temp_variables_obj; 
DROP TABLE IF EXISTS reporting.temp_aa_objectives;
DROP TABLE IF EXISTS reporting.temp_acs_objectives;
DROP TABLE IF EXISTS reporting.temp_t24_objectives;
DROP TABLE IF EXISTS reporting.temp_objectives;
DROP TABLE IF EXISTS reporting.temp_objective_interactions;

