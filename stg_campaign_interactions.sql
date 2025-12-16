/********************************
-- VERSION 
1.00 - 11/12/2025 
1.01 - 12/12/2025	- 	FIX to ONLY be containing the in-scope campaign to reduce to volumn  
********************************/ 

/******** STAGING TABLE ***********/ 

/*******************************************************/ 
/*** reporting.stg_campaign_interactions **************/ 
/*****************************************************/ 

DROP TABLE IF EXISTS reporting.temp_variables_camp;
CREATE TABLE IF NOT EXISTS reporting.temp_variables_camp AS
SELECT
  COALESCE((
    SELECT MAX(max_updated_time)
    FROM reporting.log_campaign_performance
    WHERE table_name = 'ContactHistory'
      AND run_timestamp::date < CURRENT_DATE
  ), TIMESTAMP '1900-01-01') AS last_run_max,
  CURRENT_TIMESTAMP AS today_run_max;

-- delete all records from last run
DELETE FROM reporting.stg_campaign_interactions A 
USING reporting.temp_variables_camp B 
WHERE A._updated::DATE = B.today_run_max::DATE 
;

/********* COMPONENT I - deriving EDM Channel logic ****************************/ 
DROP TABLE IF EXISTS reporting.temp_ch_delta;
CREATE TABLE IF NOT EXISTS reporting.temp_ch_delta AS
WITH CTE_ch_delta AS ( 
  SELECT
	COALESCE(master_boq.customer_id, master_me.customer_id, master_vma.customer_id, master_boq_can.customer_id)			 										AS customer_id 
	,master_boq.hashed_boq_cif														
	,master_me.hashed_me_cif								
	,master_vma.hashed_vma_cif					
	,master_boq_can.hashed_boq_can
	,COALESCE(master_boq.hashed_mobile_phone_v1, master_me.hashed_mobile_phone_v1, master_vma.hashed_mobile_phone_v1, master_boq_can.hashed_mobile_phone_v1)	AS hashed_mobile_phone_v1
	,COALESCE(master_boq.hashed_mobile_phone_v2, master_me.hashed_mobile_phone_v2, master_vma.hashed_mobile_phone_v2, master_boq_can.hashed_mobile_phone_v2)	AS hashed_mobile_phone_v2
	,COALESCE(master_boq.hashed_email_address, master_me.hashed_email_address, master_vma.hashed_email_address, master_boq_can.hashed_email_address)			AS hashed_email_address
	,CH.hashed_cif 
	,CH.campaign_name
	,CH.brand 
	,CH.control_group  
	,trim(
      array_to_string(
        (string_to_array(delivery_label, '-'))[
          5 :
          array_length(string_to_array(delivery_label, '-'), 1)
        ],
        '-'
      )
    ) 		                                                            AS touchpoint
	,CONCAT(
      	COALESCE(CH.hashed_cif, ''), '-',
      	COALESCE(CH.delivery_label, ''), '-',
		COALESCE(CH.campaign_name, '')
    	) 																AS communication_id
	,TO_TIMESTAMP(CH.delivery_time, 'YYYY-MM-DD HH24:MI:SS') 			AS communication_date
	,CH.delivery_channel
	,CH.delivery_label 
	,CH.opens::INTEGER 
	,CH.clicks::INTEGER
	,CH.unsubscribes::INTEGER 
	,CH.delivery_type 
	,CH.purpose 
	,CH.status 
	,CH._updated 																						AS contact_history_updated 
	,COALESCE(master_boq._updated, master_me._updated, master_vma._updated, master_boq_can._updated)	AS customer_updated
	,ROW_NUMBER() OVER (PARTITION BY CH.delivery_label, CH.hashed_cif, CH.campaign_name 
				ORDER BY CH._updated DESC, COALESCE(master_boq._updated, master_me._updated, master_vma._updated, master_boq_can._updated) DESC) AS RNK  
FROM dw."ContactHistory" CH
	INNER JOIN reporting.temp_variables_camp dt 
		ON ch._updated::DATE > dt.last_run_max 
  		AND ch._updated::DATE <= dt.today_run_max 	
    LEFT JOIN dw."CDPMasterFile" master_boq
      	ON CH.hashed_cif = master_boq.hashed_boq_cif 
    LEFT JOIN dw."CDPMasterFile" master_me
      	ON CH.hashed_cif = master_me.hashed_me_cif 	
    LEFT JOIN dw."CDPMasterFile" master_vma
      	ON CH.hashed_cif = master_vma.hashed_vma_cif 	
    LEFT JOIN dw."CDPMasterFile" master_boq_can
      	ON CH.hashed_cif = master_boq_can.hashed_boq_can
WHERE COALESCE(master_boq.hashed_boq_cif, master_me.hashed_me_cif,master_vma.hashed_vma_cif,master_boq_can.hashed_boq_can) IS NOT NULL 
AND ch.hashed_cif IS NOT NULL 
) 
SELECT * FROM CTE_ch_delta WHERE RNK = 1 
;

DROP TABLE IF EXISTS reporting.temp_contact;
CREATE TABLE IF NOT EXISTS reporting.temp_contact AS
WITH contact AS (
  SELECT
    CURRENT_TIMESTAMP 											AS _updated
    ,CH.customer_id
	,CH.hashed_cif
  	,CH.hashed_boq_cif
  	,CH.hashed_me_cif
	 ,CH.hashed_vma_cif 
	 ,CH.hashed_boq_can 
  	,CH.hashed_mobile_phone_v1
  	,CH.hashed_mobile_phone_v2
  	,CH.hashed_email_address
    ,COALESCE(ct.campaign_name,ch.campaign_name) AS campaign_name 
	 ,REFCAM.campaign_portfolio 
    ,REFCAM.campaign_start_date
    ,CH.brand
    ,CH.control_group
    ,CH.communication_id
    ,CH.communication_date
    ,CH.delivery_channel 										AS channel_name
    ,CH.touchpoint
    ,CH.communication_date										AS first_exposure_date
	,CH.opens 
	,CH.clicks 
	,CH.unsubscribes 
    ,CH.delivery_label 
	,CH.delivery_type 
	,CH.purpose 
	,CH.status 
	,CH.contact_history_updated 
	,CH.customer_updated 
    ,ROW_NUMBER() OVER (
		PARTITION BY CH.hashed_cif, CH.communication_date, CH.delivery_label
		ORDER BY ch.contact_history_updated, ch.customer_updated DESC
		)	AS rnk
    FROM reporting.temp_ch_delta CH
    LEFT JOIN reporting.ref_campaign_touchpoint_vw ct 
        ON UPPER(TRIM(CH.touchpoint)) = UPPER(TRIM(ct.touchpoint)) 
    LEFT JOIN reporting.ref_campaign_vw REFCAM
      	ON COALESCE(ct.campaign_name,ch.campaign_name) = REFCAM.campaign_name 
)
SELECT DISTINCT cont.* 
FROM contact cont 
INNER JOIN reporting.ref_campaign_vw REFCAM 
	ON cont.campaign_name = REFCAM.campaign_name 
WHERE REFCAM.campaign_status = 'A'
	AND cont.rnk = 1;


/********* COMPONENT II - deriving In-app tile's logic ****************************/ 
DROP TABLE IF EXISTS reporting.temp_variables_inapp; 
CREATE TABLE IF NOT EXISTS reporting.temp_variables_inapp AS 
SELECT DISTINCT master.table_name 
      ,COALESCE(MAX (TBL.max_updated_time), '1900-01-01')   AS last_run_max 
      ,CURRENT_TIMESTAMP                                AS today_run_max   
FROM (SELECT DISTINCT array_to_string ((string_to_array(_filename, '_'))[1:4], '_') AS table_name 
        FROM dw."CDP_OBJ_InApp_BOQ_v1"
      UNION ALL 
      SELECT DISTINCT array_to_string ((string_to_array(_filename, '_'))[1:4], '_') AS table_name 
        FROM dw."CDP_OBJ_InApp_MEB_v3"
	) MASTER
INNER JOIN reporting.log_campaign_performance TBL 
  ON MASTER.table_name = TBL.table_name 
WHERE UPPER(MASTER.table_name) LIKE 'CDP_OBJ_INAPP%'
GROUP BY 1,3 
; 

/************ STEP 1 - CREATING InApp Contact FILES STACKED FROM DIFFERENT SOURCES ****************/ 
DROP TABLE IF EXISTS reporting.temp_aa_inapp; 
CREATE TABLE IF NOT EXISTS reporting.temp_aa_inapp AS
WITH CTE_inapp_aa AS (
  SELECT
    _updated
    ,date_range_day
    ,hashed_mobile_1_id_evar100 
    ,target_activities
    ,target_experiences 
    ,(string_to_array(_filename, '_'))[4]                           AS brand 
    ,array_to_string ((string_to_array(_filename, '_'))[1:4], '_')  AS source 
    FROM dw."CDP_OBJ_InApp_BOQ_v1"
    WHERE _updated::DATE > (SELECT MAX(last_run_max) FROM reporting.temp_variables_inapp WHERE UPPER(table_name) LIKE '%BOQ%')::DATE
    AND _updated::DATE <= (SELECT DISTINCT today_run_max FROM reporting.temp_variables_inapp WHERE UPPER(table_name) LIKE '%BOQ%')::DATE
    AND hashed_mobile_1_id_evar100 IS NOT NULL 

  UNION ALL

  SELECT
    _updated
    ,date_range_day
    ,hashed_mobile_1_id_evar100 
    ,target_activities
    ,target_experiences 
    ,(string_to_array(_filename, '_'))[4]                           AS brand 
    ,array_to_string ((string_to_array(_filename, '_'))[1:4], '_')  AS source
    FROM dw."CDP_OBJ_InApp_MEB_v3"
    WHERE _updated::DATE > (SELECT MAX(last_run_max) FROM reporting.temp_variables_inapp WHERE UPPER(table_name) LIKE '%ME%')::DATE
    AND _updated::DATE <= (SELECT DISTINCT today_run_max FROM reporting.temp_variables_inapp WHERE UPPER(table_name) LIKE '%ME%')::DATE
    AND hashed_mobile_1_id_evar100 IS NOT NULL 
) 

,CTE_inapp_aa1 AS ( 
SELECT inapp._updated 
      ,TO_DATE(
      '20' || SUBSTR(inapp.date_range_day, 2, 2) ||
      LPAD((CAST(SUBSTR(inapp.date_range_day, 4, 2) AS INT) + 1)::TEXT, 2, '0') ||
      SUBSTR(inapp.date_range_day, 6, 2),
      'YYYYMMDD'
    )                                                                                                                 AS exposure_date 
    ,inapp.target_activities 
    ,inapp.target_experiences 
    ,inapp.brand 
    ,inapp.source 
    ,COALESCE(master.customer_id , master1.customer_id, master2.customer_id, master3.customer_id)                     AS customer_id 
    ,COALESCE(master.hashed_boq_cif, master1.hashed_boq_cif, master2.hashed_boq_cif, master3.hashed_boq_cif)          AS hashed_boq_cif 
    ,COALESCE(master.hashed_me_cif, master1.hashed_me_cif, master2.hashed_me_cif, master3.hashed_me_cif)              AS hashed_me_cif
    ,COALESCE(master.hashed_vma_cif, master1.hashed_vma_cif, master2.hashed_vma_cif, master3.hashed_vma_cif)          AS hashed_vma_cif 
    ,COALESCE(master.hashed_boq_can, master1.hashed_boq_can, master2.hashed_boq_can, master3.hashed_boq_can)          AS hashed_boq_can 
    ,COALESCE(master.hashed_mobile_phone_v1, master1.hashed_mobile_phone_v1, master2.hashed_mobile_phone_v1, master3.hashed_mobile_phone_v1)  AS hashed_mobile_phone_v1 
    ,COALESCE(master.hashed_mobile_phone_v2, master1.hashed_mobile_phone_v2, master2.hashed_mobile_phone_v2, master3.hashed_mobile_phone_v2)  AS hashed_mobile_phone_v2 
    ,COALESCE(master.hashed_email_address, master1.hashed_email_address, master2.hashed_email_address, master3.hashed_email_address)          AS hashed_email_address  
    ,COALESCE(master._updated, master1._updated, master2._updated, master3._updated)                                  AS customer_updated 
FROM CTE_inapp_aa inapp 
    LEFT JOIN dw."CDPMasterFile" master
      	ON inapp.hashed_mobile_1_id_evar100 = master.hashed_mobile_phone_v1
    LEFT JOIN dw."CDPMasterFile" master1
      	ON inapp.hashed_mobile_1_id_evar100 = master1.hashed_mobile_phone_v1
    LEFT JOIN dw."CDPMasterFile" master2
      	ON inapp.hashed_mobile_1_id_evar100 = master2.hashed_mobile_phone_v2
    LEFT JOIN dw."CDPMasterFile" master3
      	ON inapp.hashed_mobile_1_id_evar100 = master3.hashed_mobile_phone_v2
    WHERE COALESCE(master.hashed_mobile_phone_v1, master1.hashed_mobile_phone_v1, master2.hashed_mobile_phone_v2, master3.hashed_mobile_phone_v2 ) IS NOT NULL 
) 
,CTE_inapp_aa2 AS (
  SELECT CURRENT_TIMESTAMP        AS  _updated 
      ,aa1.customer_id 
      ,COALESCE(CH_BOQ.hashed_cif, CH_me.hashed_cif, CH_vma.hashed_cif, CH_boq_can.hashed_cif)                          AS hashed_cif 
      ,aa1.hashed_boq_cif 
      ,aa1.hashed_me_cif 
      ,aa1.hashed_vma_cif 
      ,aa1.hashed_boq_can 
      ,aa1.hashed_mobile_phone_v1 
      ,aa1.hashed_mobile_phone_v2 
      ,aa1.hashed_email_address 
      ,COALESCE(CH_boq.campaign_name, CH_me.campaign_name, CH_vma.campaign_name, CH_boq_can.campaign_name)              AS campaign_name 
      ,COALESCE(CH_boq.brand, CH_me.brand, CH_vma.brand, CH_boq_can.brand)                                              AS brand 
      ,COALESCE(CH_boq.control_group, CH_me.control_group, CH_vma.control_group, CH_boq_can.control_group )             AS control_group 
      ,COALESCE(CH_boq.delivery_label, CH_me.delivery_label, CH_vma.delivery_label, CH_boq_can.delivery_label)          AS delivery_label 
      ,COALESCE(CH_boq.delivery_time, CH_me.delivery_time, CH_vma.delivery_time, CH_boq_can.delivery_time)              AS delivery_time 
      ,COALESCE(CH_boq.delivery_channel, CH_me.delivery_channel, CH_vma.delivery_channel, CH_boq_can.delivery_channel)  AS delivery_channel 
      ,COALESCE(CH_boq.delivery_type, CH_me.delivery_type, CH_vma.delivery_type, CH_boq_can.delivery_type)              AS delivery_type 
      ,COALESCE(CH_boq.purpose, CH_me.purpose, CH_vma.purpose, CH_boq_can.purpose)                                      AS purpose
      ,COALESCE(CH_boq.status, CH_me.status, CH_vma.status, CH_boq_can.status)                                          AS status
      ,COALESCE(CH_boq._updated, CH_me._updated, CH_VMA._updated, CH_boq_can._updated)                                  AS contact_history_updated 
      ,aa1.customer_updated 
      ,aa1._updated               AS inapp_updated
      ,aa1.source                 AS inapp_source 
      ,aa1.exposure_date 
      ,ROW_NUMBER () OVER (PARTITION BY aa1.exposure_date, aa1.target_activities, aa1.target_experiences, aa1.customer_id  
                ORDER BY aa1._updated DESC, COALESCE(CH_boq._updated, CH_me._updated, CH_VMA._updated, CH_boq_can._updated) DESC, aa1.customer_updated DESC) AS RNK  
  FROM CTE_inapp_aa1 aa1
  LEFT JOIN dw."ContactHistory" CH_boq 
    ON aa1.target_activities = CH_boq.delivery_label 
    AND aa1.hashed_boq_cif = CH_boq.hashed_cif 
  LEFT JOIN dw."ContactHistory" CH_me 
    ON aa1.target_activities = CH_me.delivery_label 
    AND aa1.hashed_me_cif = CH_me.hashed_cif 
  LEFT JOIN dw."ContactHistory" CH_vma 
    ON aa1.target_activities = CH_vma.delivery_label 
    AND aa1.hashed_vma_cif = CH_vma.hashed_cif 
  LEFT JOIN dw."ContactHistory" CH_boq_can
    ON aa1.target_activities = CH_boq_can.delivery_label 
    AND aa1.hashed_vma_cif = CH_boq_can.hashed_cif 
  WHERE COALESCE(CH_boq.delivery_label, CH_me.delivery_label, CH_vma.delivery_label, CH_boq_can.delivery_label) IS NOT NULL 
)

,CTE_inapp_aa3 AS (
SELECT aa2._updated 
      ,aa2.customer_id 
      ,aa2.hashed_cif 
      ,aa2.hashed_boq_cif 
      ,aa2.hashed_me_cif 
      ,aa2.hashed_vma_cif 
      ,aa2.hashed_boq_can 
      ,aa2.hashed_mobile_phone_v1 
      ,aa2.hashed_mobile_phone_v2 
      ,aa2.hashed_email_address 
      ,aa2.campaign_name 
      ,aa2.brand 
      ,aa2.control_group 
      ,CONCAT(
            COALESCE(aa2.hashed_cif, ''), '-',
            COALESCE(aa2.delivery_label, ''), '-',
            COALESCE(aa2.campaign_name, '')
          ) 															AS communication_id
      ,TO_TIMESTAMP(aa2.delivery_time, 'YYYY-MM-DD HH24:MI:SS') 		AS communication_date
      ,aa2.delivery_channel                                         	AS channel_name 
      ,trim(
          array_to_string(
            (string_to_array(aa2.delivery_label, '-'))[
              5 :
              array_length(string_to_array(aa2.delivery_label, '-'), 1)
            ],
            '-'
          )
        ) 		             AS touchpoint
      ,aa2.exposure_date     AS first_exposure_date 
      ,0                     AS opens
      ,0                     AS clicks 
      ,0                     AS unsubscribes  
      ,aa2.delivery_label 
      ,aa2.delivery_type 
      ,aa2.purpose 
      ,aa2.status 
      ,aa2.contact_history_updated 
      ,aa2.customer_updated 
      ,aa2.inapp_updated
      ,aa2.inapp_source 
FROM CTE_inapp_aa2 aa2
WHERE aa2.rnk = 1 
) 
,CTE_inapp_aa4 AS ( 
SELECT DISTINCT 
      aa3._updated 
      ,aa3.customer_id 
      ,aa3.hashed_cif 
      ,aa3.hashed_boq_cif 
      ,aa3.hashed_me_cif 
      ,aa3.hashed_vma_cif 
      ,aa3.hashed_boq_can 
      ,aa3.hashed_mobile_phone_v1 
      ,aa3.hashed_mobile_phone_v2 
      ,aa3.hashed_email_address 
      ,COALESCE(ct.campaign_name, aa3.campaign_name) AS campaign_name  
      ,REFCAM.campaign_portfolio 
      ,REFCAM.campaign_start_date 
      ,aa3.brand 
      ,aa3.control_group 
      ,aa3.communication_id
      ,aa3.communication_date
      ,aa3.channel_name 
      ,aa3.touchpoint
      ,aa3.first_exposure_date 
      ,aa3.opens
      ,aa3.clicks 
      ,aa3.unsubscribes  
      ,aa3.delivery_label 
      ,aa3.delivery_type 
      ,aa3.purpose 
      ,aa3.status 
      ,aa3.contact_history_updated 
      ,aa3.customer_updated 
      ,aa3.inapp_updated
      ,aa3.inapp_source 
FROM CTE_inapp_aa3 aa3
    LEFT JOIN reporting.ref_campaign_touchpoint_vw ct 
        ON UPPER(TRIM(aa3.touchpoint)) = UPPER(TRIM(ct.touchpoint)) 
    LEFT JOIN reporting.ref_campaign_vw REFCAM
      	ON COALESCE(ct.campaign_name,aa3.campaign_name) = REFCAM.campaign_name
) 
SELECT aa4.*
FROM CTE_inapp_aa4 aa4
INNER JOIN reporting.ref_campaign_vw REFCAM 
	ON aa4.campaign_name = REFCAM.campaign_name 
WHERE REFCAM.campaign_status = 'A'
; 

INSERT INTO reporting.stg_campaign_interactions ( 
    _updated                
    ,customer_id      
    ,hashed_cif      
    ,hashed_boq_cif         
    ,hashed_me_cif          
    ,hashed_vma_cif         
    ,hashed_boq_can         
    ,hashed_mobile_phone_v1 
    ,hashed_mobile_phone_v2 
    ,hashed_email_address   
    ,campaign_name          
    ,campaign_portfolio     
    ,campaign_start_date    
    ,brand                  
    ,control_group             
    ,communication_id       
    ,communication_date     
    ,channel_name           
    ,touchpoint             
    ,first_exposure_date    
    ,opens                   
    ,clicks                  
    ,unsubscribes 
    ,delivery_label            
    ,delivery_type           
    ,purpose                 
    ,status         
    ,contact_history_updated 
	,customer_updated        
)
SELECT
    _updated                
    ,customer_id 
    ,hashed_cif                 
    ,hashed_boq_cif         
    ,hashed_me_cif          
    ,hashed_vma_cif         
    ,hashed_boq_can         
    ,hashed_mobile_phone_v1 
    ,hashed_mobile_phone_v2 
    ,hashed_email_address   
    ,campaign_name          
    ,campaign_portfolio     
    ,campaign_start_date    
    ,brand                  
    ,control_group             
    ,communication_id       
    ,communication_date     
    ,channel_name           
    ,touchpoint             
    ,first_exposure_date    
    ,opens                   
    ,clicks                  
    ,unsubscribes    
    ,delivery_label         
    ,delivery_type           
    ,purpose                 
    ,status 
	,contact_history_updated 
	,customer_updated
FROM reporting.temp_contact;


INSERT INTO reporting.stg_campaign_interactions ( 
    _updated                
    ,customer_id      
    ,hashed_cif      
    ,hashed_boq_cif         
    ,hashed_me_cif          
    ,hashed_vma_cif         
    ,hashed_boq_can         
    ,hashed_mobile_phone_v1 
    ,hashed_mobile_phone_v2 
    ,hashed_email_address   
    ,campaign_name          
    ,campaign_portfolio     
    ,campaign_start_date    
    ,brand                  
    ,control_group             
    ,communication_id       
    ,communication_date     
    ,channel_name           
    ,touchpoint             
    ,first_exposure_date    
    ,opens                   
    ,clicks                  
    ,unsubscribes 
    ,delivery_label            
    ,delivery_type           
    ,purpose                 
    ,status         
    ,contact_history_updated 
	,customer_updated        
)
SELECT
    _updated                
    ,customer_id 
    ,hashed_cif                 
    ,hashed_boq_cif         
    ,hashed_me_cif          
    ,hashed_vma_cif         
    ,hashed_boq_can         
    ,hashed_mobile_phone_v1 
    ,hashed_mobile_phone_v2 
    ,hashed_email_address   
    ,campaign_name          
    ,campaign_portfolio     
    ,campaign_start_date    
    ,brand                  
    ,control_group             
    ,communication_id       
    ,communication_date     
    ,channel_name           
    ,touchpoint             
    ,first_exposure_date    
    ,opens                   
    ,clicks                  
    ,unsubscribes    
    ,delivery_label         
    ,delivery_type           
    ,purpose                 
    ,status 
	,contact_history_updated 
	,customer_updated
FROM reporting.temp_aa_inapp;

/* insert campaign interaction's log */ 
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
  'stg_campaign_interactions'			AS table_name
  ,pg_backend_pid()						AS session_id 
  ,CURRENT_TIMESTAMP					AS run_timestamp
  ,MAX(_updated) 						AS max_updated_time
  ,COUNT(*)								AS rows_inserted
  ,COUNT(DISTINCT customer_id)			AS distinct_customers
  ,COUNT(DISTINCT communication_id)		AS distinct_communications
FROM ( SELECT _updated, customer_id, communication_id FROM reporting.temp_contact
        UNION ALL 
        SELECT _updated, customer_id, communication_id FROM reporting.temp_aa_inapp
) 
;

/* insert Contact History's log */ 
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
  'ContactHistory'							AS table_name
  ,pg_backend_pid()							AS session_id 
  ,CURRENT_TIMESTAMP						AS run_timestamp
  ,MAX(contact_history_updated)				AS max_updated_time
  ,COUNT(*)									AS rows_inserted
  ,COUNT(DISTINCT customer_id)				AS distinct_customers
  ,COUNT(DISTINCT MD5(communication_id))	AS distinct_communications
FROM reporting.temp_ch_delta;

/* insert Inapp's log */ 
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
  inapp_source                AS table_name  
  ,pg_backend_pid()			  AS session_id 
  ,CURRENT_TIMESTAMP
  ,MAX(_updated)
  ,COUNT(*)
  ,COUNT(DISTINCT customer_id)
  ,COUNT(DISTINCT communication_id)
FROM reporting.temp_aa_inapp
GROUP BY 1,2,3
; 

DROP TABLE IF EXISTS reporting.temp_variables_camp;
DROP TABLE IF EXISTS reporting.temp_contact;
DROP TABLE IF EXISTS reporting.temp_ch_delta;
DROP TABLE IF EXISTS reporting.temp_variables_inapp; 
DROP TABLE IF EXISTS reporting.temp_aa_inapp; 
