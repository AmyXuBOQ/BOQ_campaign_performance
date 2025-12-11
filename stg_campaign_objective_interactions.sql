
/******************************************************************/ 
/************** reporting.stg_campaign_objective_interactions ****/ 
/****************************************************************/ 

DROP TABLE IF EXISTS reporting.temp_stg_campaign_objective_interactions; 
CREATE TABLE IF NOT EXISTS reporting.temp_stg_campaign_objective_interactions AS 
WITH CTE AS ( 
SELECT 	CURRENT_TIMESTAMP 			AS _updated
		,ref_cam.campaign_id 
        ,cam.customer_id 
		,cam.brand
		,cam.campaign_name 
		,cam.campaign_portfolio 
		,cam.communication_id 
		,cam.communication_date 
		,CASE WHEN cam.control_group = '0' THEN 'I' ELSE 'C' END 		AS customer_group 
		,cam.channel_name 
		,cam.touchpoint
		,cam.first_exposure_date
		,obj.objective_id
		,obj.objective_rank 
		,obj.objective_is_positive
		,obj.objective_met_date
		,CASE /* for the case where we have objective met for this communcation / select */  
			WHEN cam.communication_date >= roccdb_comp.lower_bound_comm_date AND cam.communication_date < roccdb_comp.upper_bound_comm_date THEN 'Complete Cohort'
			WHEN cam.communication_date >= roccdb_incomp.lower_bound_comm_date AND cam.communication_date < roccdb_incomp.upper_bound_comm_date THEN 'Incomplete Cohort'
			/* if there is no objective met for this communication / select - then use the max default period across the campaign to decide this communication fall in complete or incomplete */ 
			WHEN cam.communication_date >= roccdb_no_comp.lower_bound_comm_date AND cam.communication_date < roccdb_no_comp.upper_bound_comm_date THEN 'Complete Cohort' 
			WHEN cam.communication_date >= roccdb_no_incomp.lower_bound_comm_date AND cam.communication_date < roccdb_no_incomp.upper_bound_comm_date THEN 'Incomplete Cohort' 
			/* catch all bucket */ 
			WHEN cam.communication_date < roccdb_no_comp.lower_bound_comm_date THEN 'Complete Cohort' 
			WHEN cam.communication_date > roccdb_no_incomp.upper_bound_comm_date THEN 'Incomplete Cohort' 
			ELSE NULL END 			                                                                                                                                    AS cohort -- obtain if the comms is complete OR incomplete cohort 
		,COALESCE (roccdb_comp.capture_period, cp.max_capture_period)::interval 																						AS capture_period -- capture period is at objective level, hence same in roccdb_comp and roccdb_incomp
		,CASE WHEN obj.objective_met_date >= coalesce(cam.first_exposure_date, '9999-01-01') THEN 1 ELSE 0 END::boolean													AS objective_conv_after_contact 		
		,ROW_NUMBER () OVER (PARTITION BY cam.communication_id, cam.customer_id, obj.objective_id, cam.campaign_name  ORDER BY cam._updated DESC, obj._updated DESC) 	AS RNK 
FROM reporting.stg_campaign_interactions cam
INNER JOIN reporting.ref_campaign_vw ref_cam
	ON cam.campaign_name = ref_cam.campaign_name 
	AND ref_cam.campaign_status = 'A'
LEFT JOIN reporting.stg_objective_interactions obj
	ON cam.campaign_name = obj.campaign_name 
	AND cam.communication_id = obj.communication_id 
	AND cam.customer_id = obj.customer_id 
LEFT JOIN ( SELECT CAMPAIGN_ID
					, MAX( CAST (capture_period AS INTERVAL) ) AS max_capture_period 
			FROM reporting.ref_campaign_objective_vw
			GROUP BY 1 
			) cp 
	ON ref_cam.campaign_id = cp.campaign_id
LEFT JOIN ( SELECT DISTINCT campaign_id
							, objective_id 
							, CAST (capture_period AS INTERVAL) AS capture_period
							, lower_bound_comm_date
							, upper_bound_comm_date 
			FROM reporting.stg_objective_campaign_comm_date_bound 
			WHERE section = 'Intended outcome'
				AND cohort_type = 'Compete cohort') roccdb_comp 
	ON ref_cam.campaign_id = roccdb_comp.campaign_id 	
	AND obj.objective_id = roccdb_comp.objective_id 
LEFT JOIN ( SELECT DISTINCT campaign_id
							, objective_id 
							, CAST (capture_period AS INTERVAL) AS capture_period
							, lower_bound_comm_date
							, upper_bound_comm_date 
			FROM reporting.stg_objective_campaign_comm_date_bound
			WHERE section = 'Intended outcome'
				AND cohort_type = 'Incompete cohort') roccdb_incomp 
	ON ref_cam.campaign_id = roccdb_incomp.campaign_id 	
	AND obj.objective_id = roccdb_incomp.objective_id 
LEFT JOIN ( SELECT DISTINCT campaign_id
							, MAX (CAST (capture_period AS INTERVAL))	AS capture_period
							, MIN (lower_bound_comm_date)				AS lower_bound_comm_date
							, MAX (upper_bound_comm_date )				AS upper_bound_comm_date 
			FROM reporting.stg_objective_campaign_comm_date_bound 
			WHERE campaign_id is not null -- filter out the default objective section of the code 
				AND cohort_type = 'Compete cohort'
			GROUP BY 1
			) roccdb_no_comp 
	ON ref_cam.campaign_id = roccdb_no_comp.campaign_id 
	AND cp.max_capture_period = roccdb_no_comp.capture_period 
LEFT JOIN ( SELECT DISTINCT campaign_id
							, MAX ( CAST (capture_period AS INTERVAL))	AS capture_period
							, MIN (lower_bound_comm_date)				AS lower_bound_comm_date
							, MAX (upper_bound_comm_date )				AS upper_bound_comm_date 
			FROM reporting.stg_objective_campaign_comm_date_bound 
			WHERE campaign_id is not null -- filter out the default objective section of the code 
				AND cohort_type = 'Incompete cohort'
			GROUP BY 1
			) roccdb_no_incomp
	ON ref_cam.campaign_id = roccdb_no_incomp.campaign_id 
	AND cp.max_capture_period = roccdb_no_incomp.capture_period 	
WHERE ( obj.objective_met_date is NULL OR obj.objective_met_date >= cam.communication_date ) 
	AND cam.communication_date + COALESCE (roccdb_comp.capture_period, cp.max_capture_period)::interval  >= current_date + '- 1 year'::interval 
) 
SELECT * FROM CTE WHERE RNK = 1 
;

TRUNCATE TABLE reporting.stg_campaign_objective_interactions; 

INSERT INTO reporting.stg_campaign_objective_interactions ( 
	_updated
	,campaign_id 
	,campaign_name 
	,campaign_portfolio 
    ,customer_id 
	,brand
	,communication_id 
	,communication_date 
	,customer_group 
	,channel_name 
	,touchpoint
	,first_exposure_date
	,objective_id
	,objective_rank 
	,objective_is_positive
	,objective_met_date
	,cohort
	,capture_period 
	,objective_conv_after_contact 							
)
SELECT 	
	_updated
	,campaign_id 
	,campaign_name 
	,campaign_portfolio 
    ,customer_id 
	,brand
	,communication_id 
	,communication_date 
	,customer_group 
	,channel_name 
	,touchpoint
	,first_exposure_date
	,objective_id
	,objective_rank 
	,objective_is_positive
	,objective_met_date
	,cohort
	,capture_period 
	,objective_conv_after_contact 		
FROM reporting.temp_stg_campaign_objective_interactions
; 

INSERT INTO reporting.log_campaign_performance (
  table_name
  ,session_id
  ,max_updated_time
  ,run_timestamp
  ,rows_inserted
  ,distinct_customers
  ,distinct_communications
)
SELECT
  'stg_campaign_objective_interactions'     AS table_name
  ,pg_backend_pid()							AS session_id 
  ,MAX(_updated)                            AS max_updated_time                   
  ,CURRENT_TIMESTAMP                        AS run_timestamp
  ,COUNT(*)                                 AS rows_inserted
  ,COUNT(DISTINCT customer_id)              AS distinct_customers
  ,COUNT(DISTINCT communication_id)         AS distinct_communications
FROM reporting.temp_stg_campaign_objective_interactions;

DROP TABLE IF EXISTS reporting.temp_stg_campaign_objective_interactions; 
