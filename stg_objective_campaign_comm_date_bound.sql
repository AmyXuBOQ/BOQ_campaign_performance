
/*******************************************************/ 
/*** reporting.stg_objective_campaign_comm_date_bound */ 
/*****************************************************/ 

DROP TABLE IF EXISTS reporting.temp_stg_objective_campaign_comm_date_bound; 
CREATE TABLE IF NOT EXISTS reporting.temp_stg_objective_campaign_comm_date_bound AS 
WITH obj_cam_cap AS ( 
/* this is for any objective that can be associated to a campaign */ 
SELECT 	'Intended outcome' 		AS section 
		,campaign_objective_id 
		,campaign_id 
		,objective_id 
		,capture_period 			AS capture_period 
FROM reporting.ref_campaign_objective_vw 
UNION ALL
SELECT 	'Unintended outcome' 	AS  section 
		,NULL::VARCHAR			AS  campaign_objective_id 
		,NULL::VARCHAR			AS	campaign_id 
		,objective_id 
		,default_capture_period 	AS capture_period 
FROM reporting.ref_objective_vw
) 
,obj_cam_cap2 AS (
SELECT section 
		,campaign_objective_id 
		,campaign_id 
		,objective_id
		,'Compete cohort' 									AS cohort_type 
		,capture_period 
		,current_date - capture_period - '1 year'::interval	AS lower_bound_comm_date 
		,current_date - capture_period 						AS upper_bound_comm_date 
		FROM obj_cam_cap
UNION ALL
SELECT section 
		,campaign_objective_id 
		,campaign_id 
		,objective_id
		,'Incompete cohort' 					AS cohort_type 
		,capture_period 
		,current_date - capture_period			AS lower_bound_comm_date 
		,current_date 							AS upper_bound_comm_date 
		FROM obj_cam_cap
) 
SELECT CURRENT_TIMESTAMP                AS _updated
        ,section	 					AS section 
		,campaign_objective_id			AS campaign_objective_id 
		,campaign_id 					AS campaign_id 
		,objective_id					AS objective_id 
		,cohort_type					AS cohort_type 
		,capture_period 				AS capture_period 
		,lower_bound_comm_date::DATE		AS lower_bound_comm_date
		,upper_bound_comm_date::DATE		AS upper_bound_comm_date 
FROM obj_cam_cap2 A 
; 

TRUNCATE TABLE reporting.stg_objective_campaign_comm_date_bound; 

INSERT INTO  reporting.stg_objective_campaign_comm_date_bound 
(
	_updated
    ,section 				
	,campaign_objective_id	
	,campaign_id			
	,objective_id			
	,cohort_type			 				
	,capture_period 				
	,lower_bound_comm_date 	
	,upper_bound_comm_date 	 
)
SELECT 	_updated 
        ,section 				
		,campaign_objective_id	
		,campaign_id			
		,objective_id			
		,cohort_type			 				
		,capture_period 				
		,lower_bound_comm_date 	
		,upper_bound_comm_date 	
FROM reporting.temp_stg_objective_campaign_comm_date_bound 
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
  'stg_objective_campaign_comm_date_bound'  AS table_name
  ,pg_backend_pid()							AS session_id 
  ,MAX(_updated)                            AS max_updated_time  
  ,CURRENT_TIMESTAMP                        AS run_timestamp
  ,COUNT(*)                                 AS rows_inserted
  ,NULL::integer                            AS distinct_customers
  ,NULL::integer                            AS distinct_communications
FROM reporting.temp_stg_objective_campaign_comm_date_bound;

DROP TABLE IF EXISTS reporting.temp_stg_objective_campaign_comm_date_bound;
