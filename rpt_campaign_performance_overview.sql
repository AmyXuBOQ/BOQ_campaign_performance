DROP TABLE IF EXISTS reporting.temp_rpt_campaign_performance_overview; 
CREATE TABLE reporting.temp_rpt_campaign_performance_overview AS 
WITH CTE0 AS (
SELECT * 
FROM reporting.rpt_campaign_objective_interactions  
	WHERE nbr = '4c' 
	AND cohort = 'Complete Cohort'
	AND objective_id IS NOT NULL 
	AND campaign_id IS NOT NULL 
)
,CTE AS (
		SELECT cam.campaign_id 
				,cam_obj.objective_id 
				,cam_obj.rt_convert_cust_i 										AS intervention_conversion_rate
				,cam_obj.relative_conversion_rate
				,cam_obj.incremental_uplift 
				,cam_obj.incremental_uplift_num 
				,cam_obj.annualised_uplift_num 
				,cam_obj.incremental_uplift / nullif (sqrt ( cam_obj.rt_convert_cust * (1 - cam_obj.rt_convert_cust) * ( coalesce(1 / nullif(cam.cntd_cust_i,0),0)  + coalesce(1 / nullif(cam.cntd_cust_c,0),0))),0)	AS test_stats 
FROM CTE0 cam_obj					
INNER JOIN ( SELECT DISTINCT campaign_id
							, cntd_cust_i
							, cntd_cust_c 
						FROM reporting.rpt_campaign_objective_interactions 
						WHERE nbr = '3b' and cohort = 'Complete Cohort') cam 
	ON COALESCE (cam_obj.campaign_id, '') = COALESCE(cam.campaign_id, '')
)
,CTE2 AS ( 
SELECT DISTINCT master_cam.campaign_id 
			,cam_obj.objective_id 
			,coalesce(master_cam.campaign_name,  agg1.campaign_name) 			AS campaign_name  
			,master_cam.campaign_brand
			,coalesce(master_cam.campaign_portfolio, agg1.campaign_portfolio) 	AS campaign_portfolio  
			,COALESCE(agg1.objective_name, obj.objective_name) 					AS objective_name 
			,COALESCE(agg1.objective_rank, cam_obj.objective_rank) 			AS objective_rank  
			,True::boolean 											  		AS intended 
			,master_cam.cntd_cust 											AS total_customers_per_campaign  
			,master_cam.cntd_cust_i 										AS total_customers_per_group
			,CASE WHEN agg1.campaign_id IS NOT NULL THEN agg1.relative_conversion_rate_indicator
				ELSE 'No complete cohort' END::VARCHAR						AS relative_conversion_rate_indicator 
FROM reporting.rpt_campaign_objective_interactions master_cam 
LEFT JOIN reporting.ref_campaign_objective_vw cam_obj 
	ON master_cam.campaign_id = cam_obj.campaign_id 
LEFT JOIN reporting.ref_objective_vw obj 
	ON cam_obj.objective_id = obj.objective_id
LEFT JOIN CTE0 agg1 
	ON master_cam.campaign_id = agg1.campaign_id 
	AND cam_obj.objective_id = agg1.objective_id 
	AND agg1.objective_rank != 'Unintended Outcome' 
WHERE master_cam.nbr = '2a'
UNION ALL 
SELECT DISTINCT agg.campaign_id 
			,agg.objective_id 
			,agg.campaign_name 
			,agg.campaign_brand
			,agg.campaign_portfolio 
			,agg.objective_name 
			,agg.objective_rank 
			,FALSE::boolean 									AS intended 
			,0													AS total_customers_per_campaign  
			,0													AS total_customers_per_group 
			,relative_conversion_rate_indicator 
FROM cte0  agg 
WHERE agg.objective_rank = 'Unintended Outcome' 
)
SELECT DISTINCT 
		CURRENT_TIMESTAMP 												AS _updated 
		,cam.campaign_id 
		,cam.campaign_name 
		,cam.campaign_brand 
		,cam.campaign_portfolio
		,cam.objective_name												AS objective_name  
		,cam.objective_rank												AS objective_type 
		,CASE WHEN cam.campaign_portfolio IS NOT NULL AND cam.objective_name IS NOT NULL 
					THEN cam.campaign_portfolio || ' - ' || cam.objective_name
				WHEN cam.objective_name IS NOT NULL THEN cam.objective_name 
				WHEN cam.campaign_portfolio IS NOT NULL THEN cam.campaign_portfolio 
				ELSE NULL END::VARCHAR 						 											AS product_objective 		
		,CASE WHEN cam.objective_rank IS NOT NULL AND cam.objective_name IS NOT NULL 
					THEN cam.objective_rank || ' - ' || cam.objective_name
				ELSE NULL END::VARCHAR																	AS type_objective		
		,cam.intended 								
		,cam.total_customers_per_campaign  			
		,cam.total_customers_per_group 				
		,cte.intervention_conversion_rate			
		,cte.relative_conversion_rate				 
		,cam.relative_conversion_rate_indicator 		
		,cte.incremental_uplift 					
		,cte.incremental_uplift_num 				
		,cte.annualised_uplift_num 					
		,ROUND(cte.test_stats::NUMERIC,3) AS test_stats			
		,0::NUMERIC 																				 	AS mean 
		,1::NUMERIC																						AS standard_dev 					
FROM CTE2 cam 
LEFT JOIN CTE
	ON COALESCE (cam.campaign_id, '') = COALESCE(cte.campaign_id, '')
	AND COALESCE (cam.objective_id, '') = COALESCE(cte.objective_id, '')
;

TRUNCATE TABLE reporting.rpt_campaign_performance_overview; 

INSERT INTO reporting.rpt_campaign_performance_overview (
	_updated 	
	,campaign_id 							
	,campaign_name 		
	,campaign_brand 					
	,campaign_portfolio						
	,objective_name  						
	,objective_type 						
	,product_objective		
	,type_objective				
	,intended 								
	,total_customers_per_campaign  			
	,total_customers_per_group 				
	,intervention_conversion_rate			
	,relative_conversion_rate				 
	,relative_conversion_rate_indicator 		
	,incremental_uplift 					
	,incremental_uplift_num 				
	,annualised_uplift_num 					
	,test_stats 		
	,mean 
	,standard_dev 					
)
SELECT 	_updated 	
		,campaign_id 							
		,campaign_name 		
		,campaign_brand 					
		,campaign_portfolio						
		,objective_name  						
		,objective_type 						
		,product_objective	
		,type_objective					
		,intended 								
		,total_customers_per_campaign  			
		,total_customers_per_group 				
		,intervention_conversion_rate			
		,relative_conversion_rate				 
		,relative_conversion_rate_indicator 		
		,incremental_uplift 					
		,incremental_uplift_num 				
		,annualised_uplift_num 					
		,test_stats 
		,mean 
		,standard_dev 	
FROM reporting.temp_rpt_campaign_performance_overview
; 

INSERT INTO reporting.log_campaign_performance (
  table_name
  ,session_id 
  ,max_updated_time
  ,run_timestamp
  ,rows_inserted
)
SELECT
  'rpt_campaign_performance_overview'     	AS table_name
  ,pg_backend_pid()							AS session_id 
  ,MAX(_updated)                            AS max_updated_time                   
  ,CURRENT_TIMESTAMP                        AS run_timestamp
  ,COUNT(*)                                 AS rows_inserted
FROM reporting.temp_rpt_campaign_performance_overview
;

DROP TABLE IF EXISTS reporting.temp_rpt_campaign_performance_overview; 
