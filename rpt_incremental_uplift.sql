
/*************************************************************************/ 
/******** reporting.rpt_incremental_uplift_complete *********************/ 
/***********************************************************************/

DROP TABLE IF EXISTS reporting.temp_rpt_incremental_uplift; 
CREATE TABLE reporting.temp_rpt_incremental_uplift AS 
WITH CTE AS ( 
SELECT CURRENT_TIMESTAMP		        AS _updated
        ,coi.performance_measurement_date	
        ,CAST ('Daily' AS VARCHAR) 		AS cal_type	
        ,coi.campaign_id						
        ,coi.campaign_name						
        ,coi.campaign_brand						
        ,coi.campaign_portfolio					
        ,coi.objective_name						
		,coi.objective_rank												                        AS objective_type 
		,coi.product_objective 		
		,coi.type_objective 
        ,coi.incremental_uplift 
        ,coi.incremental_uplift                                                                 AS incremental_uplift_complete_total 
        ,CASE WHEN coi.nbr = '4d' AND coi.performance_measurement_date::DATE >= (CURRENT_DATE - cam_obj.capture_period)  THEN coi.incremental_uplift ELSE NULL END   AS incremental_uplift_total 
        ,CASE WHEN coi.performance_measurement_date::DATE = (CURRENT_DATE - cam_obj.capture_period)::DATE 
                    THEN coi.performance_measurement_date 
                ELSE NULL END::DATE                                                             AS date_total   
        ,coi.relative_conversion_rate 
	    ,coi.relative_conversion_rate_indicator    
        ,coi.incremental_uplift_num				
        ,coi.annualised_uplift_num				
        ,ROW_NUMBER() OVER (PARTITION BY coi.performance_measurement_date, coi.campaign_id, coi.objective_name, coi.objective_rank 
                                ORDER BY CASE WHEN coi.nbr = '5c' THEN 1 
                                                WHEN coi.nbr = '4d' THEN 2 
                                                ELSE 99 END ASC
									,coi.incremental_uplift DESC) AS RNK                                              
FROM reporting.rpt_campaign_objective_interactions coi
INNER JOIN reporting.ref_campaign_vw cam 
    ON coi.campaign_id = cam.campaign_id 
INNER JOIN reporting.ref_campaign_objective_vw cam_obj 
    ON coi.campaign_id = cam_obj.campaign_id 
    AND coi.objective_id = cam_obj.objective_id 
WHERE  ( coi.nbr = '4d' AND coi.performance_measurement_date::DATE >= (CURRENT_DATE - cam_obj.capture_period))
 OR ( coi.nbr = '5c' AND UPPER(coi.cohort) = 'COMPLETE COHORT'  AND coi.performance_measurement_date::DATE < (CURRENT_DATE - cam_obj.capture_period))
	)
SELECT * FROM CTE WHERE RNK = 1
;

TRUNCATE TABLE reporting.rpt_incremental_uplift; 

INSERT INTO reporting.rpt_incremental_uplift ( 
_updated							
,performance_measurement_date		
,cal_type							
,campaign_id						
,campaign_name						
,campaign_brand						
,campaign_portfolio					
,objective_name						
,objective_type 					
,product_objective					
,type_objective						
,incremental_uplift 				
,incremental_uplift_complete_total										
,relative_conversion_rate			
,relative_conversion_rate_indicator	
,incremental_uplift_num				
,annualised_uplift_num				
,test_stats							
)
SELECT CURRENT_TIMESTAMP		AS _updated							
    ,agg._updated               AS performance_measurement_date		
    ,CAST ('Aggregation' AS VARCHAR) 		AS cal_type							
    ,agg.campaign_id						
    ,agg.campaign_name						
    ,agg.campaign_brand						
    ,agg.campaign_portfolio					
    ,agg.objective_name						
    ,agg.objective_type 					
    ,agg.product_objective					
    ,agg.type_objective						
    ,agg.incremental_uplift 				
    ,agg.incremental_uplift                 AS incremental_uplift_complete_total									
    ,agg.relative_conversion_rate			
    ,agg.relative_conversion_rate_indicator	
    ,agg.incremental_uplift_num				
    ,agg.annualised_uplift_num				
    ,agg.test_stats							
FROM reporting.rpt_campaign_performance_overview agg
WHERE intended is True 
AND incremental_uplift IS NOT NULL 
; 

INSERT INTO reporting.rpt_incremental_uplift ( 
    _updated							
    ,performance_measurement_date		
    ,cal_type							
    ,campaign_id						
    ,campaign_name						
    ,campaign_brand						
    ,campaign_portfolio					
    ,objective_name						
    ,objective_type 					
    ,product_objective					
    ,type_objective						
    ,incremental_uplift 				
    ,incremental_uplift_complete_total		
    ,incremental_uplift_total			
    ,date_total							
    ,relative_conversion_rate			
    ,relative_conversion_rate_indicator	
    ,incremental_uplift_num				
    ,annualised_uplift_num				
    ,test_stats							
)
SELECT _updated							
    ,performance_measurement_date		
    ,cal_type							
    ,campaign_id						
    ,campaign_name						
    ,campaign_brand						
    ,campaign_portfolio					
    ,objective_name						
    ,objective_type 					
    ,product_objective					
    ,type_objective						
    ,incremental_uplift 				
    ,incremental_uplift_complete_total		
    ,incremental_uplift_total			
    ,date_total							
    ,relative_conversion_rate			
    ,relative_conversion_rate_indicator	
    ,incremental_uplift_num				
    ,annualised_uplift_num	
    ,NULL::NUMERIC        AS test_stats		
FROM reporting.temp_rpt_incremental_uplift
WHERE rnk=1 
;  

INSERT INTO reporting.log_campaign_performance (
  table_name
  ,session_id
  ,max_updated_time
  ,run_timestamp
  ,rows_inserted
)
SELECT
  'rpt_incremental_uplift'     	            AS table_name
  ,pg_backend_pid()							AS session_id 
  ,MAX(_updated)                            AS max_updated_time                   
  ,CURRENT_TIMESTAMP                        AS run_timestamp
  ,COUNT(*)                                 AS rows_inserted
FROM reporting.rpt_incremental_uplift
;

DROP TABLE IF EXISTS reporting.temp_rpt_incremental_uplift; 
