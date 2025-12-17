
/*************************************************************/ 
/******** reporting.rpt_campaign_drilldown ******************/ 
/***********************************************************/

DROP TABLE IF EXISTS reporting.temp_rpt_campaign_drilldown; 
CREATE TABLE reporting.temp_rpt_campaign_drilldown AS 
WITH cte_4a AS ( 
SELECT campaign_id
		, campaign_name
		, campaign_brand 
		, campaign_portfolio
		, channel
		, action
		, touchpoint
		, agg.cntd_comm_i		 	AS communication_sent_num
		, agg.cntd_cust_i		 	AS cust_communication_sent_num 
		, agg.cntd_comm_contact		AS communication_contacted_num 
		, agg.cntd_cust_contact		AS cust_contacted_num 
FROM reporting.rpt_campaign_objective_interactions  agg 
WHERE agg.nbr = '4a' 
	AND touchpoint IS NOT NULL 
	AND channel IS NOT NULL 
)
,cte_5b AS ( 
SELECT campaign_id
		, campaign_name
		, campaign_portfolio
		, channel
		, touchpoint
		, action
		, objective_name 
		, objective_rank 
		, cntd_comm_convert_after_contact 	AS conv_communication_after_contact_num 
		, cntd_cust_convert_after_contact	AS conv_cust_after_contact_num 
FROM reporting.rpt_campaign_objective_interactions  agg 
WHERE agg.nbr = '5b' 
	AND objective_name IS NOT NULL 
)
SELECT DISTINCT CURRENT_TIMESTAMP 							AS _updated 
		, a.campaign_id
		, a.campaign_name
		, a.campaign_brand 
		, a.campaign_portfolio
		, a.channel
		, a.action
		, a.touchpoint
		, b.objective_name 
		, CASE WHEN a.campaign_portfolio IS NOT NULL AND b.objective_name IS NOT NULL THEN a.campaign_portfolio || ' - ' ||b.objective_name 
				WHEN b.objective_name IS NOT NULL THEN b.objective_name 
				ELSE NULL END::VARCHAR 				AS product_objective 
		, a.communication_sent_num 
		, a.cust_communication_sent_num 
		, a.communication_contacted_num 
		, a.cust_contacted_num 
		, COALESCE(b.conv_communication_after_contact_num, 0) AS conv_communication_after_contact_num
		, COALESCE(b.conv_cust_after_contact_num, 0) AS conv_cust_after_contact_num
FROM cte_4a a 
LEFT JOIN cte_5b b 
ON a.campaign_id = b.campaign_id 
AND a.touchpoint = b.touchpoint 
AND a.channel = b.channel 
;

TRUNCATE TABLE reporting.rpt_campaign_drilldown; 

INSERT INTO reporting.rpt_campaign_drilldown (
_updated                         
, campaign_id                   
, campaign_name          
, campaign_brand        
, campaign_portfolio            
, channel                       
, action                        
, touchpoint                    
, objective_name                
, product_objective              
, communication_sent_num                 
, cust_communication_sent_num            
, communication_contacted_num            
, cust_contacted_num             
, conv_communication_after_contact_num   
, conv_cust_after_contact_num   					
)
SELECT 	_updated                         
		, campaign_id                   
		, campaign_name  
		, campaign_brand                
		, campaign_portfolio            
		, channel                       
		, action                        
		, touchpoint                    
		, objective_name                
		, product_objective              
		, communication_sent_num                 
		, cust_communication_sent_num            
		, communication_contacted_num            
		, cust_contacted_num             
		, conv_communication_after_contact_num   
		, conv_cust_after_contact_num  
FROM reporting.temp_rpt_campaign_drilldown
; 

INSERT INTO reporting.log_campaign_performance (
  table_name
  ,session_id
  ,max_updated_time
  ,run_timestamp
  ,rows_inserted
)
SELECT
  'rpt_campaign_drilldown'			     	AS table_name
  ,pg_backend_pid()							AS session_id 
  ,MAX(_updated)                            AS max_updated_time                   
  ,CURRENT_TIMESTAMP                        AS run_timestamp
  ,COUNT(*)                                 AS rows_inserted
FROM reporting.temp_rpt_campaign_drilldown
;

DROP TABLE IF EXISTS reporting.temp_rpt_campaign_drilldown
; 
