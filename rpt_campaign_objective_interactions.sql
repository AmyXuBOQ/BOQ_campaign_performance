/*****************************************************************/ 
/******** reporting.rpt_campaign_objective_interactions *********/ 
/***************************************************************/

/* 'campaign-portfolio' */ 
DROP TABLE IF EXISTS reporting.temp_rpt_agg_2a; 
CREATE TABLE reporting.temp_rpt_agg_2a AS 
WITH CTE1 AS ( 
SELECT campaign_id 
		,brand 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
		,COUNT (DISTINCT communication_date) AS cntd_cohort 
FROM reporting.stg_campaign_objective_interactions
GROUP BY 1,2
		)
,CTE2 AS ( 
SELECT campaign_id , brand 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE first_exposure_date IS NOT NULL 
GROUP BY 1,2
		)
,CTE3 AS ( 
SELECT campaign_id 
		,brand 
		,customer_group 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
		,COUNT (DISTINCT communication_date) AS cntd_cohort 
FROM reporting.stg_campaign_objective_interactions
GROUP BY 1,2,3
		)
,CTE4 AS ( 
SELECT campaign_id 
		,brand 
		,customer_group 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE objective_met_date IS NOT NULL 
GROUP BY 1,2,3
)
,CTE5 AS ( 
SELECT campaign_id , brand 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE objective_met_date IS NOT NULL 
GROUP BY 1,2
)
,CTE6 AS ( 
SELECT campaign_id 
		,brand 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE objective_met_date IS NOT NULL 
	and objective_conv_after_contact IS TRUE
	and customer_group = 'I' 
GROUP BY 1,2
)
SELECT 'campaign-portfolio' AS grain 
		, master.campaign_id
		, master.campaign_portfolio 
		, COALESCE  (cte1.cntd_cust, 0)			AS cntd_cust
		, COALESCE  (cte1.cntd_comm, 0)			AS cntd_comm 
		, COALESCE  (cte1.cntd_cohort, 0)		AS cntd_cohort  
		, COALESCE  (cte2.cntd_cust, 0)			AS cntd_cust_contact 
		, COALESCE  (cte2.cntd_comm, 0)			AS cntd_comm_contact
		, COALESCE  (cte3i.cntd_cust, 0)		AS cntd_cust_i 
		, COALESCE  (cte3i.cntd_comm, 0)		AS cntd_comm_i 
		, COALESCE  (cte3i.cntd_cohort, 0)		AS cntd_cohort_i 
		, COALESCE  (cte3c.cntd_cust, 0)		AS cntd_cust_c 
		, COALESCE  (cte3c.cntd_comm, 0)		AS cntd_comm_c 
		, COALESCE  (cte3c.cntd_cohort, 0)		AS cntd_cohort_c 
		, COALESCE  (cte4i.cntd_cust, 0) 		AS cntd_cust_i_convert
		, COALESCE  (cte4i.cntd_comm, 0) 		AS cntd_comm_i_convert 
		, COALESCE  (cte4c.cntd_cust, 0)		AS cntd_cust_c_convert 
		, COALESCE  (cte4c.cntd_comm, 0)		AS cntd_comm_c_convert 
		, COALESCE  (cte5.cntd_cust, 0)			AS cntd_cust_convert 
		, COALESCE  (cte5.cntd_comm, 0)			AS cntd_comm_convert 
		, COALESCE  (cte6.cntd_cust, 0)			AS cntd_cust_convert_after_contact 
		, COALESCE  (cte6.cntd_comm, 0)			AS cntd_comm_convert_after_contact 
FROM ( SELECT DISTINCT cam.campaign_id
                    , cam.campaign_portfolio
		FROM reporting.ref_campaign_vw cam 
            WHERE cam.campaign_status = 'A'            
            ) master  		
LEFT JOIN CTE1 
	ON COALESCE(master.campaign_id,'') 			= COALESCE(cte1.campaign_id,'')		
LEFT JOIN CTE2 -- contacted 
	ON COALESCE(master.campaign_id,'') 			= COALESCE(cte2.campaign_id,'') 		
LEFT JOIN CTE3 cte3i-- total break C/I
	ON COALESCE(master.campaign_id,'') 			= COALESCE(cte3i.campaign_id,'') 		
	AND cte3i.customer_group 		= 'I'
LEFT JOIN CTE3 cte3c -- total break C/I
	ON COALESCE(master.campaign_id,'') 			= COALESCE(cte3c.campaign_id,'')	
	AND cte3c.customer_group 		= 'C'	
LEFT JOIN CTE4 cte4i -- object
	ON COALESCE(master.campaign_id,'') 			= COALESCE(cte4i.campaign_id,'')		
	AND cte4i.customer_group 		= 'I'		
LEFT JOIN CTE4 cte4c
	ON COALESCE(master.campaign_id,'') 			= COALESCE(cte4c.campaign_id,'')		
	AND cte4c.customer_group 		= 'C'	
LEFT JOIN CTE5 cte5 
	ON COALESCE(master.campaign_id,'') 			= COALESCE(cte5.campaign_id,'')	
LEFT JOIN CTE6 cte6 
	ON COALESCE(master.campaign_id,'') 			= COALESCE(cte6.campaign_id,'')	
; 	


/* 'campaign-portfolio-cohort' */ 
DROP TABLE IF EXISTS reporting.temp_rpt_agg_3b; 
CREATE TABLE reporting.temp_rpt_agg_3b AS 
WITH CTE1 AS ( 
SELECT campaign_id 
		,cohort
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
		,COUNT (DISTINCT communication_date) AS cntd_cohort 
FROM reporting.stg_campaign_objective_interactions
GROUP BY 1,2
		)
,CTE2 AS ( 
SELECT campaign_id 
		,cohort  
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE first_exposure_date IS NOT NULL 
GROUP BY 1,2
		)
,CTE3 AS ( 
SELECT campaign_id 
		,cohort
		,customer_group 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
		,COUNT (DISTINCT communication_date) AS cntd_cohort 
FROM reporting.stg_campaign_objective_interactions
GROUP BY 1,2,3 
		)
,CTE4 AS ( 
SELECT campaign_id 
		,cohort
		,customer_group 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE objective_met_date IS NOT NULL 
GROUP BY 1,2,3
)
,CTE5 AS ( 
SELECT campaign_id 
		,cohort
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE objective_met_date IS NOT NULL 
GROUP BY 1,2
)
,CTE6 AS ( 
SELECT campaign_id 
		,cohort
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE objective_met_date IS NOT NULL 
	and objective_conv_after_contact IS TRUE
	and customer_group = 'I' 
GROUP BY 1,2
)
SELECT 'campaign-portfolio-cohort' AS grain 
		, master.campaign_id
		, master.campaign_portfolio
		, master.cohort 	
		, COALESCE  (cte1.cntd_cust, 0)			AS cntd_cust
		, COALESCE  (cte1.cntd_comm, 0)			AS cntd_comm 
		, COALESCE  (cte1.cntd_cohort, 0)		AS cntd_cohort  
		, COALESCE  (cte2.cntd_cust, 0)			AS cntd_cust_contact 
		, COALESCE  (cte2.cntd_comm, 0)			AS cntd_comm_contact
		, COALESCE  (cte3i.cntd_cust, 0)		AS cntd_cust_i 
		, COALESCE  (cte3i.cntd_comm, 0)		AS cntd_comm_i 
		, COALESCE  (cte3i.cntd_cohort, 0)		AS cntd_cohort_i 
		, COALESCE  (cte3c.cntd_cust, 0)		AS cntd_cust_c 
		, COALESCE  (cte3c.cntd_comm, 0)		AS cntd_comm_c 
		, COALESCE  (cte3c.cntd_cohort, 0)		AS cntd_cohort_c 
		, COALESCE  (cte4i.cntd_cust, 0) 		AS cntd_cust_i_convert
		, COALESCE  (cte4i.cntd_comm, 0) 		AS cntd_comm_i_convert 
		, COALESCE  (cte4c.cntd_cust, 0)		AS cntd_cust_c_convert 
		, COALESCE  (cte4c.cntd_comm, 0)		AS cntd_comm_c_convert 
		, COALESCE  (cte5.cntd_cust, 0)			AS cntd_cust_convert 
		, COALESCE  (cte5.cntd_comm, 0)			AS cntd_comm_convert 
		, COALESCE  (cte6.cntd_cust, 0)			AS cntd_cust_convert_after_contact 
		, COALESCE  (cte6.cntd_comm, 0)			AS cntd_comm_convert_after_contact 
FROM ( SELECT DISTINCT cam.campaign_id
                    , cam.campaign_portfolio
                    , agg.cohort
		FROM reporting.ref_campaign_vw cam 
            CROSS JOIN (SELECT DISTINCT cohort 
						FROM reporting.stg_campaign_objective_interactions
						WHERE cohort IS NOT NULL) agg 
            WHERE cam.campaign_status = 'A'            
            ) master  		
LEFT JOIN CTE1 
	ON COALESCE(master.campaign_id,'') 			= COALESCE(cte1.campaign_id,'')		
	AND COALESCE(master.cohort,'') 				= COALESCE(cte1.cohort,'') 
LEFT JOIN CTE2 -- contacted 
	ON COALESCE(master.campaign_id,'') 			= COALESCE(cte2.campaign_id,'') 		
	AND COALESCE(master.cohort,'') 				= COALESCE(cte2.cohort,'') 
LEFT JOIN CTE3 cte3i-- total break C/I
	ON COALESCE(master.campaign_id,'') 			= COALESCE(cte3i.campaign_id,'') 		
	AND COALESCE(master.cohort,'') 				= COALESCE(cte3i.cohort,'') 
	AND cte3i.customer_group 		= 'I'
LEFT JOIN CTE3 cte3c -- total break C/I
	ON COALESCE(master.campaign_id,'') 			= COALESCE(cte3c.campaign_id,'')	
	AND COALESCE(master.cohort,'') 				= COALESCE(cte3c.cohort,'') 
	AND cte3c.customer_group 		= 'C'	
LEFT JOIN CTE4 cte4i -- object
	ON COALESCE(master.campaign_id,'') 			= COALESCE(cte4i.campaign_id,'')		
	AND COALESCE(master.cohort,'') 				= COALESCE(cte4i.cohort,'') 
	AND cte4i.customer_group 		= 'I'		
LEFT JOIN CTE4 cte4c
	ON COALESCE(master.campaign_id,'') 			= COALESCE(cte4c.campaign_id,'')	
	AND COALESCE(master.cohort,'') 				= COALESCE(cte4c.cohort,'') 	
	AND cte4c.customer_group 		= 'C'	
LEFT JOIN CTE5 cte5 
	ON COALESCE(master.campaign_id,'') 			= COALESCE(cte5.campaign_id,'')	
	AND COALESCE(master.cohort,'') 				= COALESCE(cte5.cohort,'') 	
LEFT JOIN CTE6 cte6 
	ON COALESCE(master.campaign_id,'') 			= COALESCE(cte6.campaign_id,'')	
	AND COALESCE(master.cohort,'') 				= COALESCE(cte6.cohort,'') 	
; 	

/* at campaign / touch point */ 
DROP TABLE IF EXISTS reporting.temp_rpt_agg_4a; 
CREATE TABLE reporting.temp_rpt_agg_4a AS 
WITH CTE1 AS ( 
SELECT campaign_id 
		,touchpoint 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
		,COUNT (DISTINCT communication_date) AS cntd_cohort 
FROM reporting.stg_campaign_objective_interactions
GROUP BY 1,2 
		)
,CTE2 AS ( 
SELECT campaign_id 
		,touchpoint 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE first_exposure_date IS NOT NULL 
GROUP BY 1,2
		)
,CTE3 AS ( 
SELECT campaign_id 
		,touchpoint 
		,customer_group 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
		,COUNT (DISTINCT communication_date) AS cntd_cohort 
FROM reporting.stg_campaign_objective_interactions
GROUP BY 1,2,3
		)
,CTE4 AS ( 
SELECT campaign_id 
		,touchpoint 
		,customer_group 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE objective_met_date IS NOT NULL 
GROUP BY 1,2,3
		)
,CTE5 AS ( 
SELECT campaign_id 
		,touchpoint 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE objective_met_date IS NOT NULL 
GROUP BY 1,2
		)
,CTE6 AS ( 
SELECT campaign_id  
		,touchpoint 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE objective_met_date IS NOT NULL 
	and objective_conv_after_contact IS TRUE
	and customer_group = 'I' -- contacted would only for intervention 
GROUP BY 1,2
		)
SELECT 'campaign-portfolio-channel-touchpoint' AS grain 
        , master.campaign_id
		, master.campaign_portfolio
		, master.channel
		, master.action
		, master.touchpoint
		, COALESCE  (cte1.cntd_cust, 0)			AS cntd_cust
		, COALESCE  (cte1.cntd_comm, 0)			AS cntd_comm 
		, COALESCE  (cte1.cntd_cohort, 0)		AS cntd_cohort  
		, COALESCE  (cte2.cntd_cust, 0)			AS cntd_cust_contact 
		, COALESCE  (cte2.cntd_comm, 0)			AS cntd_comm_contact
		, COALESCE  (cte3i.cntd_cust, 0)		AS cntd_cust_i 
		, COALESCE  (cte3i.cntd_comm, 0)		AS cntd_comm_i 
		, COALESCE  (cte3i.cntd_cohort, 0)		AS cntd_cohort_i 
		, COALESCE  (cte3c.cntd_cust, 0)		AS cntd_cust_c 
		, COALESCE  (cte3c.cntd_comm, 0)		AS cntd_comm_c 
		, COALESCE  (cte3c.cntd_cohort, 0)		AS cntd_cohort_c 
		, COALESCE  (cte4i.cntd_cust, 0) 		AS cntd_cust_i_convert
		, COALESCE  (cte4i.cntd_comm, 0) 		AS cntd_comm_i_convert 
		, COALESCE  (cte4c.cntd_cust, 0)		AS cntd_cust_c_convert 
		, COALESCE  (cte4c.cntd_comm, 0)		AS cntd_comm_c_convert 
		, COALESCE  (cte5.cntd_cust, 0)			AS cntd_cust_convert 
		, COALESCE  (cte5.cntd_comm, 0)			AS cntd_comm_convert 
		, COALESCE  (cte6.cntd_cust, 0)			AS cntd_cust_convert_after_contact 
		, COALESCE  (cte6.cntd_comm, 0)			AS cntd_comm_convert_after_contact 
FROM ( 	SELECT DISTINCT  cam.campaign_id
						, cam.campaign_portfolio
						, camt.channel
						, camt.action 
						, camt.touchpoint
		FROM reporting.ref_campaign_vw cam 
            INNER JOIN reporting.ref_campaign_touchpoint_vw camt 
                ON camt.campaign_id = cam.campaign_id 
            WHERE cam.campaign_status = 'A' 
		) master
LEFT JOIN CTE1 
	ON COALESCE(master.campaign_id,'')    = COALESCE(cte1.campaign_id, '')		
	AND COALESCE(master.touchpoint, '')     = COALESCE(cte1.touchpoint, '')	
LEFT JOIN CTE2 -- contacted 
	ON COALESCE(master.campaign_id,'')    = COALESCE(cte2.campaign_id, '')		
	AND COALESCE(master.touchpoint, '')     = COALESCE(cte2.touchpoint, '')	
LEFT JOIN CTE3 cte3i-- total break C/I
	ON COALESCE(master.campaign_id,'')    = COALESCE(cte3i.campaign_id, '')		
	AND COALESCE(master.touchpoint, '')     = COALESCE(cte3i.touchpoint, '')	
	AND cte3i.customer_group 		= 'I'
LEFT JOIN CTE3 cte3c -- total break C/I
	ON COALESCE(master.campaign_id,'')    = COALESCE(cte3c.campaign_id, '')		
	AND COALESCE(master.touchpoint, '')     = COALESCE(cte3c.touchpoint, '')	
	AND cte3c.customer_group 		= 'C'	
LEFT JOIN CTE4 cte4i 
	ON COALESCE(master.campaign_id,'')    = COALESCE(cte4i.campaign_id, '')		
	AND COALESCE(master.touchpoint, '')     = COALESCE(cte4i.touchpoint, '')	
	AND coalesce(cte4i.customer_group, '') 		= 'I'		
LEFT JOIN CTE4 cte4c 
	ON COALESCE(master.campaign_id,'')    = COALESCE(cte4c.campaign_id, '')		
	AND COALESCE(master.touchpoint, '')     = COALESCE(cte4c.touchpoint, '')	
	AND cte4c.customer_group 		= 'C'	
LEFT JOIN CTE5 cte5 
	ON COALESCE(master.campaign_id,'')    = COALESCE(cte5.campaign_id, '')		
	AND COALESCE(master.touchpoint, '')     = COALESCE(cte5.touchpoint, '')	
LEFT JOIN CTE6 cte6 
	ON COALESCE(master.campaign_id,'')    = COALESCE(cte6.campaign_id, '')		
	AND COALESCE(master.touchpoint, '')     = COALESCE(cte6.touchpoint, '')	
; 	


DROP TABLE IF EXISTS reporting.temp_rpt_agg_4c; 
CREATE TABLE reporting.temp_rpt_agg_4c AS 
WITH CTE1 AS ( 
SELECT campaign_id 
		,cohort
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
		,COUNT (DISTINCT communication_date) AS cntd_cohort 
FROM reporting.stg_campaign_objective_interactions
GROUP BY 1,2
		)
,CTE2 AS ( 
SELECT campaign_id 
		,cohort 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE first_exposure_date IS NOT NULL 
GROUP BY 1,2
		)
,CTE3 AS ( 
/* at compaign level (no objective) C VS T -- total */ 
SELECT campaign_id 
		,cohort 
		,customer_group 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
		,COUNT (DISTINCT communication_date) AS cntd_cohort 
FROM reporting.stg_campaign_objective_interactions
GROUP BY 1,2,3
		)
,CTE4 AS ( 
SELECT campaign_id 
		,cohort 
		,objective_id 
		,customer_group 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE objective_met_date IS NOT NULL --> i.e. no objective met -- note this condition should have been redundant becuase this is mandatory 
GROUP BY 1,2,3,4
		)
,CTE5 AS ( 
SELECT campaign_id 
		,cohort 
		,objective_id  
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE objective_met_date IS NOT NULL --> i.e. no objective met -- note this condition should have been redundant becuase this is mandatory 
GROUP BY 1,2,3
		)
,CTE6 AS ( 
SELECT campaign_id 
		,cohort 
		,objective_id 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE objective_met_date IS NOT NULL 
	and objective_conv_after_contact IS TRUE
	and customer_group = 'I' -- contacted would only for intervention 
GROUP BY 1,2,3
		)
SELECT 'campaign-portfolio-objective-cohort' AS grain 
		, master.campaign_id
		, master.campaign_portfolio
		, master.cohort 
		, master.objective_id 
		, master.objective_name		
		, master.objective_rank
		, master.objective_is_positive		
		, COALESCE  (cte1.cntd_cust, 0)			AS cntd_cust
		, COALESCE  (cte1.cntd_comm, 0)			AS cntd_comm 
		, COALESCE  (cte1.cntd_cohort, 0)		AS cntd_cohort  
		, COALESCE  (cte2.cntd_cust, 0)			AS cntd_cust_contact 
		, COALESCE  (cte2.cntd_comm, 0)			AS cntd_comm_contact
		, COALESCE  (cte3i.cntd_cust, 0)		AS cntd_cust_i 
		, COALESCE  (cte3i.cntd_comm, 0)		AS cntd_comm_i 
		, COALESCE  (cte3i.cntd_cohort, 0)		AS cntd_cohort_i 
		, COALESCE  (cte3c.cntd_cust, 0)		AS cntd_cust_c 
		, COALESCE  (cte3c.cntd_comm, 0)		AS cntd_comm_c 
		, COALESCE  (cte3c.cntd_cohort, 0)		AS cntd_cohort_c 
		, COALESCE  (cte4i.cntd_cust, 0) 		AS cntd_cust_i_convert
		, COALESCE  (cte4i.cntd_comm, 0) 		AS cntd_comm_i_convert 
		, COALESCE  (cte4c.cntd_cust, 0)		AS cntd_cust_c_convert 
		, COALESCE  (cte4c.cntd_comm, 0)		AS cntd_comm_c_convert 
		, COALESCE  (cte5.cntd_cust, 0)			AS cntd_cust_convert 
		, COALESCE  (cte5.cntd_comm, 0)			AS cntd_comm_convert 
		, COALESCE  (cte6.cntd_cust, 0)			AS cntd_cust_convert_after_contact 
		, COALESCE  (cte6.cntd_comm, 0)			AS cntd_comm_convert_after_contact 
FROM ( SELECT DISTINCT agg.campaign_id
					, agg.campaign_portfolio
					, agg.objective_id
					, obj.objective_name 
					, COALESCE(agg.objective_rank, 'Unintend Outcome') AS objective_rank  
					, agg.objective_is_positive
                    , agg.cohort
		FROM reporting.stg_campaign_objective_interactions agg 
			LEFT JOIN reporting.ref_objective_vw obj 
				ON agg.objective_id = obj.objective_id 
			WHERE agg.objective_id IS NOT NULL 
            ) MASTER		
LEFT JOIN CTE1 
	ON COALESCE(master.campaign_id, '') 	= COALESCE(cte1.campaign_id, '')		
	AND COALESCE(master.cohort, '') 	    = COALESCE(cte1.cohort, '') 
LEFT JOIN CTE2 -- contacted 
	ON COALESCE(master.campaign_id, '') 	= COALESCE(cte2.campaign_id, '')		
	AND COALESCE(master.cohort, '') 	    = COALESCE(cte2.cohort, '') 
LEFT JOIN CTE3 cte3i-- total break C/I
	ON COALESCE(master.campaign_id, '') 	= COALESCE(cte3i.campaign_id, '')		
	AND COALESCE(master.cohort, '') 	    = COALESCE(cte3i.cohort, '') 
	AND cte3i.customer_group 		= 'I'
LEFT JOIN CTE3 cte3c -- total break C/I
	ON COALESCE(master.campaign_id, '') 	= COALESCE(cte3c.campaign_id, '')		
	AND COALESCE(master.cohort, '') 	    = COALESCE(cte3c.cohort, '') 
	AND cte3c.customer_group 		= 'C'	
LEFT JOIN CTE4 cte4i -- object
	ON COALESCE(master.campaign_id, '') 	= COALESCE(cte4i.campaign_id, '')		
	AND COALESCE(master.objective_id, '') 	= COALESCE(cte4i.objective_id, '') 
	AND COALESCE(master.cohort, '') 	    = COALESCE(cte4i.cohort, '') 
	AND cte4i.customer_group 		= 'I'		
LEFT JOIN CTE4 cte4c
	ON COALESCE(master.campaign_id, '') 	= COALESCE(cte4c.campaign_id, '')		
	AND COALESCE(master.objective_id, '') 	= COALESCE(cte4c.objective_id, '') 
	AND COALESCE(master.cohort, '') 	    = COALESCE(cte4c.cohort, '') 
	AND cte4c.customer_group 		= 'C'	
LEFT JOIN CTE5 cte5 
	ON COALESCE(master.campaign_id, '') 	= COALESCE(cte5.campaign_id, '')		
	AND COALESCE(master.objective_id, '') 	= COALESCE(cte5.objective_id, '') 
	AND COALESCE(master.cohort, '') 	    = COALESCE(cte5.cohort, '') 
LEFT JOIN CTE6 cte6 
	ON COALESCE(master.campaign_id, '') 	= COALESCE(cte6.campaign_id, '')		
	AND COALESCE(master.objective_id, '') 	= COALESCE(cte6.objective_id, '') 
	AND COALESCE(master.cohort, '') 	    = COALESCE(cte6.cohort, '') 
; 	


DROP TABLE IF EXISTS reporting.temp_rpt_agg_5b; 
CREATE TABLE reporting.temp_rpt_agg_5b AS 
WITH CTE1 AS ( 
SELECT campaign_id  
		,touchpoint 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm
		,COUNT (DISTINCT communication_date) AS cntd_cohort 
FROM reporting.stg_campaign_objective_interactions
GROUP BY 1,2
		)
,CTE2 AS ( 
SELECT campaign_id  
		,touchpoint 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE first_exposure_date IS NOT NULL 
GROUP BY 1,2
		)
,CTE3 AS ( 
/* at compaign level (no objective) C VS T -- total */ 
SELECT campaign_id 
		,touchpoint 
		,customer_group  
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
		,COUNT (DISTINCT communication_date) AS cntd_cohort 
FROM reporting.stg_campaign_objective_interactions
GROUP BY 1,2,3 
		)
,CTE4 AS ( 
SELECT campaign_id 
		,objective_id 
		,touchpoint 
		,customer_group  
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE objective_met_date IS NOT NULL 
GROUP BY 1,2,3,4 
		)
,CTE5 AS ( 
SELECT campaign_id 
		,objective_id 
		,touchpoint 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE objective_met_date IS NOT NULL 
GROUP BY 1,2,3
		)
,CTE6 AS ( 
SELECT campaign_id 
		,objective_id 
		,touchpoint 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.stg_campaign_objective_interactions
WHERE objective_met_date IS NOT NULL 
	and objective_conv_after_contact IS TRUE
	and customer_group = 'I' -- contacted would only for intervention 
GROUP BY 1,2,3
		)
SELECT 'campaign-portfolio-touchpoint-channel-objective' AS grain 
        , master.campaign_id
		, master.campaign_portfolio
		, master.channel
		, master.action
		, master.touchpoint 
		, master.objective_id 
		, master.objective_name		
		, master.objective_rank
		, master.objective_is_positive 
		, COALESCE  (cte1.cntd_cust, 0)			AS cntd_cust
		, COALESCE  (cte1.cntd_comm, 0)			AS cntd_comm 
		, COALESCE  (cte1.cntd_cohort, 0)		AS cntd_cohort  
		, COALESCE  (cte2.cntd_cust, 0)			AS cntd_cust_contact 
		, COALESCE  (cte2.cntd_comm, 0)			AS cntd_comm_contact
		, COALESCE  (cte3i.cntd_cust, 0)		AS cntd_cust_i 
		, COALESCE  (cte3i.cntd_comm, 0)		AS cntd_comm_i 
		, COALESCE  (cte3i.cntd_cohort, 0)		AS cntd_cohort_i 
		, COALESCE  (cte3c.cntd_cust, 0)		AS cntd_cust_c 
		, COALESCE  (cte3c.cntd_comm, 0)		AS cntd_comm_c 
		, COALESCE  (cte3c.cntd_cohort, 0)		AS cntd_cohort_c 
		, COALESCE  (cte4i.cntd_cust, 0) 		AS cntd_cust_i_convert
		, COALESCE  (cte4i.cntd_comm, 0) 		AS cntd_comm_i_convert 
		, COALESCE  (cte4c.cntd_cust, 0)		AS cntd_cust_c_convert 
		, COALESCE  (cte4c.cntd_comm, 0)		AS cntd_comm_c_convert 
		, COALESCE  (cte5.cntd_cust, 0)			AS cntd_cust_convert 
		, COALESCE  (cte5.cntd_comm, 0)			AS cntd_comm_convert 
		, COALESCE  (cte6.cntd_cust, 0)			AS cntd_cust_convert_after_contact 
		, COALESCE  (cte6.cntd_comm, 0)			AS cntd_comm_convert_after_contact 
FROM ( SELECT DISTINCT cam.campaign_id
					, cam.campaign_portfolio
					, obj.objective_id
					, obj.objective_name 
					, camo.objective_rank 
					, obj.objective_is_positive 
					, camt.action  
					, camt.touchpoint
					, camt.channel
		FROM reporting.ref_campaign_vw cam 
            INNER JOIN reporting.ref_campaign_objective_vw camo 
                ON camo.campaign_id = cam.campaign_id 
            INNER JOIN reporting.ref_objective_vw obj
                ON camo.objective_id = obj.objective_id
            INNER JOIN reporting.ref_campaign_touchpoint_vw camt 
                ON cam.campaign_id = camt.campaign_id 
            WHERE cam.campaign_status = 'A'    
        ) MASTER		
LEFT JOIN CTE1 
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte1.campaign_id, '') 	 
	AND COALESCE(master.touchpoint, '') 	= COALESCE(cte1.touchpoint, '') 	 	
LEFT JOIN CTE2 -- contacted 
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte2.campaign_id, '') 	
	AND COALESCE(master.touchpoint, '') 	= COALESCE(cte2.touchpoint, '') 
LEFT JOIN CTE3 cte3i-- total break C/I
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte3i.campaign_id, '') 		 
	AND COALESCE(master.touchpoint, '') 	= COALESCE(cte3i.touchpoint, '') 	
	AND cte3i.customer_group 		= 'I'
LEFT JOIN CTE3 cte3c -- total break C/I
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte3c.campaign_id, '') 	
	AND COALESCE(master.touchpoint, '') 	= COALESCE(cte3c.touchpoint, '') 	
	AND cte3c.customer_group 		= 'C'	
LEFT JOIN CTE4 cte4i -- object
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte4i.campaign_id, '') 	
	AND COALESCE(master.objective_id, '') 		= COALESCE(cte4i.objective_id, '') 	 
	AND COALESCE(master.touchpoint, '') 	= COALESCE(cte4i.touchpoint, '') 	
	AND cte4i.customer_group 		= 'I'		
LEFT JOIN CTE4 cte4c -- object
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte4c.campaign_id, '') 	
	AND COALESCE(master.objective_id, '') 		= COALESCE(cte4c.objective_id, '') 	 
	AND COALESCE(master.touchpoint, '') 	= COALESCE(cte4c.touchpoint, '') 
	AND cte4c.customer_group 		= 'C'	
LEFT JOIN CTE5 cte5 -- object
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte5.campaign_id, '') 	
	AND COALESCE(master.objective_id, '') 		= COALESCE(cte5.objective_id, '') 	 
	AND COALESCE(master.touchpoint, '') 	= COALESCE(cte5.touchpoint, '') 
LEFT JOIN CTE6 cte6 -- object
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte6.campaign_id, '') 	
	AND COALESCE(master.objective_id, '') 		= COALESCE(cte6.objective_id, '') 	 
	AND COALESCE(master.touchpoint, '') 	= COALESCE(cte6.touchpoint, '') 
; 	

/* Total - complete and incomplete */ 
DROP TABLE IF EXISTS reporting.temp_rpt_agg_5cc; 
CREATE TABLE IF NOT EXISTS reporting.temp_rpt_agg_5cc AS 
SELECT DISTINCT pmd.performance_measurement_date 
        ,coi.campaign_id 
        ,coi.customer_id 
        ,coi.objective_id 
        ,coi.communication_id 
        ,coi.communication_date 
        ,coi.customer_group 
		,coi.cohort
        ,coi.objective_met_date 
        ,coi.first_exposure_date 
        ,coi.objective_conv_after_contact 
		,coi.capture_period 
FROM reporting.stg_campaign_objective_interactions coi 
INNER JOIN reporting.ref_campaign_vw ref 
	ON coi.campaign_id = ref.campaign_id 
	AND ref.campaign_status = 'A'
CROSS JOIN (SELECT GENERATE_SERIES(
           CURRENT_DATE - INTERVAL '1 year',  
           CURRENT_DATE,                      
           INTERVAL '1 day'                   
       )::DATE AS performance_measurement_date
) pmd 
WHERE coi.communication_date <= pmd.performance_measurement_date 
AND ref.campaign_start_date >= pmd.performance_measurement_date 
AND coi.communication_date <= CURRENT_DATE 
AND ( coi.objective_rank != 'Unintended Outcome' OR coi.objective_rank IS NULL) 
;

DROP TABLE IF EXISTS reporting.temp_rpt_agg_4dd; 
CREATE TABLE IF NOT EXISTS reporting.temp_rpt_agg_4dd AS 
SELECT DISTINCT pmd.performance_measurement_date 
        ,pmd.campaign_id 
        ,pmd.customer_id 
        ,pmd.objective_id 
        ,pmd.communication_id 
        ,pmd.communication_date 
        ,pmd.customer_group 
        ,pmd.objective_met_date 
        ,pmd.first_exposure_date 
        ,pmd.objective_conv_after_contact 
FROM reporting.temp_rpt_agg_5cc pmd
WHERE UPPER(cohort) = 'COMPLETE COHORT'
AND communication_date < CURRENT_DATE - capture_period 
; 

DROP TABLE IF EXISTS reporting.temp_rpt_agg_4d; 
CREATE TABLE reporting.temp_rpt_agg_4d AS 
WITH CTE1 AS ( 
SELECT performance_measurement_date 
        ,campaign_id 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm
		,COUNT (DISTINCT communication_date) AS cntd_cohort 
FROM reporting.temp_rpt_agg_4dd
GROUP BY 1,2
		)
,CTE2 AS ( 
SELECT performance_measurement_date 
        ,campaign_id 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.temp_rpt_agg_4dd
WHERE first_exposure_date IS NOT NULL 
GROUP BY 1,2
		)
,CTE3 AS ( 
SELECT performance_measurement_date 
        ,campaign_id  
		,customer_group  
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
		,COUNT (DISTINCT communication_date) AS cntd_cohort 
FROM reporting.temp_rpt_agg_4dd
GROUP BY 1,2,3
		)
,CTE4 AS ( 
SELECT performance_measurement_date 
        ,campaign_id 
		,objective_id 
		,customer_group  
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.temp_rpt_agg_4dd
WHERE objective_met_date IS NOT NULL 
GROUP BY 1,2,3,4
		)
,CTE5 AS ( 
SELECT performance_measurement_date 
        ,campaign_id
		,objective_id   
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.temp_rpt_agg_4dd
WHERE objective_met_date IS NOT NULL 
GROUP BY 1,2,3
		)
,CTE6 AS ( 
SELECT performance_measurement_date 
        ,campaign_id 
		,objective_id 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.temp_rpt_agg_4dd
WHERE objective_met_date IS NOT NULL 
	and objective_conv_after_contact IS TRUE
	and customer_group = 'I' 
GROUP BY 1,2,3
		)
SELECT 'campaign-portfolio-objective-performance_measurement_date' AS grain 
        , master.campaign_id
		, master.campaign_portfolio
		, master.objective_id 
		, master.objective_name		
		, master.objective_rank
		, master.objective_is_positive 
        , master.performance_measurement_date 
		, COALESCE  (cte1.cntd_cust, 0)			AS cntd_cust
		, COALESCE  (cte1.cntd_comm, 0)			AS cntd_comm 
		, COALESCE  (cte1.cntd_cohort, 0)		AS cntd_cohort 
		, COALESCE  (cte2.cntd_cust, 0)			AS cntd_cust_contact 
		, COALESCE  (cte2.cntd_comm, 0)			AS cntd_comm_contact
		, COALESCE  (cte3i.cntd_cust, 0)		AS cntd_cust_i 
		, COALESCE  (cte3i.cntd_comm, 0)		AS cntd_comm_i 
		, COALESCE  (cte3i.cntd_cohort, 0)		AS cntd_cohort_i 
		, COALESCE  (cte3c.cntd_cust, 0)		AS cntd_cust_c 
		, COALESCE  (cte3c.cntd_comm, 0)		AS cntd_comm_c 
		, COALESCE  (cte3c.cntd_cohort, 0)		AS cntd_cohort_c 
		, COALESCE  (cte4i.cntd_cust, 0) 		AS cntd_cust_i_convert
		, COALESCE  (cte4i.cntd_comm, 0) 		AS cntd_comm_i_convert 
		, COALESCE  (cte4c.cntd_cust, 0)		AS cntd_cust_c_convert 
		, COALESCE  (cte4c.cntd_comm, 0)		AS cntd_comm_c_convert 
		, COALESCE  (cte5.cntd_cust, 0)			AS cntd_cust_convert 
		, COALESCE  (cte5.cntd_comm, 0)			AS cntd_comm_convert 
		, COALESCE  (cte6.cntd_cust, 0)			AS cntd_cust_convert_after_contact 
		, COALESCE  (cte6.cntd_comm, 0)			AS cntd_comm_convert_after_contact 
FROM ( SELECT DISTINCT dd.performance_measurement_date 
        ,dd.campaign_id 
        ,dd.objective_id 
		,cam.campaign_portfolio 
		,obj.objective_name 
		,cam_obj.objective_rank 
		,obj.objective_is_positive
		FROM reporting.temp_rpt_agg_4dd dd 
        INNER JOIN reporting.ref_campaign_vw cam 
			ON dd.campaign_id  = cam.campaign_id 
		INNER JOIN reporting.ref_campaign_objective_vw  cam_obj 
			ON dd.campaign_id = cam_obj.campaign_id 
			AND dd.objective_id = cam_obj.objective_id 
		INNER JOIN reporting.ref_objective_vw obj 
			ON cam_obj.objective_id = obj.objective_id 
			AND dd.objective_id = obj.objective_id 
        ) MASTER		
LEFT JOIN CTE1 
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte1.campaign_id, '') 	 
	AND COALESCE(master.performance_measurement_date, '1900-01-01') 	= COALESCE(cte1.performance_measurement_date, '1900-01-01')  
LEFT JOIN CTE2 -- contacted 
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte2.campaign_id, '') 	
	AND COALESCE(master.performance_measurement_date, '1900-01-01') 	= COALESCE(cte2.performance_measurement_date, '1900-01-01') 	
LEFT JOIN CTE3 cte3i-- total break C/I
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte3i.campaign_id, '') 		 
	AND COALESCE(master.performance_measurement_date, '1900-01-01') 	= COALESCE(cte3i.performance_measurement_date, '1900-01-01') 	
	AND cte3i.customer_group 		= 'I'
LEFT JOIN CTE3 cte3c -- total break C/I
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte3c.campaign_id, '') 	
	AND COALESCE(master.performance_measurement_date, '1900-01-01') 	= COALESCE(cte3c.performance_measurement_date, '1900-01-01') 		
	AND cte3c.customer_group 		= 'C'	
LEFT JOIN CTE4 cte4i -- object
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte4i.campaign_id, '') 	
	AND COALESCE(master.objective_id, '') 	= COALESCE(cte4i.objective_id, '') 	 
	AND COALESCE(master.performance_measurement_date, '1900-01-01') 	= COALESCE(cte4i.performance_measurement_date, '1900-01-01') 	
	AND cte4i.customer_group 		= 'I'		
LEFT JOIN CTE4 cte4c -- object
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte4c.campaign_id, '') 	
	AND COALESCE(master.objective_id, '') 	= COALESCE(cte4c.objective_id, '') 	 
	AND COALESCE(master.performance_measurement_date, '1900-01-01') 	= COALESCE(cte4c.performance_measurement_date, '1900-01-01') 	
	AND cte4c.customer_group 		= 'C'	
LEFT JOIN CTE5 cte5 -- object
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte5.campaign_id, '') 	
	AND COALESCE(master.objective_id, '') 		= COALESCE(cte5.objective_id, '') 	 
	AND COALESCE(master.performance_measurement_date, '1900-01-01') 	= COALESCE(cte5.performance_measurement_date, '1900-01-01') 	
LEFT JOIN CTE6 cte6 -- object
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte6.campaign_id, '') 	
	AND COALESCE(master.objective_id, '') 		= COALESCE(cte6.objective_id, '') 	 
	AND COALESCE(master.performance_measurement_date, '1900-01-01') 	= COALESCE(cte6.performance_measurement_date, '1900-01-01') 	
; 	


DROP TABLE IF EXISTS reporting.temp_rpt_agg_5c; 
CREATE TABLE reporting.temp_rpt_agg_5c AS 
WITH CTE1 AS ( 
SELECT performance_measurement_date 
        ,campaign_id 
		,cohort 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm
		,COUNT (DISTINCT communication_date) AS cntd_cohort 
FROM reporting.temp_rpt_agg_5cc
GROUP BY 1,2,3
		)
,CTE2 AS ( 
SELECT performance_measurement_date 
        ,campaign_id 
		,cohort
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.temp_rpt_agg_5cc
WHERE first_exposure_date IS NOT NULL 
GROUP BY 1,2,3
		)
,CTE3 AS ( 
SELECT performance_measurement_date 
        ,campaign_id  
		,customer_group  
		,cohort 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
		,COUNT (DISTINCT communication_date) AS cntd_cohort 
FROM reporting.temp_rpt_agg_5cc
GROUP BY 1,2,3,4
		)
,CTE4 AS ( 
SELECT performance_measurement_date 
        ,campaign_id 
		,objective_id 
		,customer_group  
		,cohort
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.temp_rpt_agg_5cc
WHERE objective_met_date IS NOT NULL 
GROUP BY 1,2,3,4,5
		)
,CTE5 AS ( 
SELECT performance_measurement_date 
        ,campaign_id
		,objective_id   
		,cohort 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.temp_rpt_agg_5cc
WHERE objective_met_date IS NOT NULL 
GROUP BY 1,2,3,4
		)
,CTE6 AS ( 
SELECT performance_measurement_date 
        ,campaign_id 
		,objective_id 
		,cohort 
		,COUNT (DISTINCT customer_id) AS cntd_cust
		,COUNT (DISTINCT communication_id) AS cntd_comm 
FROM reporting.temp_rpt_agg_5cc
WHERE objective_met_date IS NOT NULL 
	and objective_conv_after_contact IS TRUE
	and customer_group = 'I' 
GROUP BY 1,2,3,4
		)
SELECT 'campaign-portfolio-objective-cohort-performancem_measurement_date' AS grain 
        , master.campaign_id
		, master.campaign_portfolio
        , master.cohort 
		, master.objective_id 
		, master.objective_name		
		, master.objective_rank
		, master.objective_is_positive 
        , master.performance_measurement_date 
		, COALESCE  (cte1.cntd_cust, 0)			AS cntd_cust
		, COALESCE  (cte1.cntd_comm, 0)			AS cntd_comm 
		, COALESCE  (cte1.cntd_cohort, 0)		AS cntd_cohort 
		, COALESCE  (cte2.cntd_cust, 0)			AS cntd_cust_contact 
		, COALESCE  (cte2.cntd_comm, 0)			AS cntd_comm_contact
		, COALESCE  (cte3i.cntd_cust, 0)		AS cntd_cust_i 
		, COALESCE  (cte3i.cntd_comm, 0)		AS cntd_comm_i 
		, COALESCE  (cte3i.cntd_cohort, 0)		AS cntd_cohort_i 
		, COALESCE  (cte3c.cntd_cust, 0)		AS cntd_cust_c 
		, COALESCE  (cte3c.cntd_comm, 0)		AS cntd_comm_c 
		, COALESCE  (cte3c.cntd_cohort, 0)		AS cntd_cohort_c 
		, COALESCE  (cte4i.cntd_cust, 0) 		AS cntd_cust_i_convert
		, COALESCE  (cte4i.cntd_comm, 0) 		AS cntd_comm_i_convert 
		, COALESCE  (cte4c.cntd_cust, 0)		AS cntd_cust_c_convert 
		, COALESCE  (cte4c.cntd_comm, 0)		AS cntd_comm_c_convert 
		, COALESCE  (cte5.cntd_cust, 0)			AS cntd_cust_convert 
		, COALESCE  (cte5.cntd_comm, 0)			AS cntd_comm_convert 
		, COALESCE  (cte6.cntd_cust, 0)			AS cntd_cust_convert_after_contact 
		, COALESCE  (cte6.cntd_comm, 0)			AS cntd_comm_convert_after_contact 
FROM ( SELECT DISTINCT cc.performance_measurement_date 
        ,cc.campaign_id 
        ,cc.objective_id 
        ,cc.cohort 
		,cam.campaign_portfolio 
		,obj.objective_name 
		,cam_obj.objective_rank 
		,obj.objective_is_positive
		FROM reporting.temp_rpt_agg_5cc cc
        INNER JOIN reporting.ref_campaign_vw cam 
			ON cc.campaign_id  = cam.campaign_id 
		INNER JOIN reporting.ref_campaign_objective_vw  cam_obj 
			ON cc.campaign_id = cam_obj.campaign_id 
			AND cc.objective_id = cam_obj.objective_id 
		INNER JOIN reporting.ref_objective_vw obj 
			ON cam_obj.objective_id = obj.objective_id 
			AND cc.objective_id = obj.objective_id  
        ) MASTER		
LEFT JOIN CTE1 
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte1.campaign_id, '') 	 
	AND COALESCE(master.cohort, '') 		= COALESCE(cte1.cohort, '')
	AND COALESCE(master.performance_measurement_date, '1900-01-01') 	= COALESCE(cte1.performance_measurement_date, '1900-01-01')  
LEFT JOIN CTE2 -- contacted 
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte2.campaign_id, '') 	
	AND COALESCE(master.cohort, '') 		= COALESCE(cte2.cohort, '')
	AND COALESCE(master.performance_measurement_date, '1900-01-01') 	= COALESCE(cte2.performance_measurement_date, '1900-01-01') 	
LEFT JOIN CTE3 cte3i-- total break C/I
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte3i.campaign_id, '') 	
	AND COALESCE(master.cohort, '') 		= COALESCE(cte3i.cohort, '')	 
	AND COALESCE(master.performance_measurement_date, '1900-01-01') 	= COALESCE(cte3i.performance_measurement_date, '1900-01-01') 	
	AND cte3i.customer_group 		= 'I'
LEFT JOIN CTE3 cte3c -- total break C/I
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte3c.campaign_id, '') 	
	AND COALESCE(master.cohort, '') 		= COALESCE(cte3c.cohort, '')
	AND COALESCE(master.performance_measurement_date, '1900-01-01') 	= COALESCE(cte3c.performance_measurement_date, '1900-01-01') 		
	AND cte3c.customer_group 		= 'C'	
LEFT JOIN CTE4 cte4i -- object
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte4i.campaign_id, '') 	
	AND COALESCE(master.objective_id, '') 	= COALESCE(cte4i.objective_id, '') 	 
	AND COALESCE(master.cohort, '') 		= COALESCE(cte4i.cohort, '')
	AND COALESCE(master.performance_measurement_date, '1900-01-01') 	= COALESCE(cte4i.performance_measurement_date, '1900-01-01') 	
	AND cte4i.customer_group 		= 'I'		
LEFT JOIN CTE4 cte4c -- object
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte4c.campaign_id, '') 	
	AND COALESCE(master.objective_id, '') 	= COALESCE(cte4c.objective_id, '') 	
	AND COALESCE(master.cohort, '') 		= COALESCE(cte4c.cohort, '') 
	AND COALESCE(master.performance_measurement_date, '1900-01-01') 	= COALESCE(cte4c.performance_measurement_date, '1900-01-01') 	
	AND cte4c.customer_group 		= 'C'	
LEFT JOIN CTE5 cte5 -- object
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte5.campaign_id, '') 	
	AND COALESCE(master.objective_id, '') 	= COALESCE(cte5.objective_id, '') 	
	AND COALESCE(master.cohort, '') 		= COALESCE(cte5.cohort, '') 
	AND COALESCE(master.performance_measurement_date, '1900-01-01') 	= COALESCE(cte5.performance_measurement_date, '1900-01-01') 	
LEFT JOIN CTE6 cte6 -- object
	ON COALESCE(master.campaign_id, '')	    = COALESCE(cte6.campaign_id, '') 	
	AND COALESCE(master.objective_id, '') 	= COALESCE(cte6.objective_id, '') 
	AND COALESCE(master.cohort, '') 		= COALESCE(cte6.cohort, '')	 
	AND COALESCE(master.performance_measurement_date, '1900-01-01') 	= COALESCE(cte6.performance_measurement_date, '1900-01-01') 	
; 	

DROP TABLE IF EXISTS reporting.temp_rpt_campaign_objective_interactions ; 
CREATE TABLE reporting.temp_rpt_campaign_objective_interactions  AS 
WITH CTE AS ( 
	SELECT '2a' AS NBR 
		,tmp2a.grain 
		,tmp2a.campaign_id 
		,tmp2a.campaign_portfolio 
		,CAST (NULL AS VARCHAR) AS channel 
		,CAST (NULL AS VARCHAR) AS action
		,CAST (NULL AS VARCHAR) AS touchpoint 
		,CAST (NULL AS VARCHAR) AS cohort 
		,CAST (NULL AS DATE)	AS performance_measurement_date 
		,CAST (NULL AS VARCHAR) AS objective_id 
		,CAST (NULL AS VARCHAR) AS objective_name 
		,CAST (NULL AS VARCHAR) AS objective_rank 
		,CAST (NULL AS BOOLEAN) AS objective_is_positive 
		,tmp2a.cntd_cust 
		,tmp2a.cntd_comm 
		,tmp2a.cntd_cohort 
		,tmp2a.cntd_cust_contact 
		,tmp2a.cntd_comm_contact 
		,tmp2a.cntd_cust_i 
		,tmp2a.cntd_comm_i 
		,tmp2a.cntd_cohort_i 
		,tmp2a.cntd_cust_c 
		,tmp2a.cntd_comm_c
		,tmp2a.cntd_cohort_c
		,tmp2a.cntd_cust_convert
		,tmp2a.cntd_comm_convert 
		,tmp2a.cntd_cust_i_convert 
		,tmp2a.cntd_comm_i_convert 
		,tmp2a.cntd_cust_c_convert
		,tmp2a.cntd_comm_c_convert 
		,tmp2a.cntd_cust_convert_after_contact 
		,tmp2a.cntd_comm_convert_after_contact 
FROM reporting.temp_rpt_agg_2a tmp2a
UNION ALL  
SELECT '3b' AS NBR 
		,tmp3b.grain 
		,tmp3b.campaign_id 
		,tmp3b.campaign_portfolio 
		,CAST (NULL AS VARCHAR) AS channel 
		,CAST (NULL AS VARCHAR) AS action
		,CAST (NULL AS VARCHAR) AS touchpoint 
		,tmp3b.cohort 
		,CAST (NULL AS DATE)	AS performance_measurement_date 
		,CAST (NULL AS VARCHAR) AS objective_id 
		,CAST (NULL AS VARCHAR) AS objective_name 
		,CAST (NULL AS VARCHAR) AS objective_rank 
		,CAST (NULL AS BOOLEAN) AS objective_is_positive 
		,tmp3b.cntd_cust 
		,tmp3b.cntd_comm 
		,tmp3b.cntd_cohort 
		,tmp3b.cntd_cust_contact 
		,tmp3b.cntd_comm_contact 
		,tmp3b.cntd_cust_i 
		,tmp3b.cntd_comm_i 
		,tmp3b.cntd_cohort_i 
		,tmp3b.cntd_cust_c 
		,tmp3b.cntd_comm_c
		,tmp3b.cntd_cohort_c
		,tmp3b.cntd_cust_convert
		,tmp3b.cntd_comm_convert 
		,tmp3b.cntd_cust_i_convert 
		,tmp3b.cntd_comm_i_convert 
		,tmp3b.cntd_cust_c_convert
		,tmp3b.cntd_comm_c_convert 
		,tmp3b.cntd_cust_convert_after_contact 
		,tmp3b.cntd_comm_convert_after_contact 
FROM reporting.temp_rpt_agg_3b tmp3b
UNION ALL  
SELECT '4a' AS NBR 
		,tmp4a.grain 
		,tmp4a.campaign_id 
		,tmp4a.campaign_portfolio 
		,tmp4a.channel 
		,tmp4a.action
		,tmp4a.touchpoint 
		,CAST (NULL AS VARCHAR) AS cohort 
		,CAST (NULL AS DATE)	AS performance_measurement_date 
		,CAST (NULL AS VARCHAR) AS objective_id 
		,CAST (NULL AS VARCHAR) AS objective_name 
		,CAST (NULL AS VARCHAR) AS objective_rank 
		,CAST (NULL AS BOOLEAN) AS objective_is_positive 
		,tmp4a.cntd_cust 
		,tmp4a.cntd_comm 
		,tmp4a.cntd_cohort 
		,tmp4a.cntd_cust_contact 
		,tmp4a.cntd_comm_contact 
		,tmp4a.cntd_cust_i 
		,tmp4a.cntd_comm_i 
		,tmp4a.cntd_cohort_i 
		,tmp4a.cntd_cust_c 
		,tmp4a.cntd_comm_c
		,tmp4a.cntd_cohort_c
		,tmp4a.cntd_cust_convert
		,tmp4a.cntd_comm_convert 
		,tmp4a.cntd_cust_i_convert 
		,tmp4a.cntd_comm_i_convert 
		,tmp4a.cntd_cust_c_convert
		,tmp4a.cntd_comm_c_convert 
		,tmp4a.cntd_cust_convert_after_contact 
		,tmp4a.cntd_comm_convert_after_contact 
FROM reporting.temp_rpt_agg_4a tmp4a
UNION ALL  
SELECT '4c' AS NBR 
		,tmp4c.grain 
		,tmp4c.campaign_id 
		,tmp4c.campaign_portfolio 
		,CAST (NULL AS VARCHAR) AS channel 
		,CAST (NULL AS VARCHAR) AS action
		,CAST (NULL AS VARCHAR) AS touchpoint 
		,tmp4c.cohort 
		,CAST (NULL AS DATE)	AS performance_measurement_date 
		,tmp4c.objective_id 
		,tmp4c.objective_name 
		,tmp4c.objective_rank 
		,tmp4c.objective_is_positive 
		,tmp4c.cntd_cust 
		,tmp4c.cntd_comm 
		,tmp4c.cntd_cohort 
		,tmp4c.cntd_cust_contact 
		,tmp4c.cntd_comm_contact 
		,tmp4c.cntd_cust_i 
		,tmp4c.cntd_comm_i 
		,tmp4c.cntd_cohort_i 
		,tmp4c.cntd_cust_c 
		,tmp4c.cntd_comm_c
		,tmp4c.cntd_cohort_c
		,tmp4c.cntd_cust_convert
		,tmp4c.cntd_comm_convert 
		,tmp4c.cntd_cust_i_convert 
		,tmp4c.cntd_comm_i_convert 
		,tmp4c.cntd_cust_c_convert
		,tmp4c.cntd_comm_c_convert 
		,tmp4c.cntd_cust_convert_after_contact 
		,tmp4c.cntd_comm_convert_after_contact 
FROM reporting.temp_rpt_agg_4c tmp4c
UNION ALL  
SELECT '5b' AS NBR 
		,tmp5b.grain 
		,tmp5b.campaign_id 
		,tmp5b.campaign_portfolio 
		,tmp5b.channel 
		,tmp5b.action
		,tmp5b.touchpoint 
		,CAST (NULL AS VARCHAR)	AS cohort 
		,CAST (NULL AS DATE)	AS performance_measurement_date 
		,tmp5b.objective_id 
		,tmp5b.objective_name 
		,tmp5b.objective_rank 
		,tmp5b.objective_is_positive 
		,tmp5b.cntd_cust 
		,tmp5b.cntd_comm 
		,tmp5b.cntd_cohort 
		,tmp5b.cntd_cust_contact 
		,tmp5b.cntd_comm_contact 
		,tmp5b.cntd_cust_i 
		,tmp5b.cntd_comm_i 
		,tmp5b.cntd_cohort_i 
		,tmp5b.cntd_cust_c 
		,tmp5b.cntd_comm_c
		,tmp5b.cntd_cohort_c
		,tmp5b.cntd_cust_convert
		,tmp5b.cntd_comm_convert 
		,tmp5b.cntd_cust_i_convert 
		,tmp5b.cntd_comm_i_convert 
		,tmp5b.cntd_cust_c_convert
		,tmp5b.cntd_comm_c_convert 
		,tmp5b.cntd_cust_convert_after_contact 
		,tmp5b.cntd_comm_convert_after_contact 
FROM reporting.temp_rpt_agg_5b tmp5b
UNION ALL  
SELECT '4d' AS NBR 
		,tmp4d.grain 
		,tmp4d.campaign_id 
		,tmp4d.campaign_portfolio 
		,CAST (NULL AS VARCHAR)	AS channel 
		,CAST (NULL AS VARCHAR)	AS action
		,CAST (NULL AS VARCHAR)	AS touchpoint 
		,CAST (NULL AS VARCHAR)	AS cohort 
		,tmp4d.performance_measurement_date 
		,tmp4d.objective_id 
		,tmp4d.objective_name 
		,tmp4d.objective_rank 
		,tmp4d.objective_is_positive 
		,tmp4d.cntd_cust 
		,tmp4d.cntd_comm 
		,tmp4d.cntd_cohort 
		,tmp4d.cntd_cust_contact 
		,tmp4d.cntd_comm_contact 
		,tmp4d.cntd_cust_i 
		,tmp4d.cntd_comm_i 
		,tmp4d.cntd_cohort_i 
		,tmp4d.cntd_cust_c 
		,tmp4d.cntd_comm_c
		,tmp4d.cntd_cohort_c
		,tmp4d.cntd_cust_convert
		,tmp4d.cntd_comm_convert 
		,tmp4d.cntd_cust_i_convert 
		,tmp4d.cntd_comm_i_convert 
		,tmp4d.cntd_cust_c_convert
		,tmp4d.cntd_comm_c_convert 
		,tmp4d.cntd_cust_convert_after_contact 
		,tmp4d.cntd_comm_convert_after_contact 
FROM reporting.temp_rpt_agg_4d tmp4d
UNION ALL  
SELECT '5c' AS NBR 
		,tmp5c.grain 
		,tmp5c.campaign_id 
		,tmp5c.campaign_portfolio 
		,CAST (NULL AS VARCHAR)	AS channel 
		,CAST (NULL AS VARCHAR)	AS action
		,CAST (NULL AS VARCHAR)	AS touchpoint 
		,tmp5c.cohort 
		,tmp5c.performance_measurement_date 
		,tmp5c.objective_id 
		,tmp5c.objective_name 
		,tmp5c.objective_rank 
		,tmp5c.objective_is_positive 
		,tmp5c.cntd_cust 
		,tmp5c.cntd_comm 
		,tmp5c.cntd_cohort 
		,tmp5c.cntd_cust_contact 
		,tmp5c.cntd_comm_contact 
		,tmp5c.cntd_cust_i 
		,tmp5c.cntd_comm_i 
		,tmp5c.cntd_cohort_i 
		,tmp5c.cntd_cust_c 
		,tmp5c.cntd_comm_c
		,tmp5c.cntd_cohort_c
		,tmp5c.cntd_cust_convert
		,tmp5c.cntd_comm_convert 
		,tmp5c.cntd_cust_i_convert 
		,tmp5c.cntd_comm_i_convert 
		,tmp5c.cntd_cust_c_convert
		,tmp5c.cntd_comm_c_convert 
		,tmp5c.cntd_cust_convert_after_contact 
		,tmp5c.cntd_comm_convert_after_contact 
FROM reporting.temp_rpt_agg_5c tmp5c
) 
,cte2 AS ( 
SELECT agg.* 
		,COALESCE ( agg.cntd_cust_convert::NUMERIC / NULLIF (agg.cntd_cust, 0)::NUMERIC, 0) AS rt_convert_cust 
		,COALESCE ( agg.cntd_cust_i_convert::NUMERIC / NULLIF (agg.cntd_cust_i, 0)::NUMERIC, 0) AS rt_convert_cust_i  
		,COALESCE ( agg.cntd_cust_c_convert::NUMERIC / NULLIF (agg.cntd_cust_c, 0)::NUMERIC, 0) AS rt_convert_cust_c  
FROM cte agg
) 
,cte3 AS (
SELECT * 
		,CASE WHEN objective_is_positive IS TRUE THEN (rt_convert_cust_i - rt_convert_cust_c)::NUMERIC 
				WHEN objective_is_positive IS FALSE THEN (rt_convert_cust_c - rt_convert_cust_i)::NUMERIC
			ELSE 0 END AS INCREMENTAL_UPLIFT
FROM cte2 
)
,cte4 AS (
SELECT *
-- To think thrugh IF the rt_convert_cust_c is 0 then whether put denominotr to be 1 
		,CASE WHEN objective_is_positive IS TRUE THEN COALESCE ( CASE WHEN rt_convert_cust_c IS NULL OR rt_convert_cust_c = 0 THEN 1 ELSE Incremental_uplift::NUMERIC / NULLIF (rt_convert_cust_c,0) END::NUMERIC ,0) 
				WHEN objective_is_positive IS FALSE THEN COALESCE ( CASE WHEN rt_convert_cust_i IS NULL OR rt_convert_cust_i = 0 THEN 1 ELSE Incremental_uplift::NUMERIC /  NULLIF (rt_convert_cust_i,0) END::NUMERIC ,0) 
			ELSE 0 END AS RELATIVE_CONVERSION_RATE
		,CASE WHEN objective_is_positive IS TRUE THEN Incremental_uplift::NUMERIC * cntd_cust_i::NUMERIC
				WHEN objective_is_positive IS FALSE THEN  Incremental_uplift::NUMERIC * cntd_cust_i::NUMERIC
			ELSE 0 END AS INCREMENTAL_UPLIFT_NUM
		,CASE WHEN objective_is_positive IS TRUE THEN COALESCE (Incremental_uplift::NUMERIC * 365 * cntd_cust_i::NUMERIC/ NULLIF (cntd_cohort_i::NUMERIC, 0)::NUMERIC, 0)::NUMERIC
				WHEN objective_is_positive IS FALSE THEN COALESCE ( (Incremental_uplift::NUMERIC * 365 * cntd_cust_i::NUMERIC / NULLIF (cntd_cohort_i::NUMERIC, 0)::NUMERIC, 0))::NUMERIC
			ELSE 0 END AS ANNUALISED_UPLIFT_NUM		
FROM cte3
) 
SELECT CURRENT_TIMESTAMP AS _updated 
		,fin1.NBR 
		,fin1.grain 
		,fin1.campaign_id 
		,cam.campaign_name
		,cam.campaign_brand
		,fin1.campaign_portfolio 
		,fin1.channel 
		,fin1.action 
		,fin1.touchpoint 
		,fin1.cohort 
		,fin1.performance_measurement_date
		,fin1.objective_id 
		,fin1.objective_name 
		,fin1.objective_rank 
		,fin1.objective_is_positive 
		,CASE WHEN fin1.campaign_portfolio IS NOT NULL AND fin1.objective_name IS NOT NULL 
					THEN fin1.campaign_portfolio || ' - ' || fin1.objective_name
				WHEN fin1.objective_name IS NOT NULL THEN fin1.objective_name 
				WHEN fin1.campaign_portfolio IS NOT NULL THEN fin1.campaign_portfolio 
				ELSE NULL END::VARCHAR 						 											AS product_objective 		
		,CASE WHEN fin1.objective_rank IS NOT NULL AND fin1.objective_name IS NOT NULL 
					THEN fin1.objective_rank || ' - ' || fin1.objective_name
				ELSE NULL END::VARCHAR																	AS type_objective
		,fin1.cntd_cust 
		,fin1.cntd_comm 	
		,fin1.cntd_cohort 
		,fin1.cntd_cust_contact 
		,fin1.cntd_comm_contact 
		,fin1.cntd_cust_i 
		,fin1.cntd_comm_i 
		,fin1.cntd_cohort_i 
		,fin1.cntd_cust_c 
		,fin1.cntd_comm_c
		,fin1.cntd_cohort_c
		,fin1.cntd_cust_convert 
		,fin1.cntd_comm_convert 
		,fin1.cntd_cust_i_convert 
		,fin1.cntd_comm_i_convert 
		,fin1.cntd_cust_c_convert
		,fin1.cntd_comm_c_convert 
		,fin1.cntd_cust_convert_after_contact 
		,fin1.cntd_comm_convert_after_contact 
		,fin1.rt_convert_cust 
		,fin1.rt_convert_cust_i 
		,fin1.rt_convert_cust_c
		,COALESCE(fin1.RELATIVE_CONVERSION_RATE,0 )::NUMERIC	AS relative_conversion_rate
	    ,CASE WHEN COALESCE(fin1.RELATIVE_CONVERSION_RATE,0 ) < 0 THEN round(abs(COALESCE(fin1.RELATIVE_CONVERSION_RATE,0 )),2) || ' x worse' 
			    WHEN COALESCE(fin1.RELATIVE_CONVERSION_RATE,0 ) >= 0 THEN round(abs(COALESCE(fin1.RELATIVE_CONVERSION_RATE,0 )),2) || ' x better' 
			END::VARCHAR                                        AS relative_conversion_rate_indicator 
		,COALESCE(fin1.INCREMENTAL_UPLIFT, 0)::NUMERIC			AS incremental_uplift 
		,COALESCE(fin1.INCREMENTAL_UPLIFT_num, 0)::NUMERIC		AS incremental_uplift_num
		,COALESCE(fin1.ANNUALISED_UPLIFT_NUM, 0 )::NUMERIC		AS annualised_uplift_num 
FROM CTE4 fin1
INNER JOIN reporting.ref_campaign_vw cam 
	ON fin1.campaign_id = cam.campaign_id 
	AND cam.campaign_status = 'A'
; 

TRUNCATE TABLE reporting.rpt_campaign_objective_interactions; 

INSERT INTO reporting.rpt_campaign_objective_interactions (
	_updated 
	,nbr								
	,grain 								
	,campaign_id 						
	,campaign_name		
	,campaign_brand				
	,campaign_portfolio 				
	,channel 							
	,action 							
	,touchpoint 						
	,cohort 							
	,performance_measurement_date				
	,objective_id 						
	,objective_name 					
	,objective_rank 					
	,objective_is_positive				
    ,product_objective 		             
    ,type_objective                     		 
	,cntd_cust 							
	,cntd_comm 							
	,cntd_cohort 						
	,cntd_cust_contact 					
	,cntd_comm_contact 					
	,cntd_cust_i 						
	,cntd_comm_i 						
	,cntd_cohort_i 						
	,cntd_cust_c 						
	,cntd_comm_c						
	,cntd_cohort_c												
	,cntd_cust_convert 					
	,cntd_comm_convert 					
	,cntd_cust_i_convert 					
	,cntd_comm_i_convert 					
	,cntd_cust_c_convert					
	,cntd_comm_c_convert 				
	,cntd_cust_convert_after_contact	 
	,cntd_comm_convert_after_contact 	
	,rt_convert_cust 					
	,rt_convert_cust_i 					
	,rt_convert_cust_c					
	,relative_conversion_rate	
    ,relative_conversion_rate_indicator 			
	,incremental_uplift 				
	,incremental_uplift_num				
	,annualised_uplift_num 						
)
SELECT 
	_updated 
	,nbr								
	,grain 								
	,campaign_id 						
	,campaign_name	
	,campaign_brand					
	,campaign_portfolio 				
	,channel 							
	,action 							
	,touchpoint 						
	,cohort 							
	,performance_measurement_date					
	,objective_id 						
	,objective_name 					
	,objective_rank 					
	,objective_is_positive				
    ,product_objective 		             
    ,type_objective                 	 
	,cntd_cust 							
	,cntd_comm 							
	,cntd_cohort 						
	,cntd_cust_contact 					
	,cntd_comm_contact 					
	,cntd_cust_i 						
	,cntd_comm_i 						
	,cntd_cohort_i 						
	,cntd_cust_c 						
	,cntd_comm_c						
	,cntd_cohort_c												
	,cntd_cust_convert 					
	,cntd_comm_convert 					
	,cntd_cust_i_convert 					
	,cntd_comm_i_convert 					
	,cntd_cust_c_convert					
	,cntd_comm_c_convert 				
	,cntd_cust_convert_after_contact	 
	,cntd_comm_convert_after_contact 	
	,rt_convert_cust 					
	,rt_convert_cust_i 					
	,rt_convert_cust_c					
	,relative_conversion_rate	
    ,relative_conversion_rate_indicator 			
	,incremental_uplift 				
	,incremental_uplift_num				
	,annualised_uplift_num 				
FROM reporting.temp_rpt_campaign_objective_interactions; 

INSERT INTO reporting.log_campaign_performance (
  table_name
  ,session_id 
  ,max_updated_time
  ,run_timestamp
  ,rows_inserted
)
SELECT
  'rpt_campaign_objective_interaction'     	AS table_name
  ,pg_backend_pid()							AS session_id 
  ,MAX(_updated)                            AS max_updated_time                   
  ,CURRENT_TIMESTAMP                        AS run_timestamp
  ,COUNT(*)                                 AS rows_inserted
FROM reporting.temp_rpt_campaign_objective_interactions;

DROP TABLE IF EXISTS reporting.temp_rpt_agg_2a; 
DROP TABLE IF EXISTS reporting.temp_rpt_agg_3b; 
DROP TABLE IF EXISTS reporting.temp_rpt_agg_4a; 
DROP TABLE IF EXISTS reporting.temp_rpt_agg_4c; 
DROP TABLE IF EXISTS reporting.temp_rpt_agg_5b; 
DROP TABLE IF EXISTS reporting.temp_rpt_agg_4dd; 
DROP TABLE IF EXISTS reporting.temp_rpt_agg_4d; 
DROP TABLE IF EXISTS reporting.temp_rpt_agg_5cc; 
DROP TABLE IF EXISTS reporting.temp_rpt_agg_5c; 
DROP TABLE IF EXISTS reporting.temp_rpt_campaign_objective_interactions; 
