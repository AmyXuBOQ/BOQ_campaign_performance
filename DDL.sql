/******** LOG ***********/ 

-- APPEND 
DROP TABLE IF EXISTS reporting.log_campaign_performance;
CREATE TABLE IF NOT EXISTS reporting.log_campaign_performance (
    run_id                     BIGSERIAL PRIMARY KEY
    ,table_name                VARCHAR
    ,session_id                INTEGER 
    ,max_updated_time          TIMESTAMP
    ,run_timestamp             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ,rows_inserted             INTEGER
    ,distinct_customers        INTEGER
    ,distinct_communications   INTEGER 
)
;

/******** STAGING TABLE ***********/ 

/*******************************************************/ 
/*** reporting.stg_campaign_interactions **************/ 
/*****************************************************/ 

-- APPEND
DROP TABLE IF EXISTS reporting.stg_campaign_interactions;
CREATE TABLE IF NOT EXISTS reporting.stg_campaign_interactions (
    _updated                    TIMESTAMP
    ,customer_id                VARCHAR
	,hashed_cif                 VARCHAR
    ,hashed_boq_cif             VARCHAR
    ,hashed_me_cif              VARCHAR
    ,hashed_vma_cif             VARCHAR
    ,hashed_boq_can             VARCHAR
    ,hashed_mobile_phone_v1     VARCHAR
    ,hashed_mobile_phone_v2     VARCHAR
    ,hashed_email_address       VARCHAR
    ,campaign_name              VARCHAR
    ,campaign_portfolio         VARCHAR
    ,campaign_start_date        DATE
    ,brand                      VARCHAR
    ,control_group              VARCHAR
    ,communication_id           VARCHAR
    ,communication_date         DATE
    ,channel_name               VARCHAR
    ,touchpoint                 VARCHAR
    ,first_exposure_date        DATE
    ,opens                      INTEGER 
    ,clicks                     INTEGER 
    ,unsubscribes               INTEGER 
    ,delivery_label             VARCHAR 
    ,delivery_type              VARCHAR 
    ,purpose                    VARCHAR 
    ,status                     VARCHAR 
	,contact_history_updated    TIMESTAMP
	,customer_updated           TIMESTAMP 
)
;

/*********************************************************/ 
/*** reporting.stg_objectives_interactions **************/ 
/*******************************************************/ 

-- APPEND
DROP TABLE IF EXISTS reporting.stg_objective_interactions;
CREATE TABLE IF NOT EXISTS reporting.stg_objective_interactions (
    _updated                       TIMESTAMP
    ,customer_id                   VARCHAR
    ,campaign_name                 VARCHAR
    ,campaign_portfolio            VARCHAR
    ,control_group                 VARCHAR
    ,communication_id              VARCHAR
    ,communication_date            DATE
    ,first_exposure_date           DATE
    ,objective_met_date            DATE
    ,objective_id                  VARCHAR
    ,objective_capture_period      VARCHAR
    ,objective_rank                VARCHAR
    ,objective_is_positive         BOOLEAN
    ,source                        VARCHAR 
    ,pk                            VARCHAR
)
;

/*******************************************************/ 
/*** reporting.stg_objective_campaign_comm_date_bound */ 
/*****************************************************/ 

DROP TABLE IF EXISTS reporting.stg_objective_campaign_comm_date_bound; 
CREATE TABLE IF NOT EXISTS reporting.stg_objective_campaign_comm_date_bound ( 
    _updated                TIMESTAMP
    ,section 				VARCHAR
	,campaign_objective_id	VARCHAR
	,campaign_id			VARCHAR
	,objective_id			VARCHAR
	,cohort_type			VARCHAR 				
	,capture_period 		INTERVAL		
	,lower_bound_comm_date 	DATE
	,upper_bound_comm_date 	DATE 
)
;

/******************************************************************/ 
/*** reporting.stg_campaign_objective_interactions ***************/ 
/****************************************************************/ 

DROP TABLE IF EXISTS reporting.stg_campaign_objective_interactions; 
CREATE TABLE IF NOT EXISTS reporting.stg_campaign_objective_interactions ( 
	_updated				    TIMESTAMP
	,campaign_id 			    VARCHAR	
	,campaign_name 				VARCHAR
	,campaign_portfolio 		VARCHAR
	,customer_id 				VARCHAR
    ,brand                      VARCHAR
	,communication_id 			VARCHAR
	,communication_date 		DATE
	,customer_group				VARCHAR 
	,channel_name 				VARCHAR
	,touchpoint					VARCHAR
	,first_exposure_date		DATE
	,objective_id				VARCHAR
	,objective_rank 			VARCHAR
	,objective_is_positive		BOOLEAN
	,objective_met_date			DATE
	,cohort 					VARCHAR
	,capture_period 			INTERVAL 
	,objective_conv_after_contact  	BOOLEAN
)
;


/******** REPORTING TABLE ***********/ 

/*****************************************************************/ 
/******** reporting.rpt_campaign_objective_interactions *********/ 
/***************************************************************/

DROP TABLE IF EXISTS reporting.rpt_campaign_objective_interactions; 
CREATE TABLE IF NOT EXISTS reporting.rpt_campaign_objective_interactions (
    _updated                            TIMESTAMP 
    ,nbr							    VARCHAR
    ,grain 								VARCHAR
    ,campaign_id 						VARCHAR
    ,campaign_name						VARCHAR
    ,campaign_brand                     VARCHAR
    ,campaign_portfolio 				VARCHAR
    ,channel 							VARCHAR
    ,action 							VARCHAR
    ,touchpoint 						VARCHAR
    ,cohort 							VARCHAR
    ,performance_measurement_date		Date
    ,objective_id 						VARCHAR
    ,objective_name 					VARCHAR
    ,objective_rank 					VARCHAR
    ,objective_is_positive				BOOLEAN
    ,product_objective 		            VARCHAR 
    ,type_objective                     VARCHAR
    ,cntd_cust 							NUMERIC
    ,cntd_comm 							NUMERIC
    ,cntd_cohort 						NUMERIC
    ,cntd_cust_contact 					NUMERIC
    ,cntd_comm_contact 					NUMERIC
    ,cntd_cust_i 						NUMERIC
    ,cntd_comm_i 						NUMERIC
    ,cntd_cohort_i 						NUMERIC
    ,cntd_cust_c 						NUMERIC
    ,cntd_comm_c						NUMERIC
    ,cntd_cohort_c						NUMERIC
    ,cntd_cust_convert 					NUMERIC
    ,cntd_comm_convert 					NUMERIC
    ,cntd_cust_i_convert 				NUMERIC	
    ,cntd_comm_i_convert 				NUMERIC	
    ,cntd_cust_c_convert				NUMERIC	
    ,cntd_comm_c_convert 				NUMERIC
    ,cntd_cust_convert_after_contact	NUMERIC 
    ,cntd_comm_convert_after_contact 	NUMERIC
    ,rt_convert_cust 					NUMERIC
    ,rt_convert_cust_i 					NUMERIC
    ,rt_convert_cust_c					NUMERIC
    ,relative_conversion_rate			NUMERIC	
    ,relative_conversion_rate_indicator VARCHAR
    ,incremental_uplift 				NUMERIC
    ,incremental_uplift_num				NUMERIC
    ,annualised_uplift_num 				NUMERIC
)
;


/*************************************************************/ 
/******** reporting.rpt_campaign_performance_overview *******/ 
/***********************************************************/

DROP TABLE IF EXISTS reporting.rpt_campaign_performance_overview; 
CREATE TABLE IF NOT EXISTS reporting.rpt_campaign_performance_overview (
    _updated 								TIMESTAMP
    ,campaign_id                            VARCHAR
    ,campaign_name 							VARCHAR
    ,campaign_brand                         VARCHAR
    ,campaign_portfolio						VARCHAR
    ,objective_name  						VARCHAR
    ,objective_type 						VARCHAR
    ,product_objective						VARCHAR
    ,type_objective                         VARCHAR
    ,intended 								BOOLEAN
    ,total_customers_per_campaign  			NUMERIC
    ,total_customers_per_group 				NUMERIC
    ,intervention_conversion_rate			NUMERIC
    ,relative_conversion_rate				NUMERIC 
    ,relative_conversion_rate_indicator 	VARCHAR	
    ,incremental_uplift 					NUMERIC
    ,incremental_uplift_num 				NUMERIC
    ,annualised_uplift_num 					NUMERIC
    ,test_stats 							NUMERIC
    ,mean                                   NUMERIC 
    ,standard_dev                           NUMERIC
)
;


/*************************************************************/ 
/******** reporting.rpt_campaign_drilldown ******************/ 
/***********************************************************/

DROP TABLE IF EXISTS reporting.rpt_campaign_drilldown; 
CREATE TABLE IF NOT EXISTS reporting.rpt_campaign_drilldown (
    _updated                                TIMESTAMP 
    ,campaign_id                            VARCHAR
    ,campaign_name                          VARCHAR
    ,campaign_brand                         VARCHAR 
    ,campaign_portfolio                     VARCHAR
    ,channel                                VARCHAR
    ,action                                 VARCHAR
    ,touchpoint                             VARCHAR
    ,objective_name                         VARCHAR
    ,product_objective                      VARCHAR 
    ,communication_sent_num                 NUMERIC
    ,cust_communication_sent_num            NUMERIC
    ,communication_contacted_num            NUMERIC
    ,cust_contacted_num                     NUMERIC 
    ,conv_communication_after_contact_num   NUMERIC
    ,conv_cust_after_contact_num            NUMERIC
)
;


/****************************************************************/ 
/******** reporting.rpt_incremental_uplift *********************/ 
/**************************************************************/

DROP TABLE IF EXISTS reporting.rpt_incremental_uplift; 
CREATE TABLE IF NOT EXISTS reporting.rpt_incremental_uplift (
    _updated							TIMESTAMP
    ,performance_measurement_date		DATE
    ,cal_type							VARCHAR
    ,campaign_id						VARCHAR
    ,campaign_name						VARCHAR
    ,campaign_brand						VARCHAR
    ,campaign_portfolio					VARCHAR
    ,objective_name						VARCHAR
    ,objective_type 					VARCHAR
    ,product_objective					VARCHAR
    ,type_objective						VARCHAR
    ,incremental_uplift 				NUMERIC
    ,incremental_uplift_complete_total	NUMERIC	
    ,incremental_uplift_total			NUMERIC
    ,date_total							DATE
    ,relative_conversion_rate			NUMERIC
    ,relative_conversion_rate_indicator	VARCHAR
    ,incremental_uplift_num				NUMERIC
    ,annualised_uplift_num				NUMERIC
    ,test_stats							NUMERIC
)
;
