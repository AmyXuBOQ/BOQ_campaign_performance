/**********************************************************************/
/**********                   REF_CAMPAIGN                   **********/
/**********************************************************************/

DROP VIEW IF EXISTS reporting.ref_campaign_vw;

CREATE VIEW reporting.ref_campaign_vw AS
SELECT
  _updated AS TIMESTAMP,
  _filename,
  CAST(id AS INT) AS id,
  campaign_id,
  campaign_name,
  campaign_brand,
  campaign_desc,
  campaign_portfolio,
  TO_TIMESTAMP(campaign_start_date, 'DD/MM/YYYY') AS campaign_start_date,
  TO_TIMESTAMP(campaign_end_date, 'DD/MM/YYYY') AS campaign_end_date,
  campaign_status,
  TO_DATE(SUBSTRING(_filename FROM '[0-9]{8}'), 'YYYYMMDD') AS date_insertion
FROM dw."CDP_ref_campaign";

SELECT 'VIEW CREATED' AS status;

/**********************************************************************/
/**********                  REF_OBJECTIVE                   **********/
/**********************************************************************/

DROP VIEW IF EXISTS reporting.ref_objective_vw;

CREATE VIEW reporting.ref_campaign_objective_vw AS
SELECT
  _updated AS TIMESTAMP,
  _filename,
  CAST(id AS INT) AS id,
  campaign_objective_id,
  campaign_id,
  objective_id,
  objective_rank,
  capture_period::INTERVAL AS capture_period,
  campaign_objective_status,
  TO_DATE(SUBSTRING(_filename FROM '[0-9]{8}'), 'YYYYMMDD') AS date_insertion
FROM dw."CDP_ref_campaign_objective";
SELECT 'VIEW CREATED' AS status;

/**********************************************************************/
/**********             REF_CAMPAIGN_OBJECTIVE               **********/
/**********************************************************************/

DROP VIEW IF EXISTS reporting.ref_campaign_objective_vw;

CREATE VIEW reporting.ref_campaign_objective_vw AS
SELECT
  _updated AS TIMESTAMP,
  _filename,
  CAST(id AS INT) AS id,
  campaign_objective_id,
  campaign_id,
  objective_id,
  objective_rank,
  capture_period::INTERVAL AS capture_period,
  campaign_objective_status,
  TO_DATE(SUBSTRING(_filename FROM '[0-9]{8}'), 'YYYYMMDD') AS date_insertion
FROM dw."CDP_ref_campaign_objective";
SELECT 'VIEW CREATED' AS status;

/**********************************************************************/
/**********              REF_CAMPAIGN_TOUCHPOINT             **********/
/**********************************************************************/

DROP VIEW IF EXISTS reporting.ref_campaign_touchpoint_vw;

CREATE VIEW reporting.ref_campaign_touchpoint_vw AS
SELECT
  _updated AS TIMESTAMP,
  _filename,
  CAST(id AS INT) AS id,
  campaign_id,
  campaign_name,
  action,
  touchpoint,
  channel,
  touchpoint_status,
  TO_DATE(SUBSTRING(_filename FROM '[0-9]{8}'), 'YYYYMMDD') AS date_insertion
FROM dw."CDP_ref_campaign_touchpoint";

SELECT 'VIEW CREATED' AS status;