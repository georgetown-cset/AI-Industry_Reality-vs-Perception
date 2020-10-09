WITH fedex_t AS
  (SELECT 'FedEx' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bFedEx\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC), 
  
  uber_t AS (SELECT 'Uber' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value,r'\bUber\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC),
  
  usps_t AS (SELECT 'United Parcel Service Incorporated' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bUnited Parcel Service\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC),

  aa_t AS (SELECT 'American Airlines' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bAmerican Airlines\b') AND NOT REGEXP_CONTAINS(entity.value, r'\bArena\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC),
  
  southwest_t AS (SELECT 'Southwest Airlines' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bSouthwest Airlines\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC),

  delta_t AS (SELECT 'Delta Air Lines' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bDelta Air Lines\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC),
  
  ceva_t AS (SELECT 'CEVA Logistics' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bCEVA Logistics\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC)

SELECT * FROM( 
  SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent FROM(
    SELECT * FROM (
      SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage FROM(
        SELECT *, (0.0+count)/SUM(count) OVER () AS percentage FROM fedex_t
          GROUP BY count, name, company_name, naics2
          ORDER BY percentage DESC))))
WHERE cumulative_percentage <= threshold_percent 

UNION ALL 

SELECT * FROM( 
  SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent FROM(
    SELECT * FROM (
      SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage FROM(
        SELECT *, (0.0+count)/SUM(count) OVER () AS percentage FROM uber_t
          GROUP BY count, name, company_name, naics2
          ORDER BY percentage DESC))))
WHERE cumulative_percentage <= threshold_percent 

UNION ALL 

SELECT * FROM( 
  SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent FROM(
    SELECT * FROM (
      SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage FROM(
        SELECT *, (0.0+count)/SUM(count) OVER () AS percentage FROM usps_t
          GROUP BY count, name, company_name, naics2
          ORDER BY percentage DESC))))
WHERE cumulative_percentage <= threshold_percent 

UNION ALL 

SELECT * FROM( 
  SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent FROM(
    SELECT * FROM (
      SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage FROM(
        SELECT *, (0.0+count)/SUM(count) OVER () AS percentage FROM aa_t
          GROUP BY count, name, company_name, naics2
          ORDER BY percentage DESC))))
WHERE cumulative_percentage <= threshold_percent 

UNION ALL 

SELECT * FROM( 
  SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent FROM(
    SELECT * FROM (
      SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage FROM(
        SELECT *, (0.0+count)/SUM(count) OVER () AS percentage FROM southwest_t
          GROUP BY count, name, company_name, naics2
          ORDER BY percentage DESC))))
WHERE cumulative_percentage <= threshold_percent 

UNION ALL 

SELECT * FROM( 
  SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent FROM(
    SELECT * FROM (
      SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage FROM(
        SELECT *, (0.0+count)/SUM(count) OVER () AS percentage FROM delta_t
          GROUP BY count, name, company_name, naics2
          ORDER BY percentage DESC))))
WHERE cumulative_percentage <= threshold_percent 

UNION ALL 

SELECT * FROM( 
  SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent FROM(
    SELECT * FROM (
      SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage FROM(
        SELECT *, (0.0+count)/SUM(count) OVER () AS percentage FROM ceva_t
          GROUP BY count, name, company_name, naics2
          ORDER BY percentage DESC))))
WHERE cumulative_percentage <= threshold_percent 
