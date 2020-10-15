/* 
Combined query for NER lists of companies in the Information sector
(Companies with above 0.5% job posting shares in the sector.)
*/

-- Deloitte -- 
WITH deloitte_t AS
  (SELECT 'Deloitte' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bDeloitte\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC), 
  
-- IBM --                                                                                                           --
  ibm_t AS (SELECT 'IBM' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value,r'\bIBM\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC),

-- Accenture --                                                                                                              
  accenture_t AS (SELECT 'Accenture' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bAccenture\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC),

-- Booze Allen Hamilton --                                                                                                                           
  boozallen_t AS (SELECT 'Booz Allen Hamilton Inc.' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bBooz Allen Hamilton\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC),
 
-- Leidos --                                                                                                                                         
  leidos_t AS (SELECT 'Leidos' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bLeidos\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC),

-- SAIC --                                                                                                                     
  saic_t AS (SELECT 'SAIC' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bSAIC\b') AND NOT REGEXP_CONTAINS(entity.value, r'\bMotor\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC),
  
-- PricewaterhouseCoopers --                                                                                                                
  pwc_t AS (SELECT 'PricewaterhouseCoopers' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bPricewaterhouseCoopers\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC), 
 
-- Infosys --                                                                                                                                  
  infosys_t AS (SELECT 'Infosys' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bInfosys\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC),
  
 -- CACI --                                                                                                                      
  caci_t AS (SELECT 'CACI' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bCACI\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC), 
                                                                                                                
-- NTT Data --   
  ntt_t AS (SELECT 'NTT Data' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bNTT Data\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC)

SELECT * FROM( 
  SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent FROM(
    SELECT * FROM (
      SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage FROM(
        SELECT *, (0.0+count)/SUM(count) OVER () AS percentage FROM deloitte_t
          GROUP BY count, name, company_name, naics2
          ORDER BY percentage DESC))))
WHERE cumulative_percentage <= threshold_percent 

UNION ALL 

SELECT * FROM( 
  SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent FROM(
    SELECT * FROM (
      SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage FROM(
        SELECT *, (0.0+count)/SUM(count) OVER () AS percentage FROM ibm_t
          GROUP BY count, name, company_name, naics2
          ORDER BY percentage DESC))))
WHERE cumulative_percentage <= threshold_percent 

UNION ALL 

SELECT * FROM( 
  SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent FROM(
    SELECT * FROM (
      SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage FROM(
        SELECT *, (0.0+count)/SUM(count) OVER () AS percentage FROM accenture_t
          GROUP BY count, name, company_name, naics2
          ORDER BY percentage DESC))))
WHERE cumulative_percentage <= threshold_percent 

UNION ALL 

SELECT * FROM( 
  SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent FROM(
    SELECT * FROM (
      SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage FROM(
        SELECT *, (0.0+count)/SUM(count) OVER () AS percentage FROM boozallen_t
          GROUP BY count, name, company_name, naics2
          ORDER BY percentage DESC))))
WHERE cumulative_percentage <= threshold_percent 

UNION ALL 

SELECT * FROM( 
  SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent FROM(
    SELECT * FROM (
      SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage FROM(
        SELECT *, (0.0+count)/SUM(count) OVER () AS percentage FROM leidos_t
          GROUP BY count, name, company_name, naics2
          ORDER BY percentage DESC))))
WHERE cumulative_percentage <= threshold_percent 

UNION ALL 

SELECT * FROM( 
  SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent FROM(
    SELECT * FROM (
      SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage FROM(
        SELECT *, (0.0+count)/SUM(count) OVER () AS percentage FROM saic_t
          GROUP BY count, name, company_name, naics2
          ORDER BY percentage DESC))))
WHERE cumulative_percentage <= threshold_percent 

UNION ALL 

SELECT * FROM( 
  SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent FROM(
    SELECT * FROM (
      SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage FROM(
        SELECT *, (0.0+count)/SUM(count) OVER () AS percentage FROM pwc_t
          GROUP BY count, name, company_name, naics2
          ORDER BY percentage DESC))))
WHERE cumulative_percentage <= threshold_percent 

UNION ALL 

SELECT * FROM( 
  SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent FROM(
    SELECT * FROM (
      SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage FROM(
        SELECT *, (0.0+count)/SUM(count) OVER () AS percentage FROM infosys_t
          GROUP BY count, name, company_name, naics2
          ORDER BY percentage DESC))))
WHERE cumulative_percentage <= threshold_percent 

UNION ALL 

SELECT * FROM( 
  SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent FROM(
    SELECT * FROM (
      SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage FROM(
        SELECT *, (0.0+count)/SUM(count) OVER () AS percentage FROM caci_t
          GROUP BY count, name, company_name, naics2
          ORDER BY percentage DESC))))
WHERE cumulative_percentage <= threshold_percent 

UNION ALL 

SELECT * FROM( 
  SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent FROM(
    SELECT * FROM (
      SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage FROM(
        SELECT *, (0.0+count)/SUM(count) OVER () AS percentage FROM ntt_t
          GROUP BY count, name, company_name, naics2
          ORDER BY percentage DESC))))
WHERE cumulative_percentage <= threshold_percent 
