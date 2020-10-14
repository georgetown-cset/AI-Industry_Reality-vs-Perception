/* 

Combined query for NER lists of companies in the Professional, Scientific, and Technical Services sector

(Companies with above 0.5% job posting shares in the sector.)

*/

WITH deloitte_t AS
  (SELECT 'Deloitte' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bDeloitte\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC), 
  
  ibm_t AS (SELECT 'IBM' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value,r'\bIBM\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC),
  
  accenture_t AS (SELECT 'Accenture' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bAccenture\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC),

  boozallen_t AS (SELECT 'Booz Allen Hamilton Inc.' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bBooz Allen Hamilton\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC),
  
  leidos_t AS (SELECT 'Leidos' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bLeidos\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC),

  saic_t AS (SELECT 'SAIC' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bSAIC\b') AND NOT REGEXP_CONTAINS(entity.value, r'\bMotor\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC),
  
  pwc_t AS (SELECT 'PricewaterhouseCoopers' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bPricewaterhouseCoopers\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC), 
  
  infosys_t AS (SELECT 'Infosys' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bInfosys\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC),
  
  caci_t AS (SELECT 'CACI' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value, r'\bCACI\b') AND entity.type = "Company"
  GROUP BY name, company_name, naics2
  ORDER BY count DESC), 
  
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

UNION ALL

--- KPMG ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'KPMG' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bKPMG\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Jacobs Engineering Group ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Jacobs Engineering Group' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bJacobs Engineering Group\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Wipro ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Wipro' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bWipro\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- AECOM Technology Corporation ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'AECOM' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bAECOM\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      AND NOT REGEXP_CONTAINS(entity.value,r"Gross Profit")
      AND NOT REGEXP_CONTAINS(entity.value,r"(?i)AECOM AECOM")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- IDC Technologies ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'IDC Technologies' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bIDC Technologies\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      AND NOT REGEXP_CONTAINS(entity.value,r"v.")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- ManTech ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'ManTech' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bManTech\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Capgemini ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Capgemini' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE (REGEXP_CONTAINS(entity.value,r"\bCapgemini\b") OR REGEXP_CONTAINS(entity.value,r"\bCap Gemini\b")) 
      AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      AND NOT REGEXP_CONTAINS(entity.value,r"Profit")
      AND NOT REGEXP_CONTAINS(entity.value,r"Press")
      AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Source")
      AND NOT REGEXP_CONTAINS(entity.value,r"Foodstuff")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Fast Switch ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Fast Switch' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bFast Switch\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- CGI Group ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'CGI' as company_name, 'Professional, Scientific, and Technical Services' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bCGI\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      AND NOT REGEXP_CONTAINS(entity.value,r"(?i)SOURCE")
      AND NOT REGEXP_CONTAINS(entity.value,r"GroupCGI")
      AND NOT REGEXP_CONTAINS(entity.value,r"CGI Group CGI Group")
      AND NOT REGEXP_CONTAINS(entity.value,r"Glass Lewis")
      AND NOT REGEXP_CONTAINS(entity.value,r"360")
      AND NOT REGEXP_CONTAINS(entity.value,r"Profit")
      AND NOT REGEXP_CONTAINS(entity.value,r"Merchant Group")
      AND NOT REGEXP_CONTAINS(entity.value,r"Pharmaceuticals")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
