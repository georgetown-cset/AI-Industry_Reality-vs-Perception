/* 

Combined query for NER lists of companies in the Transportation and Warehousing sector

(Companies with above 0.5% job posting shares in the sector.)

*/

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

UNION ALL

--- DHL ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'DHL' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bDHL\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- AAA ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'AAA' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bAmerican Automobile Association\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- BCD Travel ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'BCD Travel' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bBCD Travel\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      AND NOT REGEXP_CONTAINS(entity.value,r"BCD Travel BCD Travel")
      AND NOT REGEXP_CONTAINS(entity.value,r"Fareportal")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Dematic ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Dematic' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bDematic\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      AND NOT REGEXP_CONTAINS(entity.value,r"Siemens")
      AND NOT REGEXP_CONTAINS(entity.value,r"Bastian Solutions")
      AND NOT REGEXP_CONTAINS(entity.value,r"Mannesmann")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- APL ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'APL' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bAmerican President Lines\b")
      AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      AND NOT REGEXP_CONTAINS(entity.value,r"Antipodes Global Investment Company")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- United Airlines ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'United Airlines' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bUnited Airlines\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- American Medical Response ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'American Medical Response' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE (REGEXP_CONTAINS(entity.value,r"\bAmerican Medical Response\b") OR REGEXP_CONTAINS(entity.value,r"\bAMR\b"))
      AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      AND NOT REGEXP_CONTAINS(entity.value,r"Current")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- C.H. Robinson Worldwide ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'C.H. Robinson' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE (REGEXP_CONTAINS(entity.value,r"\bC.H. Robinson\b") OR REGEXP_CONTAINS(entity.value,r"\bC. H. Robinson\b"))
      AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Kinder Morgan ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Kinder Morgan' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bKinder Morgan\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Royal Caribbean Cruise ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Royal Caribbean Cruise' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bRoyal Caribbean Cruise\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      AND NOT REGEXP_CONTAINS(entity.value,r"Wo")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Amtrak ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Amtrak' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bAmtrak\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Pure Storage ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Pure Storage' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bPure Storage\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- J.B. Hunt Transport ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'J.B. Hunt Transport' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE (REGEXP_CONTAINS(entity.value,r"\bJ.B. Hunt Transport\b") OR REGEXP_CONTAINS(entity.value,r"\bJB Hunt Transport\b"))
      AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Uber ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Uber' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bUber\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Expeditors ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Expeditors' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bExpeditors\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Cruise ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Cruise' as company_name, 'Transportation and Warehousing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE (entity.value = "Cruise LLC" OR REGEXP_CONTAINS(entity.value,r"\bGM Cruise\b") OR REGEXP_CONTAINS(entity.value,r"\bGeneral Motors Cruise\b"))
      AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent