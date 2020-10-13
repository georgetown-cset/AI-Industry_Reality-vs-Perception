/* 

Combined query for NER lists of companies in the Real Estate and Rental and Leasing sector

(Right now, the list only contains companies with above 1% job posting shares in the sector. Update when companies with above 0.5% shares have been added.)

*/

--- CBRE ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'CBRE' as company_name, 'Real Estate and Rental and Leasing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE (REGEXP_CONTAINS(entity.value,r"\bCBRE\b") OR REGEXP_CONTAINS(entity.value,r"\bColdwell Banker Richard Ellis\b"))
      AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      AND NOT REGEXP_CONTAINS(entity.value,r"CBRE GroupCBRE Group")
      AND NOT REGEXP_CONTAINS(entity.value,r"CBRE Group CBRE Group")
      AND NOT REGEXP_CONTAINS(entity.value,r"NYSE")
      AND NOT REGEXP_CONTAINS(entity.value,r"MARKET POSITION")
      AND NOT REGEXP_CONTAINS(entity.value,r"536452Regional Brand:CBRE LIMITED")
      AND NOT REGEXP_CONTAINS(entity.value,r"Class")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Jones Lang Lasalle Incorporated ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Jones Lang Lasalle' as company_name, 'Real Estate and Rental and Leasing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE (REGEXP_CONTAINS(entity.value,r"\bJones Lang Lasalle\b") OR REGEXP_CONTAINS(entity.value,r"\bJLL\b"))
      AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      AND NOT REGEXP_CONTAINS(entity.value,r"NYSE")
      AND NOT REGEXP_CONTAINS(entity.value,r"US48020Q1076 N:JLL")
      AND NOT REGEXP_CONTAINS(entity.value,r"JLL Partners JLL Partners")
      AND NOT REGEXP_CONTAINS(entity.value,r"Global Venture Fund")
      AND NOT REGEXP_CONTAINS(entity.value,r"JLL JLL")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- CoStar Group ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'CoStar Group' as company_name, 'Real Estate and Rental and Leasing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bCoStar Group\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Cushman & Wakefield ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Cushman & Wakefield' as company_name, 'Real Estate and Rental and Leasing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bCushman & Wakefield\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Enterprise Rent-A-Car ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Enterprise Rent-A-Car' as company_name, 'Real Estate and Rental and Leasing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bEnterprise Rent-A-Car\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Ryder System Incorporated ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Ryder System Incorporated' as company_name, 'Real Estate and Rental and Leasing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bRyder\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      AND NOT REGEXP_CONTAINS(entity.value,r"Capital")
      AND NOT REGEXP_CONTAINS(entity.value,r"Supply")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Hertz Corporation ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Hertz Corporation' as company_name, 'Real Estate and Rental and Leasing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bHertz\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      AND NOT REGEXP_CONTAINS(entity.value,r"Arena")
      AND NOT REGEXP_CONTAINS(entity.value,r"SOURCE")
      AND NOT REGEXP_CONTAINS(entity.value,r"Bankrupt")
      AND NOT REGEXP_CONTAINS(entity.value,r"Foundation")
      AND NOT REGEXP_CONTAINS(entity.value,r"Institute")
      AND NOT REGEXP_CONTAINS(entity.value,r"Investment")
      AND NOT REGEXP_CONTAINS(entity.value,r"Real Estate")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Penske ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Penske' as company_name, 'Real Estate and Rental and Leasing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bPenske\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      AND NOT REGEXP_CONTAINS(entity.value,r"Media")
      AND NOT REGEXP_CONTAINS(entity.value,r"GroupPenske")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- U-Haul ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'U-Haul' as company_name, 'Real Estate and Rental and Leasing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bU-Haul\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Lincoln Property Company ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Lincoln Property Company' as company_name, 'Real Estate and Rental and Leasing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bLincoln Property Company\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent