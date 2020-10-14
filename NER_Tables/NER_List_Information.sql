/* 

Combined query for NER lists of companies in the Information sector

(Companies with above 0.5% job posting shares in the sector.)

*/

--- Verizon ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Verizon' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bVerizon\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Fiserv ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Fiserv' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bFiserv\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Microsoft ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Microsoft' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bMicrosoft\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Google ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Google' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bGoogle\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Facebook ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Facebook' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bFacebook\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Apple Inc. ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Apple' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bApple\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Comcast ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Comcast' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bComcast\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Salesforce ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Salesforce' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bSalesforce\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Vmware ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Vmware' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bVmware\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      AND NOT REGEXP_CONTAINS(entity.value,r"Level")
      AND NOT REGEXP_CONTAINS(entity.value,r"DBmaestro")
      AND NOT REGEXP_CONTAINS(entity.value,r"Symantec")
      AND NOT REGEXP_CONTAINS(entity.value,r"Accenture")
      AND NOT REGEXP_CONTAINS(entity.value,r"Alibaba")
      AND NOT REGEXP_CONTAINS(entity.value,r"Fujitsu")
      AND NOT REGEXP_CONTAINS(entity.value,r"plc")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Oracle ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Oracle' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bOracle\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Disney ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Disney' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bDisney\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- SAP ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'SAP' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bSAP\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Centurylink ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Centurylink' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bCenturylink\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Cox Communications ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Cox Communications' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity
      --- "Cox Communications" was used as opposed to "Cox" due to an NER list of over 200 names that was produced with the generic search term "Cox". "Cox Communications" is also the specific company listed in Burning Glass, from which we obtained the list of companies.
      WHERE REGEXP_CONTAINS(entity.value,r"\bCox Communications\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- AT&T ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'AT&T' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bAT&T\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- NBC ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'NBC' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"NBC") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      AND NOT REGEXP_CONTAINS(entity.value,r"INNBCL")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Intuit ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Intuit' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bIntuit\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Pearson ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Pearson' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bPearson\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      AND NOT REGEXP_CONTAINS(entity.value,r"Airport")
      AND NOT REGEXP_CONTAINS(entity.value,r"Toronto")
      AND NOT REGEXP_CONTAINS(entity.value,r"Gilbert")
      AND NOT REGEXP_CONTAINS(entity.value,r"Pearson-Mach-Big Gold-Eskay")
      AND NOT REGEXP_CONTAINS(entity.value,r"Metropark")
      AND NOT REGEXP_CONTAINS(entity.value,r"Profit")
      AND NOT REGEXP_CONTAINS(entity.value,r"Medical")
      AND NOT REGEXP_CONTAINS(entity.value,r"Construction")
      AND NOT REGEXP_CONTAINS(entity.value,r"Chapel")
      AND NOT REGEXP_CONTAINS(entity.value,r"Institute")
      AND NOT REGEXP_CONTAINS(entity.value,r"Newmark Grubb Pearson Commercial")
      AND NOT REGEXP_CONTAINS(entity.value,r"& Pearson")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Adobe Systems ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Adobe Systems' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bAdobe\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- T-Mobile ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'T-Mobile' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bT-Mobile\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Time Warner ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Time Warner' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bTime Warner\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Charter Communications ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Charter Communications' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bCharter Communications\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- NCR Corporation ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'NCR Corporation' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bNCR\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- FIS ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'FIS' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE (REGEXP_CONTAINS(entity.value,r"\bFIS\b") OR REGEXP_CONTAINS(entity.value,r"\bFidelity National Information Services\b"))
      AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile") AND NOT REGEXP_CONTAINS(entity.value,r"(?i)NYSE")
      AND NOT REGEXP_CONTAINS(entity.value,r"USD")
      AND NOT REGEXP_CONTAINS(entity.value,r"Credit Services")
      AND NOT REGEXP_CONTAINS(entity.value,r"FIS Group")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Windstream ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Windstream' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bWindstream\b")
      AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile") AND NOT REGEXP_CONTAINS(entity.value,r"(?i)NYSE")
      AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Windstream Windstream")
      AND NOT REGEXP_CONTAINS(entity.value,r"DESCRIPTION")
      AND NOT REGEXP_CONTAINS(entity.value,r"Buffalo")
      AND NOT REGEXP_CONTAINS(entity.value,r"Energy")
      AND NOT REGEXP_CONTAINS(entity.value,r"Windstream Holdings Windstream Holdings")
      AND NOT REGEXP_CONTAINS(entity.value,r"Yield")
      AND NOT REGEXP_CONTAINS(entity.value,r"Analysis")
      AND NOT REGEXP_CONTAINS(entity.value,r"Bond")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Pegasystems Incorporated ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Pegasystems' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bPegasystems\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Viasat ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Viasat' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bViasat\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Thomson Reuters ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Thomson Reuters' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bThomson Reuters\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      AND NOT REGEXP_CONTAINS(entity.value,r"Trust")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Navisite Incorporated ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Navisite' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bNavisite\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Dish Network ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Dish Network' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bDish Network\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Sprint Corporation ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Sprint' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bSprint\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL

--- Red Hat ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Red Hat' as company_name, 'Information' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bRed Hat\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"(?i)Profile")
      AND NOT REGEXP_CONTAINS(entity.value,r"Red Hat Society")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
