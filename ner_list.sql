/*
Create a list of variations of the company name along with the count of their mentions
Calculate the % of data each variation occupies as well as the cumulative percentage from top to bottom
*/
WITH t AS
  (SELECT COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
  WHERE REGEXP_CONTAINS(entity.value,"Verizon") AND entity.type = "Company"
  GROUP BY name
  ORDER BY count DESC)
SELECT *, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
FROM
  (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
  FROM t
  ORDER BY percentage DESC);


/*
Updated version that creates a combined query with all the NER lists from companies
Threshold_percent makes sure the NER list contains the minimum number of NERs that make up 90% or slightly above of the count data
*/
--- BAE Systems ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'BAE Systems' as company_name, 'Manufacturing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bBAE Systems\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent
UNION ALL
--- Siemens ---
SELECT * FROM
(SELECT *,  min(case when cumulative_percentage >= 0.9 then cumulative_percentage end) over (partition by company_name) as threshold_percent 
FROM
  (SELECT*, SUM(percentage) OVER (ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM
    (SELECT *, (0.0+count)/SUM(count) OVER () AS percentage
    FROM
      (SELECT 'Siemens' as company_name, 'Manufacturing' as naics2, COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name
      FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
      WHERE REGEXP_CONTAINS(entity.value,r"\bSiemens\b") AND entity.type = "Company" AND NOT REGEXP_CONTAINS(entity.value,r"Profile")
      GROUP BY name, company_name, naics2
      ORDER BY count DESC)
    GROUP BY count, name, company_name, naics2
    ORDER BY percentage DESC) ) )
WHERE cumulative_percentage <= threshold_percent