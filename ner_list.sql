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