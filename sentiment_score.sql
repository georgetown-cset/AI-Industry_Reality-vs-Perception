/*
Retrieving sentiment and entity scores for company name + AI mentions
Averaged per year
*/
WITH
-- ai_table: Joining ai_articles, which contains all articles with AI/ML mentions, with the sector NER table based on the NERs, so now each company is associated with AI articles that mention it as well as its NERs (that we selected)
ai_table AS (
  SELECT id, duplicateGroupId, content, pubdate, entity, sentiment_score, b.company_name, b.naics2
  FROM `gcp-cset-projects.ai_hype.ai_articles` CROSS JOIN UNNEST(entities) AS entity 
  JOIN `gcp-cset-projects.ai_hype.transportation_sector` b ON entity.value = b.name
)
SELECT company_name, naics2, EXTRACT(year FROM pubdate) AS pubyear, AVG(sentiment_score) AS sentiment_score_avg, AVG(entity_score) AS entity_score_avg
FROM
  (SELECT company_name, naics2, pubdate, 
  entity.score AS entity_score, 
  CAST(sentiment_score AS FLOAT64) AS sentiment_score, 
  RANK() OVER (PARTITION BY duplicateGroupId ORDER BY id ASC) rank
  FROM ai_table)
WHERE (rank = 1) AND (EXTRACT(year FROM pubdate) > 2010) AND (EXTRACT(year from pubdate) < 2021)
GROUP BY company_name, naics2, pubyear
ORDER BY company_name, naics2, pubyear DESC;


/*
Retrieving sentiment scores for sector + AI mentions
Summary statistics per year 
*/
SELECT EXTRACT(year FROM pubdate) AS pubyear, AVG(sentiment_score) AS average, STDDEV(sentiment_score) AS std_dev, MIN(sentiment_score) AS min, 
median, MAX(sentiment_score) AS max
FROM 
  (SELECT PERCENTILE_CONT(sentiment_score, 0.5) OVER (PARTITION BY EXTRACT(year FROM pubdate)) AS median, pubdate, sentiment_score 
  FROM 
    (SELECT pubdate, entity.score AS entity_score, CAST(sentiment_score AS FLOAT64) AS sentiment_score, 
    RANK() OVER (PARTITION BY duplicateGroupId ORDER BY id ASC) rank
    FROM `gcp-cset-projects.ai_hypes.ai_articles` CROSS JOIN UNNEST(entities) AS entity
    WHERE language = "English"
    AND regexp_contains(content, r"(?i)\btransportation\b") ) ---Change to "manufacturing" or "real estate" as necessary
  WHERE (rank = 1) AND (EXTRACT(year FROM pubdate) > 2010) AND (EXTRACT(year from pubdate) < 2020))
GROUP BY pubyear, median
ORDER BY pubyear DESC;

/* 
Summary statistics for sentiment and entity scores
(Limited to AI articles only because calculating for the entire LN database is computationally taxing and produces a runtime error)
Included language = "English" because witout the language specification, max/min entity scores were huge and were found to be attributed to foreign language articles
*/
SELECT 'average', AVG(entity_score) AS entity_score, AVG(sentiment_score) AS sentiment_score
FROM
  (SELECT entity.score AS entity_score, 
  CAST(sentiment.score AS FLOAT64) AS sentiment_score, 
  RANK() OVER (PARTITION BY duplicateGroupId ORDER BY id ASC) rank,
  language
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity WHERE duplicateGroupId IN(SELECT duplicateGroupId FROM ai_hype.ai_articles))
WHERE (rank = 1) AND language = "English"
UNION ALL
SELECT 'min', MIN(entity_score), MIN(sentiment_score)
FROM
  (SELECT entity.score AS entity_score, 
  CAST(sentiment.score AS FLOAT64) AS sentiment_score, 
  RANK() OVER (PARTITION BY duplicateGroupId ORDER BY id ASC) rank,
  language
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity WHERE duplicateGroupId IN(SELECT duplicateGroupId FROM ai_hype.ai_articles))
WHERE (rank = 1) AND language = "English"
UNION ALL
SELECT 'max', MAX(entity_score), MAX(sentiment_score)
FROM
  (SELECT entity.score AS entity_score, 
  CAST(sentiment.score AS FLOAT64) AS sentiment_score, 
  RANK() OVER (PARTITION BY duplicateGroupId ORDER BY id ASC) rank,
  language
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity WHERE duplicateGroupId IN(SELECT duplicateGroupId FROM ai_hype.ai_articles))
WHERE (rank = 1) AND language = "English"
UNION ALL
SELECT 'standard deviation', STDDEV(entity_score), STDDEV(sentiment_score)
FROM
  (SELECT entity.score AS entity_score, 
  CAST(sentiment.score AS FLOAT64) AS sentiment_score, 
  RANK() OVER (PARTITION BY duplicateGroupId ORDER BY id ASC) rank,
  language
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity WHERE duplicateGroupId IN(SELECT duplicateGroupId FROM ai_hype.ai_articles))
WHERE (rank = 1) AND language = "English"
UNION ALL
(SELECT 'median', PERCENTILE_CONT(entity_score, 0.5) OVER (), PERCENTILE_CONT(sentiment_score, 0.5) OVER ()
FROM
  (SELECT entity.score AS entity_score, 
  CAST(sentiment.score AS FLOAT64) AS sentiment_score, 
  RANK() OVER (PARTITION BY duplicateGroupId ORDER BY id ASC) rank,
  language
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity WHERE duplicateGroupId IN(SELECT duplicateGroupId FROM ai_hype.ai_articles))
WHERE rank = 1 AND language = "English"
LIMIT 1);


/*
Looking into the extreme outliers in entity scores
*/
SELECT company_name, entity_score, duplicateGroupID
FROM
  (SELECT entity.value as company_name,
  entity.score AS entity_score,
  duplicateGroupID,
  RANK() OVER (PARTITION BY duplicateGroupId ORDER BY id ASC) rank,
  language
  FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity WHERE duplicateGroupId IN(SELECT duplicateGroupId FROM ai_hype.ai_articles))
WHERE rank = 1 AND language = "English"
ORDER BY entity_score DESC
LIMIT 10;
