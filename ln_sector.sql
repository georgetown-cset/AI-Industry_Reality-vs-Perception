/*
Raw count and summary statistics on entity/sentiment scores per year for each sector
*/

WITH
-- all_articles: Select all articles from LexisNexis
all_articles AS (
  SELECT id, duplicateGroupId, (CASE publishedDate IS NULL WHEN TRUE THEN estimatedPublishedDate ELSE publishedDate END) AS pubdate,
  FROM gcp_cset_lexisnexis.raw_news
  WHERE language = "English"
),
-- total_yearly_counts: Get yearly counts of all articles, without any filtering by keywords
total_yearly_counts AS (
  SELECT EXTRACT(year FROM pubdate) AS pubyear, COUNT(duplicateGroupId) AS num_articles FROM
    -- Use rank to get the first article published within a duplicateGroup; this helps avoid double-counting
    (SELECT duplicateGroupId, pubdate, RANK() OVER (PARTITION BY duplicateGroupId ORDER BY id ASC) rank
    FROM all_articles)
  -- Since the year is not over, don't include 2020
  WHERE (rank = 1) AND (EXTRACT(year FROM pubdate) > 2010) AND (EXTRACT(year FROM pubdate) < 2020)
  GROUP BY pubyear 
  ORDER BY pubyear DESC
),
-- ai_table: Joining ai_articles, which contains all articles with AI/ML mentions, with the sector NER table based on the NERs, so now each company is associated with AI articles that mention it as well as its NERs (that we selected)
ai_table AS (
  SELECT id, duplicateGroupId, pubdate, entity, sentiment_score, b.company_name, b.naics2
  FROM `gcp-cset-projects.gcp_cset_lexisnexis.ai_articles` CROSS JOIN UNNEST(entities) AS entity 
  JOIN `gcp-cset-projects.ai_hype.information_sector` b ON entity.value = b.name --Change as needed
  WHERE language = "English"
  AND b.naics2 = "Information" --Change as needed
)

-- STEP 5: For each row, multiply the ai count in that row by the max of the total yearly count divided by the row's yearly count. 
-- This is an attempt to scale the number of articles up to what was seen in the year with highest publication (i.e. normalization)
SELECT ai_counts.naics2, ai_counts.pubyear, ai_counts.num_articles*max_of_total_yearly_counts.maxcount/all_counts.num_articles AS normalized_ai_count, sentiment_scores.s_avg, sentiment_scores.s_std_dev, sentiment_scores.s_min, sentiment_scores.s_median, sentiment_scores.s_max
FROM
  -- STEP 1: Get count of all articles containing our terms of interest
  (SELECT EXTRACT(year FROM pubdate) AS pubyear, COUNT(duplicateGroupId) AS num_articles, naics2 
  FROM
    (SELECT duplicateGroupId, pubdate, RANK() OVER (PARTITION BY duplicateGroupId ORDER BY id ASC) rank, entity.value, naics2
    FROM ai_table)
  WHERE (rank = 1) AND (EXTRACT(year FROM pubdate) > 2010) AND (EXTRACT(year from pubdate) < 2020)
  GROUP BY pubyear, naics2 
  ORDER BY pubyear DESC)
AS ai_counts

LEFT JOIN
-- STEP 2: Join on total_yearly_counts
total_yearly_counts AS all_counts
ON ai_counts.pubyear = all_counts.pubyear

JOIN
-- STEP 3: Add a column containing the max value from the total yearly counts
(SELECT MAX(num_articles) AS maxcount FROM total_yearly_counts) AS max_of_total_yearly_counts
ON TRUE

--- STEP 4: Join the average sentiment score and average entity sentiment score for each year and for each company
JOIN
(
SELECT naics2, EXTRACT(year FROM pubdate) AS pubyear, 
AVG(sentiment_score) AS s_avg, STDDEV(sentiment_score) AS s_std_dev, MIN(sentiment_score) AS s_min, s_median, MAX(sentiment_score) AS s_max
FROM
  (SELECT PERCENTILE_CONT(sentiment_score, 0.5) OVER (PARTITION BY EXTRACT(year FROM pubdate)) AS s_median, sentiment_score,
  naics2, pubdate
  FROM
    (SELECT naics2, pubdate,
    CAST(sentiment_score AS FLOAT64) AS sentiment_score, 
    RANK() OVER (PARTITION BY duplicateGroupId ORDER BY id ASC) rank
    FROM ai_table)
  WHERE (rank = 1) AND (EXTRACT(year FROM pubdate) > 2010) AND (EXTRACT(year from pubdate) < 2020) )
GROUP BY naics2, pubyear, s_median
ORDER BY naics2, pubyear DESC) AS sentiment_scores
ON sentiment_scores.naics2 = ai_counts.naics2 AND sentiment_scores.pubyear = ai_counts.pubyear

ORDER BY naics2, pubyear DESC;