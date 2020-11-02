/*

Case Study 4: FedEx

*/


/*
Top majors in demand (2019)
Checking sector in case the company has job postings in other sectors besides the main one
Saved as fedex_major
*/
SELECT COUNT(DISTINCT job_id) as job_count, canon_employer, naics2_name, standard_major as major FROM
(SELECT * FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
LEFT JOIN `gcp-cset-projects.burning_glass.major` USING(job_id))
WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing', 'Information') 
AND EXTRACT(YEAR FROM job_posting_date) = 2019
AND canon_employer = "FedEx"
GROUP BY naics2_name, canon_employer, major;

/*
Job posting across occupation fields, also broken down by major (2019)
Saved as fedex_field
*/
SELECT COUNT(DISTINCT job_id) as job_count, DETAIL, canon_employer, naics2_name, standard_major as major FROM
(SELECT * FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
LEFT JOIN `gcp-cset-projects.burning_glass.major` USING(job_id))
WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing', 'Information') 
AND EXTRACT(YEAR FROM job_posting_date) = 2019
AND canon_employer = "FedEx"
GROUP BY DETAIL, major, canon_employer, naics2_name;

/*
Yearly trend, broken down by occupation field
Saved as fedex_trend
*/
SELECT COUNT(DISTINCT job_id) as job_count, EXTRACT(YEAR FROM job_posting_date) as year, DETAIL, canon_employer
FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing', 'Information')
AND canon_employer = "FedEx"
GROUP BY year, DETAIL, canon_employer
ORDER BY year DESC;
/* 
Compare AI job postings to the total job postings by the company 
Saved as fedex_trend_total
*/
SELECT COUNT(DISTINCT temp.job_id) as job_count, EXTRACT(YEAR FROM temp.job_posting_date) as year, temp.canon_employer, a.DETAIL 
FROM 
  (SELECT t.*, CAST(retrieval_date AS DATETIME) AS job_posting_date FROM `gcp-cset-projects.burning_glass.job` t
  WHERE record_country = "US") temp
LEFT JOIN `gcp-cset-projects.burning_glass.AI_Occupation_Crosswalk_2010_Codes` a ON a.SOC_Code_2010 = temp.us_occ
WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing', 'Information')
AND canon_employer = "FedEx"
GROUP BY year, DETAIL, canon_employer;
/*
Examine what (if any) occupation codes are causing the huge increase in C2 hiring that occurs in 2014
Saved as fedex_trend_c2_occ_20132014
*/
WITH t AS
  (SELECT COUNT(DISTINCT job_id) as job_count, EXTRACT(YEAR FROM job_posting_date) as year, DETAIL, naics2_name, canon_employer, us_occ_name
  FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
  WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing', 'Information')
  AND DETAIL = 'C2'
  AND canon_employer = "FedEx"
  GROUP BY year, DETAIL, naics2_name, canon_employer, us_occ_name
  ORDER BY year)
SELECT * FROM t
WHERE year IN (2013, 2014);

/*
Check the share of job postings in each sector (2019)
Saved as fedex_sector
*/
SELECT COUNT(DISTINCT job_id) as job_count, naics2_name, canon_employer
FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
WHERE EXTRACT(YEAR FROM job_posting_date) = 2019
AND canon_employer = "FedEx"
GROUP BY naics2_name, canon_employer;

/*
Raw count and summary statistics on entity/sentiment scores per year from LexisNexis
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
  SELECT id, duplicateGroupId, content, pubdate, entity, sentiment_score, b.company_name, b.naics2
  FROM `gcp-cset-projects.gcp_cset_lexisnexis.ai_articles` CROSS JOIN UNNEST(entities) AS entity 
  JOIN `gcp-cset-projects.ai_hype.transportation_sector` b ON entity.value = b.name
  WHERE language = "English"
  AND b.company_name = "FedEx"
)

-- STEP 5: For each row, multiply the ai count in that row by the max of the total yearly count divided by the row's yearly count. 
-- This is an attempt to scale the number of articles up to what was seen in the year with highest publication (i.e. normalization)
SELECT ai_counts.company_name, naics2, ai_counts.pubyear, ai_counts.num_articles*max_of_total_yearly_counts.maxcount/all_counts.num_articles AS normalized_ai_count, sentiment_scores.s_avg, sentiment_scores.s_std_dev, sentiment_scores.s_min, sentiment_scores.s_median, sentiment_scores.s_max,
sentiment_scores.e_avg, sentiment_scores.e_std_dev, sentiment_scores.e_min, sentiment_scores.e_median, sentiment_scores.e_max
FROM
  -- STEP 1: Get count of all articles containing our terms of interest
  (SELECT EXTRACT(year FROM pubdate) AS pubyear, COUNT(duplicateGroupId) AS num_articles, company_name 
  FROM
    (SELECT duplicateGroupId, pubdate, RANK() OVER (PARTITION BY duplicateGroupId ORDER BY id ASC) rank, entity.value, company_name
    FROM ai_table)
  WHERE (rank = 1) AND (EXTRACT(year FROM pubdate) > 2010) AND (EXTRACT(year from pubdate) < 2020)
  GROUP BY pubyear, company_name ORDER BY pubyear DESC)
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
SELECT company_name, naics2, EXTRACT(year FROM pubdate) AS pubyear, 
AVG(sentiment_score) AS s_avg, STDDEV(sentiment_score) AS s_std_dev, MIN(sentiment_score) AS s_min, s_median, MAX(sentiment_score) AS s_max, 
AVG(entity_score) AS e_avg, STDDEV(entity_score) AS e_std_dev, MIN(entity_score) AS e_min, e_median, MAX(entity_score) AS e_max
FROM
  (SELECT PERCENTILE_CONT(sentiment_score, 0.5) OVER (PARTITION BY EXTRACT(year FROM pubdate)) AS s_median, sentiment_score, 
  PERCENTILE_CONT(entity_score, 0.5) OVER (PARTITION BY EXTRACT(year FROM pubdate)) AS e_median, entity_score, 
  company_name, naics2, pubdate
  FROM
    (SELECT company_name, naics2, pubdate, 
    entity.score AS entity_score, 
    CAST(sentiment_score AS FLOAT64) AS sentiment_score, 
    RANK() OVER (PARTITION BY duplicateGroupId ORDER BY id ASC) rank
    FROM ai_table)
  WHERE (rank = 1) AND (EXTRACT(year FROM pubdate) > 2010) AND (EXTRACT(year from pubdate) < 2020) )
GROUP BY company_name, naics2, pubyear, s_median, e_median
ORDER BY company_name, naics2, pubyear DESC) AS sentiment_scores
ON sentiment_scores.company_name = ai_counts.company_name AND sentiment_scores.pubyear = ai_counts.pubyear

ORDER BY company_name, naics2, pubyear DESC;

/* 
Looking at FedEx AI articles with max/min entity scores overall
*/
WITH
fedex_ai_table AS (
  SELECT id, duplicateGroupId, content, pubdate, entity, sentiment_score, b.company_name, b.naics2
  FROM `gcp-cset-projects.gcp_cset_lexisnexis.ai_articles` CROSS JOIN UNNEST(entities) AS entity 
  JOIN `gcp-cset-projects.ai_hype.transportation_sector` b ON entity.value = b.name
  WHERE language = "English"
  AND b.company_name = "FedEx"
),
t AS
(SELECT company_name, EXTRACT(year FROM pubdate) AS pubyear, duplicateGroupID,
AVG(sentiment_score) AS s_avg, STDDEV(sentiment_score) AS s_std_dev, MIN(sentiment_score) AS s_min, s_median, MAX(sentiment_score) AS s_max, 
AVG(entity_score) AS e_avg, STDDEV(entity_score) AS e_std_dev, MIN(entity_score) AS e_min, e_median, MAX(entity_score) AS e_max
FROM
  (SELECT PERCENTILE_CONT(sentiment_score, 0.5) OVER (PARTITION BY EXTRACT(year FROM pubdate)) AS s_median, sentiment_score, 
  PERCENTILE_CONT(entity_score, 0.5) OVER (PARTITION BY EXTRACT(year FROM pubdate)) AS e_median, entity_score, 
  company_name, pubdate, duplicateGroupID
  FROM
    (SELECT entity.value AS company_name, pubdate, 
    entity.score AS entity_score, 
    CAST(sentiment_score AS FLOAT64) AS sentiment_score, 
    duplicateGroupID,
    RANK() OVER (PARTITION BY duplicateGroupId ORDER BY id ASC) rank
    FROM fedex_ai_table)
  WHERE (rank = 1) AND (EXTRACT(year FROM pubdate) > 2010) AND (EXTRACT(year from pubdate) < 2020) )
GROUP BY company_name, pubyear, duplicateGroupID, s_median, e_median
ORDER BY company_name, pubyear DESC)
SELECT * FROM t
WHERE e_max = (SELECT MAX(e_max) FROM t) OR e_min = (SELECT MIN(e_min) FROM t);
/* Inspect the max/min articles */
SELECT duplicateGroupID, title, content, url FROM `gcp-cset-projects.gcp_cset_lexisnexis.raw_news`
WHERE duplicateGroupID = 38651263686 ---max
OR duplicateGroupID = 40919997848 ---min