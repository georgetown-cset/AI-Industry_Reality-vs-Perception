--generates the ai_articles table 

WITH AI_articles AS(SELECT id, duplicateGroupId, content, (case publishedDate is null when true then estimatedPublishedDate else publishedDate end) as pubdate, sentiment.entities, language, sentiment.score as sentiment_score FROM gcp_cset_lexisnexis.raw_news
WHERE regexp_contains(content, r"(?i)\bartificial intelligence\b") or regexp_contains(content, r"(?i)\bmachine learning\b"))
SELECT * FROM AI_articles
