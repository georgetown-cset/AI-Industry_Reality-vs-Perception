SELECT COUNT(sentiment_score), bucket 
FROM
  (SELECT sentiment_score,
  CASE WHEN sentiment_score < -0.05 THEN "Negative"
       WHEN sentiment_score >= -0.05 AND sentiment_score < 0.22 THEN "Neutral"
       WHEN sentiment_score >= 0.22 THEN "Positive"
  END as bucket
  FROM
    (SELECT *
     FROM
       (SELECT CAST(sentiment.score AS FLOAT64) AS sentiment_score, 
       RANK() OVER (PARTITION BY duplicateGroupId ORDER BY id ASC) rank,
       language
       FROM `gcp-cset-projects.ai_hype.ai_articles` CROSS JOIN UNNEST(sentiment.entities) AS entity)
     WHERE (rank = 1) AND language = "English")
   )
GROUP BY bucket
ORDER BY bucket;
