/*

find the list of values in the entity.value field for companies of interest 
Example query for Raytheon

*/

SELECT COUNT(DISTINCT(duplicateGroupId)) AS count, entity.value AS name FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) AS entity 
WHERE REGEXP_CONTAINS(entity.value,"Raytheon") AND entity.type = "Company"
GROUP BY entity.value
ORDER BY count DESC
