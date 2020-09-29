/*

find the list of values in the entity.value field for companies of interest 
Example query for Raytheon

*/

SELECT COUNT(DISTINCT(duplicateGroupId)) FROM gcp_cset_lexisnexis.raw_news CROSS JOIN UNNEST(sentiment.entities) as entity 
WHERE REGEXP_CONTAINS(entity.value,"Raytheon") AND entity.type = "Company"
