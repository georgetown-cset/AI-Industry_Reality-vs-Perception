/*
What degrees are most sought after in each sector? (2019)
Saved as major_sector_2019
*/
SELECT COUNT(DISTINCT job_id) as job_count, naics2_name, standard_major as major FROM
(SELECT * FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
LEFT JOIN `gcp-cset-projects.burning_glass.major` USING(job_id, record_country, import_time))
WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing', 'Information') 
AND EXTRACT(YEAR FROM job_posting_date) = 2019
GROUP BY naics2_name, major;
/*
Analyze as aggregate over the years
Saved as major_sector_aggregate
*/
SELECT COUNT(DISTINCT job_id) as job_count, naics2_name, standard_major as major FROM
(SELECT * FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
LEFT JOIN `gcp-cset-projects.burning_glass.major` USING(job_id, record_country, import_time))
WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing', 'Information') 
GROUP BY naics2_name, major;


/*
What degrees are most sought after for each type of AI job (field)? (2019)
Saved as major_field_2019
*/
SELECT COUNT(DISTINCT job_id) as job_count, DETAIL, standard_major as major FROM
(SELECT * FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
LEFT JOIN `gcp-cset-projects.burning_glass.major` USING(job_id, record_country, import_time))
WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing', 'Information') 
AND EXTRACT(YEAR FROM job_posting_date) = 2019
GROUP BY DETAIL, major;
/* 
Analyze as aggregate over the years 
Saved as major_field_aggregate
*/
SELECT COUNT(DISTINCT job_id) as job_count, DETAIL, standard_major as major FROM
(SELECT * FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
LEFT JOIN `gcp-cset-projects.burning_glass.major` USING(job_id, record_country, import_time))
WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing', 'Information') 
GROUP BY DETAIL, major;


/*
How has labor demand for AI skills changed over the years in each sector?
Saved as trend_sector
*/
SELECT COUNT(DISTINCT job_id) as job_count, EXTRACT(YEAR FROM job_posting_date) as year, naics2_name
FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing', 'Information') 
GROUP BY year, naics2_name
ORDER BY year DESC;


/*
How has labor demand for AI skills changed over the years for each type of AI job (field)?
Saved as trend_field
*/
SELECT COUNT(DISTINCT job_id) as job_count, EXTRACT(YEAR FROM job_posting_date) as year, DETAIL
FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing', 'Information') 
GROUP BY year, DETAIL
ORDER BY year DESC;


/*
Trend in AI labor demand over the years, analyze by sector and AI occupation field
Saved as trend_sector-field
*/
SELECT COUNT(DISTINCT job_id) as job_count, EXTRACT(YEAR FROM job_posting_date) as year, DETAIL, naics2_name
FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing', 'Information') 
GROUP BY year, DETAIL, naics2_name
ORDER BY year DESC;
/* 
Compare with total trend in each sector
Saved as trend_sector-field_total
*/
SELECT COUNT(DISTINCT temp.job_id) as job_count, EXTRACT(YEAR FROM temp.job_posting_date) as year, a.DETAIL, naics2_name,
FROM 
  (SELECT t.*, CAST(retrieval_date AS DATETIME) AS job_posting_date FROM `gcp-cset-projects.burning_glass.job` t
  WHERE record_country = "US") temp
LEFT JOIN `gcp-cset-projects.burning_glass.AI_Occupation_Crosswalk_2010_Codes` a ON a.SOC_Code_2010 = temp.us_occ
WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing', 'Information')
GROUP BY year, DETAIL, naics2_name;
/*
Examine the CT divergence from overall trend in Real Estate sector by looking if any particular jobs are causing it
Saved as trend_realestate_ct_occ_20152016
*/
WITH t AS
  (SELECT COUNT(DISTINCT job_id) as job_count, EXTRACT(YEAR FROM job_posting_date) as year, DETAIL, naics2_name, us_occ_name
  FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
  WHERE naics2_name = 'Real Estate and Rental and Leasing'
  AND DETAIL = 'CT'
  GROUP BY year, DETAIL, naics2_name, us_occ_name
  ORDER BY year DESC)
SELECT * FROM t
WHERE year IN (2015, 2016);


/*
How does labor market share by employers change across sectors? (2019)
Saved as employer_share
*/
WITH t1 AS 
  (SELECT naics2_name, canon_employer, COUNT(DISTINCT job_id) as job_count
  FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
  WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing', 'Information')
  AND EXTRACT(YEAR FROM job_posting_date) = 2019
  GROUP BY naics2_name, canon_employer)
SELECT * FROM
  (SELECT naics2_name, canon_employer, job_count, 
        (0.0+job_count)/(SUM(job_count) OVER (PARTITION BY naics2_name)) as percentage
  FROM t1
  WHERE canon_employer is not null)
WHERE percentage > 0.01
ORDER BY percentage DESC;


/*
Do different sectors tend to hire in different locations?
Saved as location_sector
*/
SELECT COUNT(DISTINCT job_id) as job_count, canon_state, naics2_name
FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing', 'Information')
AND EXTRACT(YEAR FROM job_posting_date) = 2019
GROUP BY canon_state, naics2_name;