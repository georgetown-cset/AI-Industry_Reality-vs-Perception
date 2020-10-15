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
LEFT JOIN `gcp-cset-projects.burning_glass.major` USING(job_id, record_country, import_time))
WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing') 
AND EXTRACT(YEAR FROM job_posting_date) = 2019
AND canon_employer = "FedEx"
GROUP BY naics2_name, canon_employer, major;

/*
Job posting across occupation fields, also broken down by major (2019)
Saved as fedex_field
*/
SELECT COUNT(DISTINCT job_id) as job_count, DETAIL, canon_employer, naics2_name, standard_major as major FROM
(SELECT * FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
LEFT JOIN `gcp-cset-projects.burning_glass.major` USING(job_id, record_country, import_time))
WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing') 
AND EXTRACT(YEAR FROM job_posting_date) = 2019
AND canon_employer = "FedEx"
GROUP BY DETAIL, major, canon_employer, naics2_name;

/*
Yearly trend, broken down by occupation field
Saved as fedex_trend
*/
SELECT COUNT(DISTINCT job_id) as job_count, EXTRACT(YEAR FROM job_posting_date) as year, DETAIL, canon_employer
FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing')
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
WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing')
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
