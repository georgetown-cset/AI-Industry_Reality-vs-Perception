/*
Top employers in each sector with job posting share above 0.5% (2019)
*/
WITH t1 AS 
  (SELECT naics2_name, canon_employer, COUNT(DISTINCT job_id) as job_count
  FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
  WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing', 'Information')
  AND EXTRACT(YEAR FROM job_posting_date) = 2019
  GROUP BY naics2_name, canon_employer),
list AS
(SELECT naics2_name, canon_employer FROM
  (SELECT naics2_name, canon_employer, job_count, 
        (0.0+job_count)/(SUM(job_count) OVER (PARTITION BY naics2_name))*100 as percentage
  FROM t1
  WHERE canon_employer is not null)
WHERE percentage > 0.5
ORDER BY naics2_name, percentage DESC)
/*
Check each company's share of job postings in each sector
*/
SELECT *, (0.0+job_count)/(SUM(job_count) OVER (PARTITION BY canon_employer))*100 as percentage FROM
  (SELECT COUNT(DISTINCT job_id) as job_count, naics2_name, canon_employer
  FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
  WHERE EXTRACT(YEAR FROM job_posting_date) = 2019
  AND canon_employer IN (SELECT canon_employer FROM list)
  GROUP BY naics2_name, canon_employer)
ORDER BY canon_employer, job_count DESC;
