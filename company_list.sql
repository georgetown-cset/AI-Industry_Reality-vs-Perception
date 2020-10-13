/*
Top 90% share in each industry
*/
WITH company_list AS
  (SELECT COUNT(DISTINCT job_id) as job_count, canon_employer, naics2_name
  FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
  WHERE EXTRACT(YEAR FROM job_posting_date) = 2019
  AND naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing', 'Information')
  AND canon_employer IS NOT NULL
  GROUP BY canon_employer, naics2_name),
company_list_with_percentage AS
  (SELECT *, (0.0+job_count)/SUM(job_count) OVER (PARTITION BY naics2_name) AS percentage
  FROM company_list
  ORDER BY percentage DESC)
SELECT * FROM
  (SELECT *, SUM(percentage) OVER (PARTITION BY naics2_name ORDER BY percentage DESC rows between unbounded preceding and current row) as cumulative_percentage
  FROM company_list_with_percentage)
WHERE cumulative_percentage < 0.9;


/*
Top 25 companies in each sector
*/
WITH company_list_with_rank AS
  (SELECT *, RANK() OVER (PARTITION BY naics2_name ORDER BY job_count DESC) rank
  FROM
    (SELECT COUNT(DISTINCT job_id) as job_count, canon_employer, naics2_name, 
    FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
    WHERE EXTRACT(YEAR FROM job_posting_date) = 2019
    AND naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing', 'Information')
    AND canon_employer IS NOT NULL
    GROUP BY canon_employer, naics2_name) )
SELECT * FROM company_list_with_rank
WHERE rank <= 25;


/*
Top employers in each sector with job posting share above 0.5%
*/
WITH t1 AS 
  (SELECT naics2_name, canon_employer, COUNT(DISTINCT job_id) as job_count
  FROM `gcp-cset-projects.burning_glass.US_AI_framework_postings_wDETAIL`
  WHERE naics2_name IN ('Manufacturing', 'Professional, Scientific, and Technical Services', 'Transportation and Warehousing', 'Real Estate and Rental and Leasing', 'Information')
  AND EXTRACT(YEAR FROM job_posting_date) = 2019
  GROUP BY naics2_name, canon_employer)
SELECT * FROM
  (SELECT naics2_name, canon_employer, job_count, 
        (0.0+job_count)/(SUM(job_count) OVER (PARTITION BY naics2_name))*100 as percentage
  FROM t1
  WHERE canon_employer is not null)
WHERE percentage > 0.5
ORDER BY naics2_name, percentage DESC;