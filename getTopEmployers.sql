/* 

generate table for a given NAICS2 or AI occupation classification 

options for NAICS 2: Information, Manufacturing, Transportation and Warehousing, Professional, Scientific, and Technical Services, Real Estate and Rental and Leasing

options for AI occupation classification: C1, C2, PT, CT

Below is an example query using Transportation and Warehousing with all AI occupations (`gcp-cset-projects.burning_glass.US_AI_framework_wDETAIL`). 
Change the table for different occupation classifications and change the naics2_name for different naics 2 options. 

*/


SELECT COUNT(*) as job_posting_count, canon_employer as employer FROM `gcp-cset-projects.burning_glass.US_AI_framework_wDETAIL`
WHERE naics2_name = "Transportation and Warehousing"
AND EXTRACT(year FROM job_posting_date) = 2019
AND canon_employer IS NOT NULL
GROUP BY employer
ORDER BY job_posting_count DESC
LIMIT 100 --for the top 100 employers, remove for the full list 
