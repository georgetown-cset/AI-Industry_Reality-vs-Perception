# AI-Hype-vs-Hiring

## Code

### Burning Glass
* [getTopEmployers.sql](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/getTopEmployers.sql) generates the tables for the top employers in different sectors.
* [bg_eda_v2.sql](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/bg_eda_v2.sql) and [bg_eda_v2.Rmd](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/bg_eda_v2.Rmd) are the query and R markdown file, respectively, that produce various visualizations for the Burning Glass AI job posting data.
* Case studies on top employers from each sector can be found here:
  * Northrop Grumman: [ng.sql](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/Case_Studies/ng.sql), [ng.Rmd](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/Case_Studies/ng.Rmd)
  * Deloitte: [deloitte.sql](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/Case_Studies/deloitte.sql), [deloitte.Rmd](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/Case_Studies/deloitte.Rmd)
  * cbre: [cbre.sql](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/Case_Studies/cbre.sql), [cbre.Rmd](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/Case_Studies/cbre.Rmd)
  * FedEx: [fedex.sql](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/Case_Studies/fedex.sql), [fedex.Rmd](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/Case_Studies/fedex.Rmd)
  * Verizon: [verizon.sql](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/Case_Studies/verizon.sql), [verizon.Rmd](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/Case_Studies/verizon.Rmd)
* [company_list.sql](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/company_list.sql) contains the code for different approaches we tested for compiling top employers lists (Top 90%, Top 25 companies, All companies w/ 0.5% share).

### LexisNexis
* [findNERS.sql](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/findNERs.sql) generates the list of NER values for companies of interest.
* [ner_list.sql](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/ner_list.sql) takes the list of NER values for a company and calculates count, percentage, and cumulative percentage. It also contains the code template for creating a combined query with all the NER lists from companies. 
* [NER_Tables](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/tree/master/NER_Tables) contains the combined queries for NER lists of companies in each sector.
* [AI_Articles.sql](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/AI_Articles.sql) genereates the ai_articles table referenced in LN queries.
* [ln_sector.sql](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/ln_sector.sql) is the query for compiling the raw count and sentiment score summary statistics for each sector over the years.
* [sentiment_score_distribution.sql](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/sentiment_score_distribution.sql) analyzes the distribution of sentiment scores according to the classifications "Positive", "Neutral", and "Negative" in all articles mentioning the AI keywords.
* The LN analysis for the case studies can be found in the same files from above: [ng.sql](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/Case_Studies/ng.sql), [deloitte.sql](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/Case_Studies/deloitte.sql), [cbre.sql](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/Case_Studies/cbre.sql), [fedex.sql](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/Case_Studies/fedex.sql), [verizon.sql](https://github.com/georgetown-cset/AI-Hype-vs-Hiring/blob/master/Case_Studies/verizon.sql)
