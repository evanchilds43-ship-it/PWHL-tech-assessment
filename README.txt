# PWHL-tech-assessment
Setup Instructions
------------------
BigQuery Dataset
 - Dataset ID: grand-icon-475820-e5.pwhl_analytics_assessment
 - URL: https://console.cloud.google.com/bigquery?hl=en&project=grand-icon-475820-e5&ws=!1m4!1m3!3m2!1sgrand-icon-475820-e5!2spwhl_analytics_assessment
 - I set up public read-only permissions so that the dataset can be queried

Option 1. Run scripts manually in the following order:
python scripts/fetch_weather.py
python scripts/prepare_clean_data.py
python scripts/upload_to_bigquery.py

Option 2. Run the automated pipeline script:
python scripts/run_pipeline.py
This script executes all three steps sequentially and includes logging for each stage.
It also demonstrates how the pipeline could easily be automated using a Unix cron entry, for example:
0 0 * * * /usr/bin/python3 /path/to/scripts/run_pipeline.py
Typically in AWS, I've set up automation using Glue triggers.

All data files and log files "data/" directory. Cleaned datasets prepared for upload can be found in the data/cleaned/ directory.
Note: I used Python version 3.13.9 with the dependencies listed in included requirements.txt.


Assumptions
-----------
For the purpose of this exercise, I assumed that only one event takes place per day, making event_date a suitable unique identifier for joining the ticket sales and game section capacity datasets. In a real world scenario, I would use something along the lines of venue and game number as the unique identifier, but no such data was available in the pwhl_ticket_sales dataset.

Design Decisions
----------------
I chose to implement the ingestion, transformation, and loading steps using Python scripts, as it’s the environment I’m most comfortable with and best suited to demonstrate core data engineering skills.

The first script, fetch_weather.py, retrieves two months of weather data from the Meteostat API. Although the assessment instructions suggested other APIs, historical data was not freely accessible, making Meteostat a good alternative.

The second script, prepare_clean_data.py, loads the provided CSV files along with the fetched weather data, performs cleaning and transformation, and outputs the results into dimension and fact tables following a star schema structure—ready for upload to BigQuery.

Finally, upload_to_bigquery.py connects to my BigQuery sandbox and uploads all tables with the desired clustering configuration.

Schema Decision
--------------
I chose a star schema to clearly separate transactional ticket data from descriptive (dimensional) data. This structure improves query performance, simplifies joins, reduces redundancy, and provides flexibility for future analytical use cases. A data model diagram is included to illustrate the keys and measures of the fact table and its relationships to the dimension tables.

Data Cleaning
-------------
- All string fields were trimmed and lowercased for normalization.
- Missing or invalid numeric fields were coerced and filled where appropriate.
- Essential records missing key fields were dropped.

Partitioning & Clustering
-------------------------
The fact_ticket_sales table is clustered by venue_id, channel_id, and event_date to improve query performance on those common filters.

Note: Depending on business requirements and the scale of the ticketing data, it may be effective to partition the fact_ticket_sales table by season or year to optimize future query performance and simplify time-based filtering.

Exploratory Analysis
---------------------
Included is a notebook file, pwhl_assessment_eda.ipynb, which contains the queries used for my data explorations. I also saved the queries as individual .sql files in the sql directory. Additionally, I’ve included an eda_summary.pdf where I summarize my key findings.
