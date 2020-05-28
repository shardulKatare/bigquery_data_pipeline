import json
from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


# Config variables
dag_config = Variable.get("bigquery_covid19_hpsa_variables", deserialize_json=True)
BQ_CONN_ID = dag_config["bq_conn_id"]
BQ_PROJECT = dag_config["bq_project"]
BQ_DATASET = dag_config["bq_dataset"]

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,

    'start_date': datetime(2020, 5, 22),
    'end_date': datetime(2020, 5, 27),
    'email': ['shardulkatare@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Set Schedule: Run pipeline once a day.
# Use cron to define exact time. Eg. 8:15am would be "15 08 * * *"
schedule_interval = "00 20 * * *"

# Define DAG: Set ID and assign default args and schedule interval
dag = DAG(
    'bigquery_covid19_hpsa',
    default_args=default_args,
    schedule_interval=schedule_interval
)

## Task 1: Create a Table with HPSA Shortage Data and where HPSA_is still a high priority

# To test this task, run this command:
# docker-compose -f docker-compose-gcloud.yml run --rm webserver airflow test bigquery_github_trends bq_write_healthcare_prof_shortage 2020-05-22

t1 = BigQueryOperator(
    task_id='bq_write_healthcare_prof_shortage',
    sql='''
        #standardSQL
      SELECT County_Name, Source_Name, Component_Source_Name, Type_Desc, HPSA_Score, State_Abbr,State_Name
  FROM `bigquery-public-data.sdoh_hrsa_shortage_areas.hpsa_primary_care` 
  WHERE HPSA_Withdrawn_Date IS NULL
        ''',
    destination_dataset_table='{0}.{1}.hpsa_usa${2}'.format(
        BQ_PROJECT, BQ_DATASET, '{{ yesterday_ds_nodash }}'
    ),
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag
)

# Task2: Aggregate the the Avg. HPSA score by county and state
# docker-compose -f docker-compose-gcloud.yml run --rm webserver airflow test bigquery_github_trends bq_agg_healthcare_data 2020-05-22

t2 = BigQueryOperator(
    task_id='bq_agg_healthcare_data',
    sql='''
        #standardSQL
        SELECT ROUND(AVG(HPSA_Score),2) AS Avg_HPSA_Score,State_Abbr,
        County_Name,
        State_Name
        FROM `{0}.{1}.hpsa_usa` 
        GROUP BY County_Name,State_Abbr,State_Name
        '''.format(
        BQ_PROJECT, BQ_DATASET, "{{ yesterday_ds }}"
    ),
    destination_dataset_table='{0}.{1}.health_agg_county${2}'.format(
        BQ_PROJECT, BQ_DATASET, '{{ yesterday_ds_nodash }}'
    ),
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag
)

## Task 3: Query the number of covid-10 deaths and cases by county and state
# docker-compose -f docker-compose-gcloud.yml run --rm webserver airflow test bigquery_github_trends bq_agg_covid19 2020-05-22
t3 = BigQueryOperator(
    task_id='bq_agg_covid19',
    sql='''
        #standardSQL
      SELECT
  covid19.state,
  covid19.county_name,
  ROUND(confirmed_cases/total_pop *100000,2) AS confirmed_cases_per_100000,
  ROUND(deaths/total_pop *100000,2) AS deaths_per_100000,
  confirmed_cases AS confirmed_cases,
  deaths, 
  total_pop AS county_population, # why is this a float?
FROM `bigquery-public-data.covid19_usafacts.summary` covid19
JOIN `bigquery-public-data.census_bureau_acs.county_2017_5yr` acs 
ON covid19.county_fips_code = acs.geo_id
WHERE date = DATE_SUB(CURRENT_DATE(), INTERVAL 30 day) 
AND county_fips_code != "00000"
AND confirmed_cases + deaths > 0
ORDER BY confirmed_cases_per_100000 DESC, deaths_per_100000 DESC
        ''',
    destination_dataset_table='{0}.{1}.covid_aggs${2}'.format(
        BQ_PROJECT, BQ_DATASET, '{{ yesterday_ds_nodash }}'
    ),
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag
)

# Task4
# docker-compose -f docker-compose-gcloud.yml run --rm webserver airflow test bigquery_github_trends bq_write_to_covid-19_agg 2020-05-22
t4 = BigQueryOperator(
    task_id='bq_write_to_covid-19_agg',
    sql='''
        #standardSQL
    SELECT 
    a.county_name,
    b.State_Name,
    a.state,
    a.deaths,
    a.deaths_per_100000,
    a.confirmed_cases_per_100000,
    a.confirmed_cases,
    b.Avg_HPSA_Score,
    FROM 
        `{0}.{1}.covid_aggs` as a
    LEFT JOIN `{0}.{1}.health_agg_county` as b
         ON a.county_name = b.County_Name
         AND a.state = b.State_Abbr
        ORDER BY
        a.confirmed_cases_per_100000 DESC
    '''.format(
        BQ_PROJECT, BQ_DATASET, "{{ yesterday_ds }}"
    ),
    destination_dataset_table='{0}.{1}.Covid19_final_table${2}'.format(
        BQ_PROJECT, BQ_DATASET, '{{ yesterday_ds_nodash }}'
    ),
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag
)

# Setting up Dependencies
t2.set_upstream(t1)
t4.set_upstream(t2)
t4.set_upstream(t3)


