# WGU D608 Sparkify Redshift/Apache Airflow Pipeline Development

## Intro
This repo houses work for my [Udacity 'nanodegree' submission](https://www.udacity.com/certificate/e/04d4ba20-d1d4-11ef-a85d-b772db4b5ffd). 

## Project basics

Tables are created in a serverless Amazon Redshift instance using the 'create_tables.sql' script.

An amazon s3 bucket is created as your-bucket-name.

Environment variables are created in a bash shell script as follows:

```
/opt/airflow/start-services.sh
/opt/airflow/start.sh
airflow users create --email student@example.com --firstname aStudent --lastname aStudent --password admin --role Admin --username admin
airflow variables set s3_bucket your-bucket-name
airflow variables set s3_prefix data-pipelines
airflow scheduler
# airflow connections get aws_credentials -o json
airflow connections add aws_credentials --conn-uri 'whatever you get from the above line'
# airflow connections get redshift -o json
airflow connections add redshift --conn-uri 'whatever you get from the above line'
```
