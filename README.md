# ETL for Mysql to Redshift

A full fledged production tested ETL for Mysql to Redshift

## Prerequisites
python 2.7
The scripts is tested for only Python 2.7

## Installing

> Step 1: Clone the repo

>> git clone https://git.clicklabs.in/ClickLabs/RedshiftScopeWorkerScripts.git

> Step 2: Virtual Environment

>>Make sure **_virtualenv_** is installed else installed using command: `pip install virtualenv`.

>>Setup virtual environemnt of name **env** with command: `virtualenv env`.

>>Activate the virtual env with : `source env/bin/activate`

> Step 3 : Airflow HOME

>> export AIRFLOW_HOME=`pwd`/airflow_home

> Step 4: Install Dependencies

>> `pip install airflow`
>> `pip install apache-airflow[mysql]`
>> `pip install apache-airflow[s3]`
>> `pip install apache-airflow[postgres]`

> Step 5: Update airflow.cfg and mention the required connection variables

> Step 6: Initialize the airflow meta database

>>  airflow initdb


> Step 7 : Start server
>>  airflow webserver -p 8080

> Step 8 : Start Scheduler

>> airflow scheduler










