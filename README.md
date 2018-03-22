# ETL for Mysql to Redshift

A full fledged production tested ETL for Mysql to Redshift

### This is a prerequisite for the setting up the Real time solutions. This will support the Real time solutions in inital times.

## Prerequisites
python 2.7
The scripts is tested for only Python 2.7

## Installing

> Step 1 : Creating the Directory

>>mkdir airflow_scripts

>>cd airflow_scripts

> Step 2: Virtual Environment

>>Make sure **_virtualenv_** is installed else installed using command: `pip install virtualenv`.

>>Setup virtual environemnt of name **env** with command: `virtualenv env`.

>>Activate the virtual env with : `source env/bin/activate`

> Step 3 : Create Directory : `airflow_home`

> Step 4 : Clone the Repo:

>> `mkdir pseudo`
>> `cd pseudo`
>> `git clone https://git.clicklabs.in/kato/redshiftEtlLayer.git`
>> `cd ..`


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

> Step 7 : Copy the dags and plugin from the pseudo folder to Airflow_home:

>> `cp -r pseudo/dags airflow_home/dags`
>> `cp -r pseudo/plugins airflow_home/plugins`

> Step 7 : Start server as a Daemon
>>  `airflow webserver -D`

> Step 8 : Start Scheduler as a Daemon

>> `airflow scheduler -D`


> Step 9 : Ensure Create Connection dag is run first







