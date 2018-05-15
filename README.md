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
`pip install -r requirements.txt`
> Step 3 : Create Directory : `airflow_home`

> Step 4 : Clone the Repo:

>> `mkdir pseudo`
>> `cd pseudo`
>> `git clone https://git.clicklabs.in/kato/redshiftEtlLayer.git`
>> `cd ..`


> Step 3 : Airflow HOME

>> export AIRFLOW_HOME=`pwd`/airflow_home

<!-- > Step 4: Install Dependencies

>> `pip install airflow`
>> `pip install apache-airflow[mysql]`
>> `pip install apache-airflow[s3]`
>> `pip install apache-airflow[postgres]`
>> `pip install boto3` -->

> Step 5: Initialize the airflow meta database
>> `python run fernetKey.py`
>> Now copy the output and 
>> `EXPORT AIRFLOW__CORE__FERNET_KEY = <your_fernet_key>`
>>  airflow initdb

> Step 6: Copy mysql, s3, postgress section of airflow.cfg from the pseudo folder into the current one. Also copy the properties from the remote log connection for S3.
>> remote_base_log_folder = scopeworkeretllogs
>> remote_log_conn_id = s3_scopeworker
>> make `load_examples` value to false


> Step 7 : Copy the dags and plugin from the pseudo folder to Airflow_home:

>> `cp -r pseudo/redshiftEtlLayer/airflow_home/dags airflow_home/dags && cp -r pseudo/redshiftEtlLayer/airflow_home/plugins airflow_home/plugins`

>> Step 8 : Change the S3 bucket names and S3 bucket region in all the dags to point to bucket where your intermediate steps will be done.

> Step 9 : Start server as a Daemon
>>  `airflow webserver -D`
>> or without daemon: `airflow webserver -p 8080`

> Step 10 : Start Scheduler as a Daemon

>> `airflow scheduler -D`

> Step 9 : Ensure Create Connection dag is run first