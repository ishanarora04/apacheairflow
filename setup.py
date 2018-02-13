from __future__ import absolute_import
from __future__ import unicode_literals

from setuptools import find_packages
from setuptools import setup

setup(
    name='airflow_mysql_to_redshift',
    version='0.1.0',
    description='',
    author='Ishan Arora',
    author_email='ishan.arora@scopeworker.com',
    packages=find_packages(exclude=['tests']),
    setup_requires=['setuptools'],
    install_requires=[
        'boto3>=1.5.18',
        'apache-airflow>=1.8.0',
        'apache-airflow[mysql]>=1.8.0',
        'apache-airflow[s3]>=1.8.0',
        'apache-airflow[postgresql]>=1.8.0',
    ],
    license='Copyright Scopeworker 2018')
