#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example DAG demonstrating the usage of the PythonOperator."""
import time
from pprint import pprint

from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

args = {
    'owner': 'airflow',
    'depends_on_past': True
}

with DAG(
    dag_id='dag_test_agam.py',
    default_args=args,
    schedule_interval='*/5 * * * *',
    start_date=days_ago(2),
    catchup=False
    tags=['example'],
) as dag:

    # [START howto_operator_python]
    task1 = DummyOperator(
        task_id='run_this_last',
    )

    # [START howto_operator_bash]
    task2 = BashOperator(
        task_id='run_after_loop',
        bash_command='echo 1',
    )
    
 task1 << task2