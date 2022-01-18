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
from airflow.models import DagRun
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import TaskInstance
from airflow.api.common.experimental.get_task_instance import get_task_instance

args = {
    'owner': 'airflow'
}



def get_dag_status():
    dag_ls = ['example_bash_operator']
    dags_status={}
    for dag_id in dag_ls:
       dag_runs = DagRun.find(dag_id=dag_id)
       print(dag_runs[-1])
      #  for dag_run in dag_runs[-1]:
       dag_run=dag_runs[-1]
       if dag_run.state=='running':
          task_list = dag_run.get_task_instances("running")
         
       else:
           task_list = dag_run.get_task_instances("success")
       print(dag_run.execution_date)
       print(dag_run.run_id.split('__')[0])
       print('yahan')
       print(task_list[0].task_id) 
       ti = get_task_instance(dag_id,task_list[0].task_id, dag_run.execution_date)
       print(ti.duration)
           #   task_status = TaskInstance(operator_instance, execution_date).current_state()
             # dags_status[dag_id] = dag_run.state+' and it is currently executing '+ operator_instance
        
            
    
    #dag_status_mail(dags_status)
    
    
#def dag_status_mail(dags_status):
    
          

with DAG(
    dag_id='dag_test_agam.py',
    default_args=args,
    schedule_interval='*/5 * * * *',
    start_date=days_ago(2),
    catchup=False,
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
    
    task3  = PythonOperator(
        task_id='task3',
        python_callable=get_dag_status,
        provide_context=True
    )
    
    
    task1 >> task2 >> task3