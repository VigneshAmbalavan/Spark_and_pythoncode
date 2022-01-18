from datetime import timedelta
from datetime import datetime
import datetime as dt
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
#from airflow.operators.sensors import ExternalTaskSensor
from airflow.sensors.base import BaseSensorOperator
#from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.dates import days_ago
from airflow.utils.helpers import chain
from airflow.models import Variable
from airflow.utils.state import State
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
import logging

 

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization

 

vtime = datetime.now(dt.timezone.utc) - timedelta(minutes=60)
validation_time = str(vtime.strftime('%Y-%m-%d %H:%M:%S %Z'))[:23]

 


default_args = {
    'owner': 'zhongti2',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 6,
    'retry_delay': timedelta(minutes=5),
}

 

def return_branch(ts_nodash, **kwargs):
    
    logging.info(ts_nodash[9::])
    
    if  ts_nodash[9::] == '120600':
       
       return "check_status_of_scout_europe_landing_merge"
    elif ts_nodash[9::] == '120800':
       
       return  "check_status_of_10pm_iqvia_sandoz_ita_landing_merge"
    elif ts_nodash[9::] == '120900':
       return   "check_status_of_10am_iqvia_sandoz_ita_landing_merge"
    

 

    
    

 

 

dag = DAG(
    'dag_deshared_product_unified_test',
    default_args=default_args,
    description='This DAG will run ETL_framework start and monitoring scripts for file based ingestion',
    schedule_interval='06,08,09,10 12 * * *',
    catchup=False
)

 

 

 


start_product_unified = BashOperator(
    task_id='start_product_unified',
    bash_command='echo start',
    dag=dag
)

 


branching = BranchPythonOperator(
        task_id='branching',
        python_callable=return_branch,
        dag=dag,
        op_kwargs={'ts_nodash': '{{ ts_nodash }}'},
        provide_context=True)

 


check_status_of_scout_europe_landing_merge_test_test_test = ExternalTaskSensor(
    task_id='check_status_of_scout_europe_landing_merge_test_test_test',
    external_dag_id='dag_deshared_scout_europe_landing_merge',
    external_task_id='dependency_check_landing_merge_jobs',
    allowed_states=[State.SUCCESS],
    execution_delta=timedelta(minutes=1350),
    mode="reschedule",
    check_existence=True,
    timeout=4800,
    dag=dag
)

 

submit_scout_europe_product_unified = BashOperator(
    task_id='submit_scout_europe_product_unified',
    bash_command='echo 1',
    dag=dag,
)

 

monitor_scout_europe_product_unified = BashOperator(
    task_id='monitor_scout_europe_product_unified',
    bash_command='echo 1',
    dag=dag,
)

 

notify_scout_europe_product_unified = BashOperator(
    task_id='notify_scout_europe_product_unified',
    bash_command='echo 1',
    dag=dag,
)

 

stop_scout_europe_product_unified = BashOperator(
    task_id='stop_scout_europe_product_unified',
    bash_command='echo stop',
    dag=dag
)
  

 

 


check_status_of_10pm_iqvia_sandoz_ita_landing_merge = ExternalTaskSensor(
    task_id='check_status_of_10pm_iqvia_sandoz_ita_landing_merge',
    external_dag_id='dag_deshared_iqvia_sandoz_ita_landing_merge',
    external_task_id='dependency_check_iqvia_sandoz_ita_landing_merge',
    allowed_states=[State.SUCCESS],
    execution_delta=timedelta(minutes=360),
    mode="reschedule",
    check_existence=True,
    timeout=4800,
    dag=dag
)

 

 

check_status_of_10am_iqvia_sandoz_ita_landing_merge = ExternalTaskSensor(
    task_id='check_status_of_10am_iqvia_sandoz_ita_landing_merge',
    external_dag_id='dag_deshared_iqvia_sandoz_ita_landing_merge',
    external_task_id='dependency_check_iqvia_sandoz_ita_landing_merge',
    allowed_states=[State.SUCCESS],
    execution_delta=timedelta(minutes=60),
    mode="reschedule",
    check_existence=True,
    timeout=4800,
    dag=dag
)

 


submit_iqvia_product_unified = BashOperator(
    task_id='submit_iqvia_product_unified',
    bash_command='echo 1',
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    dag=dag
)

 

monitor_iqvia_product_unified = BashOperator(
    task_id='monitor_iqvia_product_unified',
    bash_command='echo 1',
    dag=dag,
)

 

notify_iqvia_product_unified = BashOperator(
    task_id='notify_iqvia_product_unified',
    bash_command='echo 1',
    dag=dag,
)

 

dependency_check_iqvia_product_unified = BashOperator(
    task_id='dependency_check_iqvia_product_unified',
    bash_command='echo 1',
    dag=dag,
)

 

stop_iqvia_product_unified = BashOperator(
    task_id='stop_iqvia_product_unified',
    bash_command='echo 1',
    dag=dag,
)
 


# t1, t2 and t3 are examples of tasks created by instantiating operators

 

 

 

start_product_unified >> branching >> [check_status_of_10am_iqvia_sandoz_ita_landing_merge,check_status_of_scout_europe_landing_merge_test_test_test,check_status_of_10pm_iqvia_sandoz_ita_landing_merge]

check_status_of_10am_iqvia_sandoz_ita_landing_merge >> submit_iqvia_product_unified >> monitor_iqvia_product_unified >> notify_iqvia_product_unified >> dependency_check_iqvia_product_unified >> stop_iqvia_product_unified
check_status_of_10pm_iqvia_sandoz_ita_landing_merge >> submit_iqvia_product_unified >> monitor_iqvia_product_unified >> notify_iqvia_product_unified >> dependency_check_iqvia_product_unified >> stop_iqvia_product_unified
check_status_of_scout_europe_landing_merge_test_test_test >> submit_scout_europe_product_unified >> monitor_scout_europe_product_unified >> notify_scout_europe_product_unified >> stop_scout_europe_product_unified