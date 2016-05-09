# coding=utf-8
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta


default_args = {
    'owner': 'intelivix',
    'depends_on_past': False,
    'start_date': datetime(2016, 4, 1),
    'email': ['bruno@intelivix.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('extract_judges', default_args=default_args, schedule_interval="@daily")


def check_updates_with_judges():
    # TODO: Copiar textos com o termo Juiz de Direito para uma tabela de processamento
    return None  # Copiar os dados do postgres para o mongodb fazendo filtro para texto com 'JUIZ'


check_updates_with_judges_task = PythonOperator(
    task_id='check_updates_with_judges',
    python_callable=check_updates_with_judges,
    dag=dag)


def extract_name():
    # TODO: Criar função para extrair o nome do juiz do texto
    return None  # http://blog.yhat.com/posts/named-entities-in-law-and-order-using-nlp.html


def check_name():
    # TODO: Verificar o nome extraido
    return None  # Validar com uma base de nomes de JUIZES (portal da transparencia)


extract_name_task = PythonOperator(
    task_id='extract_name_task',
    python_callable=extract_name,
    dag=dag)

check_name_task = PythonOperator(
    task_id='check_name_task',
    python_callable=check_name,
    dag=dag)

extract_name_task.set_upstream(check_updates_with_judges_task)
check_name_task.set_upstream(extract_name_task)
