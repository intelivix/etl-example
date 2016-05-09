# coding=utf-8
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from mongoengine import *

connect('tjce')


class Andamentos(Document):
    processo = StringField()
    texto = StringField()
    data = StringField()
    tipo = StringField()

TIPOS = {
    u'Sentença': [u'sentença'],
    u'Juntada': [u'juntada'],
    u'Improcedente': [u'julgado', u'improcedente']
}

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

dag = DAG('update_progress', default_args=default_args)


def load_new_data():
    return None  # load new data to mongodb

load_new_data_task = PythonOperator(
    task_id='load_new_data',
    python_callable=load_new_data,
    dag=dag)


def extract_sentencas(*args, **kwargs):
    ds = kwargs['ds'] # 2016-04-22
    year, month, day = ds.split('-')
    c_ds = "%s/%s/%s" % (day, month, year)  # 15/12/2014
    count = 0
    for andamento in Andamentos.objects(data=c_ds):
        if u"sentença" in andamento.texto.lower():
            andamento.tipo = u"Sentença"
            andamento.save()
            count += 1
    return count


extract_tipo_task = PythonOperator(
    task_id='extract_sentenca_task',
    python_callable=extract_sentencas,
    dag=dag, provide_context=True)
extract_tipo_task.set_upstream(load_new_data_task)




