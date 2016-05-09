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


PROGRESS_TYPES = {
    u'sentenca': u'sentença',
    u'juntada': u'juntada',
    u'improcedente': u'improcedente'
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


def extract_type(ds, **kwargs):
    year, month, day = ds.split('-')  # 2016-04-22
    c_ds = "%s/%s/%s" % (day, month, year)  # 15/12/2014
    count = 0
    tp = kwargs['tp']
    keyword = kwargs['keyword']
    for andamento in Andamentos.objects(data=c_ds):
        texto_lw = andamento.texto.lower()
        if keyword in texto_lw:
            andamento.tipo = tp
            andamento.save()
            count += 1
    return count


for tp in PROGRESS_TYPES:
    extract_tipo_task = PythonOperator(
        task_id='extract_%s_task' % (tp,),
        python_callable=extract_type, op_kwargs={'tp': tp, 'keyword': PROGRESS_TYPES[tp]},
        dag=dag, provide_context=True)
    extract_tipo_task.set_upstream(load_new_data_task)
