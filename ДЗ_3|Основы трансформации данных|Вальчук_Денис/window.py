from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

def find_extreme_days(**context):
    file_path = '/opt/airflow/dags/data/IOT-temp.csv'
    df = pd.read_csv(file_path)
    df = df.rename(columns={'room_id/id': 'room_id', 'out/in': 'out_in'})
    
    df = df[df['out_in'] == 'In'].copy()
    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M')
    df['date_only'] = df['noted_date'].dt.date
    
    daily_max = df.groupby('date_only')['temp'].max().reset_index()
    daily_max.columns = ['date', 'max_temp']
    
    hottest_days = daily_max.nlargest(5, 'max_temp')['date'].tolist()
    
    daily_min = df.groupby('date_only')['temp'].min().reset_index()
    daily_min.columns = ['date', 'min_temp']
    
    coldest_days = daily_min.nsmallest(5, 'min_temp')['date'].tolist()
    
    result_records = []
    
    for day in hottest_days:
        day_data = df[df['date_only'] == day]
        if not day_data.empty:
            max_temp = day_data['temp'].max()
            max_records = day_data[day_data['temp'] == max_temp]
            hottest_record = max_records.iloc[0]
            result_records.append({
                'room_id': hottest_record['room_id'],
                'date_only': day,
                'temp': int(hottest_record['temp']),
                'out_in': hottest_record['out_in']
            })
    
    for day in coldest_days:
        day_data = df[df['date_only'] == day]
        if not day_data.empty:
            min_temp = day_data['temp'].min()
            min_records = day_data[day_data['temp'] == min_temp]
            coldest_record = min_records.iloc[0]
            result_records.append({
                'room_id': coldest_record['room_id'],
                'date_only': day,
                'temp': int(coldest_record['temp']),
                'out_in': coldest_record['out_in']
            })
    
    result_df = pd.DataFrame(result_records)
    
    return result_df.to_json(orient='records', date_format='iso')

def clean_temperature_data(**context):
    ti = context['ti']
    data_json = ti.xcom_pull(task_ids='find_extreme_days')
    df = pd.read_json(data_json, orient='records')
    
    if 'date_only' in df.columns:
        df['date_only'] = pd.to_datetime(df['date_only']).dt.date
    
    p5 = np.percentile(df['temp'], 5)
    p95 = np.percentile(df['temp'], 95)
    df = df[(df['temp'] >= p5) & (df['temp'] <= p95)].copy()
    
    final_df = pd.DataFrame({
        'room_id': df['room_id'],
        'noted_date': df['date_only'].astype(str),
        'temp': df['temp'].astype(int),
        'out_in': df['out_in']
    })
    
    return final_df.to_json(orient='records')

def load_to_iot_table(**context):
    ti = context['ti']
    data_json = ti.xcom_pull(task_ids='clean_temperature_data')
    final_df = pd.read_json(data_json, orient='records')
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                create table if not exists iot (
                    id serial primary key,
                    room_id text,
                    noted_date date,
                    temp integer,
                    out_in text
                )
            """)
            conn.commit()
    
    sql_insert = """
        insert into iot (room_id, noted_date, temp, out_in)
        values (%(room_id)s, %(noted_date)s, %(temp)s, %(out_in)s)
    """
    
    records = final_df.to_dict('records')
    
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql_insert, records)
            conn.commit()
    
    return f"успешно загружено {len(final_df)} записей"

dag = DAG(
    'iot_data_processing',
    default_args=default_args,
    catchup=False,
    description='обработка данных iot',
    tags=['iot'],
    schedule_interval=None
)

find_days_task = PythonOperator(
    task_id='find_extreme_days',
    python_callable=find_extreme_days,
    dag=dag,
)

clean_temp_task = PythonOperator(
    task_id='clean_temperature_data',
    python_callable=clean_temperature_data,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_to_iot_table',
    python_callable=load_to_iot_table,
    dag=dag,
)

find_days_task >> clean_temp_task >> load_data_task
