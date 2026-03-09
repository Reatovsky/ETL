from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1)
}

def transform_user_session(doc):
    device = doc['device']
    device_type = device['type']
    device_browser = device['browser']
    device_os = device['os']
    device_ip = device['ip'] 
    duration = doc.get('duration_minutes')
    
    return {
        'session_id': doc['session_id'],
        'user_id': doc['user_id'],
        'start_time': doc['start_time'],
        'end_time': doc['end_time'],
        'pages_visited': doc['pages_visited'],
        'device_type': device_type,
        'device_browser': device_browser,
        'device_os': device_os,
        'device_ip': device_ip,
        'actions': doc['actions'],
        'duration_minutes': duration
    }

def transform_event_log(doc):
    return {
        'event_id': doc['event_id'],
        'timestamp': doc['timestamp'],
        'event_type': doc['event_type'],
        'user_id': doc['user_id'],
        'session_id': doc.get('session_id'),
        'details': json.dumps(doc['details'])
    }

def transform_support_ticket(doc):
    resolution_time = None

    if doc['status'] == 'closed':
        created = datetime.fromisoformat(doc['created_at'].replace('Z', '+00:00'))
        updated = datetime.fromisoformat(doc['updated_at'].replace('Z', '+00:00'))
        resolution_time = round((updated - created).total_seconds() / 3600, 2)
    
    return {
        'ticket_id': doc['ticket_id'],
        'user_id': doc['user_id'],
        'status': doc['status'],
        'issue_type': doc['issue_type'],
        'messages': json.dumps(doc['messages']),
        'created_at': doc['created_at'],
        'updated_at': doc['updated_at'],
        'priority': doc.get('priority'),
        'category': doc.get('category'),
        'resolution_time_hours': resolution_time
    }

def transform_user_recommendation(doc):
    return {
        'user_id': doc['user_id'],
        'recommended_products': doc['recommended_products'],
        'last_updated': doc['last_updated'],
        'recommendation_type': doc.get('recommendation_type'),
        'algorithm_version': doc.get('algorithm_version'),
        'score': doc.get('score')
    }

def transform_moderation_queue(doc):
    return {
        'review_id': doc['review_id'],
        'user_id': doc['user_id'],
        'product_id': doc['product_id'],
        'review_text': doc['review_text'],
        'rating': doc['rating'],
        'moderation_status': doc['moderation_status'],
        'flags': doc['flags'],
        'submitted_at': doc['submitted_at'],
        'approved_at': doc.get('approved_at'),
        'rejected_at': doc.get('rejected_at'),
        'helpful_votes': doc.get('helpful_votes', 0)
    }

def replicate_collection(mongo_collection, pg_table, transform_func, **context):
    mongo_hook = MongoHook(conn_id='mongo_default')
    mongo_client = mongo_hook.get_conn()
    
    mongo_db = mongo_client['airflow_data']
    collection = mongo_db[mongo_collection]
    documents = list(collection.find())
    
    transformed_data = [transform_func(doc) for doc in documents]
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    pk_map = {
        'user_sessions': 'session_id',
        'event_logs': 'event_id',
        'support_tickets': 'ticket_id',
        'user_recommendations': 'user_id',
        'moderation_queue': 'review_id'
    }
    
    primary_key = pk_map[pg_table]
    
    keys = [d[primary_key] for d in transformed_data]
    if keys:
        placeholders = ','.join(['%s'] * len(keys))
        delete_query = f"DELETE FROM {pg_table} WHERE {primary_key} IN ({placeholders})"
        cursor.execute(delete_query, keys)
        print(f"Удалено {cursor.rowcount} записей из {pg_table}")
    
    inserted = 0
    for data in transformed_data:
        columns = list(data.keys())
        values = list(data.values())
        
        values = [v if v is not None else None for v in values]
        
        insert_query = f"""
            INSERT INTO {pg_table} ({', '.join(columns)})
            VALUES ({', '.join(['%s'] * len(columns))})
            ON CONFLICT ({primary_key}) DO UPDATE SET
            {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != primary_key])}
        """
        
        cursor.execute(insert_query, values)
        inserted += 1
    
    conn.commit()
    print(f"Успешно загружено {inserted} записей в {pg_table}")
    
    cursor.close()
    conn.close()

dag = DAG(
    'test_mongo',
    default_args=default_args,
    description='Репликация данных из MongoDB в PostgreSQL с трансформацией',
    schedule_interval='0 */6 * * *',
    catchup=False,
    tags=['replication', 'etl', 'transformation']
)

replicate_sessions = PythonOperator(
    task_id='replicate_user_sessions',
    python_callable=replicate_collection,
    op_kwargs={
        'mongo_collection': 'user_sessions',
        'pg_table': 'user_sessions',
        'transform_func': transform_user_session
    },
    dag=dag
)

replicate_events = PythonOperator(
    task_id='replicate_event_logs',
    python_callable=replicate_collection,
    op_kwargs={
        'mongo_collection': 'event_logs',
        'pg_table': 'event_logs',
        'transform_func': transform_event_log
    },
    dag=dag
)

replicate_tickets = PythonOperator(
    task_id='replicate_support_tickets',
    python_callable=replicate_collection,
    op_kwargs={
        'mongo_collection': 'support_tickets',
        'pg_table': 'support_tickets',
        'transform_func': transform_support_ticket
    },
    dag=dag
)

replicate_recommendations = PythonOperator(
    task_id='replicate_user_recommendations',
    python_callable=replicate_collection,
    op_kwargs={
        'mongo_collection': 'user_recommendations',
        'pg_table': 'user_recommendations',
        'transform_func': transform_user_recommendation
    },
    dag=dag
)

replicate_moderation = PythonOperator(
    task_id='replicate_moderation_queue',
    python_callable=replicate_collection,
    op_kwargs={
        'mongo_collection': 'moderation_queue',
        'pg_table': 'moderation_queue',
        'transform_func': transform_moderation_queue
    },
    dag=dag
)

replicate_sessions >> replicate_events >> replicate_tickets >> replicate_recommendations >> replicate_moderation