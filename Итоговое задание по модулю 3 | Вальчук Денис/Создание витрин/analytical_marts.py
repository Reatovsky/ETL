from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1)
}

def create_user_activity_mart():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("DROP TABLE IF EXISTS mart_user_activity;")
    create_mart = """
    CREATE TABLE mart_user_activity AS
    WITH user_sessions_stats AS (
        SELECT 
            user_id,
            COUNT(*) as total_sessions,
            AVG(duration_minutes) as avg_session_duration,
            SUM(duration_minutes) as total_time_minutes,
            COUNT(DISTINCT DATE(start_time)) as active_days,
            MODE() WITHIN GROUP (ORDER BY device_type) as preferred_device
        FROM user_sessions
        GROUP BY user_id
    ),
    user_events_stats AS (
        SELECT 
            user_id,
            COUNT(*) as total_events,
            COUNT(DISTINCT event_type) as unique_event_types,
            COUNT(*) FILTER (WHERE event_type = 'page_view') as page_views,
            COUNT(*) FILTER (WHERE event_type = 'add_to_cart') as add_to_cart,
            COUNT(*) FILTER (WHERE event_type = 'checkout_started') as checkout_starts,
            COUNT(*) FILTER (WHERE event_type = 'payment_success') as payments
        FROM event_logs
        GROUP BY user_id
    ),
    user_tickets_stats AS (
        SELECT 
            user_id,
            COUNT(*) as total_tickets,
            COUNT(*) FILTER (WHERE status = 'open') as open_tickets,
            COUNT(*) FILTER (WHERE status = 'closed') as closed_tickets,
            AVG(resolution_time_hours) as avg_resolution_time
        FROM support_tickets
        GROUP BY user_id
    ),
    user_reviews_stats AS (
        SELECT 
            user_id,
            COUNT(*) as total_reviews,
            AVG(rating) as avg_rating,
            COUNT(*) FILTER (WHERE moderation_status = 'pending') as pending_reviews
        FROM moderation_queue
        GROUP BY user_id
    )
    
    SELECT 
        COALESCE(s.user_id, e.user_id, t.user_id, r.user_id) as user_id,
        COALESCE(s.total_sessions, 0) as total_sessions,
        COALESCE(s.avg_session_duration, 0) as avg_session_duration,
        COALESCE(s.total_time_minutes, 0) as total_time_minutes,
        COALESCE(s.active_days, 0) as active_days,
        s.preferred_device,
        COALESCE(e.total_events, 0) as total_events,
        COALESCE(e.unique_event_types, 0) as unique_event_types,
        COALESCE(e.page_views, 0) as page_views,
        COALESCE(e.add_to_cart, 0) as add_to_cart,
        COALESCE(e.checkout_starts, 0) as checkout_starts,
        COALESCE(e.payments, 0) as payments,
        CASE 
            WHEN e.page_views > 0 THEN 
                ROUND(100.0 * e.payments / e.page_views, 2)
            ELSE 0 
        END as conversion_rate,
        COALESCE(t.total_tickets, 0) as total_tickets,
        COALESCE(t.open_tickets, 0) as open_tickets,
        COALESCE(t.closed_tickets, 0) as closed_tickets,
        COALESCE(t.avg_resolution_time, 0) as avg_ticket_resolution_hours,
        COALESCE(r.total_reviews, 0) as total_reviews,
        COALESCE(r.avg_rating, 0) as avg_rating,
        COALESCE(r.pending_reviews, 0) as pending_reviews,
        CURRENT_TIMESTAMP as updated_at
    FROM user_sessions_stats s
    FULL OUTER JOIN user_events_stats e ON s.user_id = e.user_id
    FULL OUTER JOIN user_tickets_stats t ON COALESCE(s.user_id, e.user_id) = t.user_id
    FULL OUTER JOIN user_reviews_stats r ON COALESCE(s.user_id, e.user_id, t.user_id) = r.user_id;
    """
    
    cursor.execute(create_mart)
    conn.commit()
    
    cursor.close()
    conn.close()

def create_support_mart():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("DROP TABLE IF EXISTS mart_support_efficiency;")
    cursor.execute("DROP TABLE IF EXISTS mart_open_tickets;")
    
    create_mart1 = """
    CREATE TABLE mart_support_efficiency AS
    WITH daily_stats AS (
        SELECT 
            DATE(created_at) as ticket_date,
            COUNT(*) as total_tickets,
            COUNT(*) FILTER (WHERE status = 'open') as open_tickets,
            COUNT(*) FILTER (WHERE status = 'closed') as closed_tickets,
            COUNT(*) FILTER (WHERE status = 'in_progress') as in_progress_tickets,
            COUNT(DISTINCT issue_type) as unique_issue_types,
            COUNT(DISTINCT user_id) as unique_users,
            AVG(resolution_time_hours) FILTER (WHERE status = 'closed') as avg_resolution_time,
            MIN(resolution_time_hours) FILTER (WHERE status = 'closed') as min_resolution_time,
            MAX(resolution_time_hours) FILTER (WHERE status = 'closed') as max_resolution_time
        FROM support_tickets
        GROUP BY DATE(created_at)
    ),
    issue_type_stats AS (
        SELECT 
            DATE(created_at) as ticket_date,
            issue_type,
            COUNT(*) as type_count,
            AVG(resolution_time_hours) FILTER (WHERE status = 'closed') as type_avg_resolution
        FROM support_tickets
        GROUP BY DATE(created_at), issue_type
    ),
    priority_stats AS (
        SELECT 
            DATE(created_at) as ticket_date,
            priority,
            COUNT(*) as priority_count
        FROM support_tickets
        GROUP BY DATE(created_at), priority
    )
    
    SELECT 
        d.ticket_date,
        d.total_tickets,
        d.open_tickets,
        d.closed_tickets,
        d.in_progress_tickets,
        d.unique_issue_types,
        d.unique_users,
        ROUND(100.0 * d.closed_tickets / NULLIF(d.total_tickets, 0), 2) as closure_rate,
        ROUND(COALESCE(d.avg_resolution_time, 0), 2) as avg_resolution_time_hours,
        ROUND(COALESCE(d.min_resolution_time, 0), 2) as min_resolution_time_hours,
        ROUND(COALESCE(d.max_resolution_time, 0), 2) as max_resolution_time_hours,
        (SELECT jsonb_object_agg(issue_type, type_count) 
         FROM issue_type_stats i 
         WHERE i.ticket_date = d.ticket_date) as issue_type_breakdown,
        (SELECT jsonb_object_agg(priority, priority_count) 
         FROM priority_stats p 
         WHERE p.ticket_date = d.ticket_date) as priority_breakdown,
        CURRENT_TIMESTAMP as updated_at
    FROM daily_stats d
    ORDER BY d.ticket_date DESC;
    """
    
    cursor.execute(create_mart1)
    
    create_mart2 = """
    CREATE TABLE mart_open_tickets AS
    SELECT 
        ticket_id,
        user_id,
        issue_type,
        priority,
        category,
        created_at,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_at))/3600 as hours_open,
        CASE 
            WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_at))/3600 > 48 THEN 'Критично'
            WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_at))/3600 > 24 THEN 'Внимание'
            ELSE 'Нормально'
        END as aging_status,
        (SELECT COUNT(*) FROM jsonb_array_elements(messages)) as message_count
    FROM support_tickets
    WHERE status IN ('open', 'in_progress')
    ORDER BY created_at;
    """
    
    cursor.execute(create_mart2)
    conn.commit()
    
    cursor.close()
    conn.close()

dag = DAG(
    'analytical_marts',
    default_args=default_args,
    description='Создание аналитических витрин',
    catchup=False,
    tags=['analytics', 'marts']
)

create_user_mart = PythonOperator(
    task_id='create_user_activity_mart',
    python_callable=create_user_activity_mart,
    dag=dag
)

create_support_mart = PythonOperator(
    task_id='create_support_mart',
    python_callable=create_support_mart,
    dag=dag
)

create_user_mart >> create_support_mart