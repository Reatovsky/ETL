from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

def transform(**context):
    data = {
        "pets": [
            {
                "name": "Purrsloud",
                "species": "Cat",
                "favFoods": ["wet food", "dry food", "<strong>any</strong> food"],
                "birthYear": 2016,
                "photo": "https://learnwebcode.github.io/json-example/images/cat-2.jpg"
            },
            {
                "name": "Barksalot",
                "species": "Dog",
                "birthYear": 2008,
                "photo": "https://learnwebcode.github.io/json-example/images/dog-1.jpg"
            },
            {
                "name": "Meowsalot",
                "species": "Cat",
                "favFoods": ["tuna", "catnip", "celery"],
                "birthYear": 2012,
                "photo": "https://learnwebcode.github.io/json-example/images/cat-1.jpg"
            }
        ]
    }
    
    linear_lines = []
    structured_data = []
    
    for pet in data['pets']:
        line_parts = []
        animal_dict = {}

        animal_dict['name'] = pet['name']
        animal_dict['species'] = pet['species']
        line_parts.append(f"Имя: {pet['name']}")
        line_parts.append(f"Вид: {pet['species']}")
        
        if 'favFoods' in pet:
            clean_foods = []
            for food in pet['favFoods']:
                clean = food.replace('<strong>', '').replace('</strong>', '')
                clean_foods.append(clean)
            
            favorite_food_str = ', '.join(clean_foods)
            animal_dict['favorite_food'] = favorite_food_str
            line_parts.append(f"Любимая еда: {favorite_food_str}")
        else:
            animal_dict['favorite_food'] = None
        
        animal_dict['birth_year'] = pet['birthYear']
        animal_dict['photo_url'] = pet['photo']
        line_parts.append(f"Год рождения: {pet['birthYear']}")
        line_parts.append(f"Фото: {pet['photo']}")
        
        line = ', '.join(line_parts)
        linear_lines.append(line)
        structured_data.append(animal_dict)
    
    result_text = "\n".join(linear_lines)
    context['ti'].xcom_push(key='linear_text', value=result_text)
    context['ti'].xcom_push(key='structured_data', value=structured_data)
    
    print("Результат:")
    print(result_text)
    
    return "Преобразование завершено"

def save(**context):
    ti = context['ti']
    transformed_data = ti.xcom_pull(task_ids='transform', key='structured_data')
    
    print(f"Получено {len(transformed_data)} записей для сохранения")
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        create_table_sql = """
        create table if not exists animals (
            id serial primary key,
            name text not null,
            species text not null,
            favorite_food text,
            birth_year integer,
            photo_url text
        );
        """
        
        cursor.execute(create_table_sql)
        
        insert_sql = """
        insert into animals (name, species, favorite_food, birth_year, photo_url)
        values (%s, %s, %s, %s, %s);
        """
        
        inserted_count = 0
        for animal in transformed_data:
            cursor.execute(insert_sql, (
                animal['name'],
                animal['species'],
                animal.get('favorite_food'),
                animal.get('birth_year'),
                animal.get('photo_url')
            ))
            inserted_count += cursor.rowcount
        
        conn.commit()
        
        cursor.execute("select count(*) from animals")
        total_count = cursor.fetchone()[0]
        
        print(f"Сохранено записей: {inserted_count}")
        
    finally:
        cursor.close()
        conn.close()

with DAG(
    'transform_json',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['json', 'transform', 'postgres']
) as dag:
    
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform
    )
    
    save_task = PythonOperator(
        task_id='save',
        python_callable=save
    )
    
    transform_task >> save_task