from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import os

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
    
    for pet in data['pets']:
        line_parts = []

        line_parts.append(f"Имя: {pet['name']}")
        line_parts.append(f"Вид: {pet['species']}")
        
        if 'favFoods' in pet:
            clean_foods = []

            for food in pet['favFoods']:
                clean = food.replace('<strong>', '').replace('</strong>', '')
                clean_foods.append(clean)

            line_parts.append(f"Любимая еда: {', '.join(clean_foods)}")
        
        line_parts.append(f"Год рождения: {pet['birthYear']}")
        line_parts.append(f"Фото: {pet['photo']}")
        
        line = ', '.join(line_parts)
        linear_lines.append(line)
    
    result_text = "\n".join(linear_lines)
    context['ti'].xcom_push(key='linear_text', value=result_text)
    
    print("Результат:")
    print(result_text)
    
    return "Преобразование завершено"

def save(**context):
    linear_text = context['ti'].xcom_pull(
        task_ids='transform', 
        key='linear_text'
    )
    
    output_dir = '/opt/airflow/output'
    os.makedirs(output_dir, exist_ok=True)

    filename = f'json_linear.txt'
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(linear_text)
    
    print("Файл сохранён. ")
    print(f"Путь: {filepath}")
    
    print("\nСодержимое:")
    with open(filepath, 'r', encoding='utf-8') as f:
        print(f.read())
    
    return f"Файл сохранен: {filepath}"

with DAG(
    'transform_json',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['json', 'transform', 'file']
) as dag:
    
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context=True
    )
    
    save_task = PythonOperator(
        task_id='save',
        python_callable=save,
        provide_context=True
    )
    
    transform_task >> save_task
