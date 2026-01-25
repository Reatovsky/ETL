from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import xml.etree.ElementTree as ET
import os

def transform_xml(**context):
    xml_data = """<?xml version="1.0"?>
<nutrition>
<daily-values>
    <total-fat units="g">65</total-fat>
    <saturated-fat units="g">20</saturated-fat>
    <cholesterol units="mg">300</cholesterol>
    <sodium units="mg">2400</sodium>
    <carb units="g">300</carb>
    <fiber units="g">25</fiber>
    <protein units="g">50</protein>
</daily-values>
<food>
    <name>Avocado Dip</name>
    <mfr>Sunnydale</mfr>
    <serving units="g">29</serving>
    <calories total="110" fat="100"/>
    <total-fat>11</total-fat>
    <saturated-fat>3</saturated-fat>
    <cholesterol>5</cholesterol>
    <sodium>210</sodium>
    <carb>2</carb>
    <fiber>0</fiber>
    <protein>1</protein>
    <vitamins>
        <a>0</a>
        <c>0</c>
    </vitamins>
    <minerals>
        <ca>0</ca>
        <fe>0</fe>
    </minerals>
</food>
<food>
    <name>Bagels, New York Style </name>
    <mfr>Thompson</mfr>
    <serving units="g">104</serving>
    <calories total="300" fat="35"/>
    <total-fat>4</total-fat>
    <saturated-fat>1</saturated-fat>
    <cholesterol>0</cholesterol>
    <sodium>510</sodium>
    <carb>54</carb>
    <fiber>3</fiber>
    <protein>11</protein>
    <vitamins>
        <a>0</a>
        <c>0</c>
    </vitamins>
    <minerals>
        <ca>8</ca>
        <fe>20</fe>
    </minerals>
</food>
<food>
    <name>Beef Frankfurter, Quarter Pound </name>
    <mfr>Armitage</mfr>
    <serving units="g">115</serving>
    <calories total="370" fat="290"/>
    <total-fat>32</total-fat>
    <saturated-fat>15</saturated-fat>
    <cholesterol>65</cholesterol>
    <sodium>1100</sodium>
    <carb>8</carb>
    <fiber>0</fiber>
    <protein>13</protein>
    <vitamins>
        <a>0</a>
        <c>2</c>
    </vitamins>
    <minerals>
        <ca>1</ca>
        <fe>6</fe>
    </minerals>
</food>
<food>
    <name>Chicken Pot Pie</name>
    <mfr>Lakeson</mfr>
    <serving units="g">198</serving>
    <calories total="410" fat="200"/>
    <total-fat>22</total-fat>
    <saturated-fat>9</saturated-fat>
    <cholesterol>25</cholesterol>
    <sodium>810</sodium>
    <carb>42</carb>
    <fiber>2</fiber>
    <protein>10</protein>
    <vitamins>
        <a>20</a>
        <c>2</c>
    </vitamins>
    <minerals>
        <ca>2</ca>
        <fe>10</fe>
    </minerals>
</food>
<food>
    <name>Cole Slaw</name>
    <mfr>Fresh Quick</mfr>
    <serving units=" cup">1.5</serving>
    <calories total="20" fat="0"/>
    <total-fat>0</total-fat>
    <saturated-fat>0</saturated-fat>
    <cholesterol>0</cholesterol>
    <sodium>15</sodium>
    <carb>5</carb>
    <fiber>2</fiber>
    <protein>1</protein>
    <vitamins>
        <a>30</a>
        <c>45</c>
    </vitamins>
    <minerals>
        <ca>4</ca>
        <fe>2</fe>
    </minerals>
</food>
<food>
    <name>Eggs</name>
    <mfr>Goodpath</mfr>
    <serving units="g">50</serving>
    <calories total="70" fat="40"/>
    <total-fat>4.5</total-fat>
    <saturated-fat>1.5</saturated-fat>
    <cholesterol>215</cholesterol>
    <sodium>65</sodium>
    <carb>1</carb>
    <fiber>0</fiber>
    <protein>6</protein>
    <vitamins>
        <a>6</a>
        <c>0</c>
    </vitamins>
    <minerals>
        <ca>2</ca>
        <fe>4</fe>
    </minerals>
</food>
<food>
    <name>Hazelnut Spread</name>
    <mfr>Ferreira</mfr>
    <serving units="tbsp">2</serving>
    <calories total="200" fat="90"/>
    <total-fat>10</total-fat>
    <saturated-fat>2</saturated-fat>
    <cholesterol>0</cholesterol>
    <sodium>20</sodium>
    <carb>23</carb>
    <fiber>2</fiber>
    <protein>3</protein>
    <vitamins>
        <a>0</a>
        <c>0</c>
    </vitamins>
    <minerals>
        <ca>6</ca>
        <fe>4</fe>
    </minerals>
</food>
<food>
    <name>Potato Chips</name>
    <mfr>Lees</mfr>
    <serving units="g">28</serving>
    <calories total="150" fat="90"/>
    <total-fat>10</total-fat>
    <saturated-fat>3</saturated-fat>
    <cholesterol>0</cholesterol>
    <sodium>180</sodium>
    <carb>15</carb>
    <fiber>1</fiber>
    <protein>2</protein>
    <vitamins>
        <a>0</a>
        <c>10</c>
    </vitamins>
    <minerals>
        <ca>0</ca>
        <fe>0</fe>
    </minerals>
</food>
<food>
    <name>Soy Patties, Grilled</name>
    <mfr>Gardenproducts</mfr>
    <serving units="g">96</serving>
    <calories total="160" fat="45"/>
    <total-fat>5</total-fat>
    <saturated-fat>0</saturated-fat>
    <cholesterol>0</cholesterol>
    <sodium>420</sodium>
    <carb>10</carb>
    <fiber>4</fiber>
    <protein>9</protein>
    <vitamins>
        <a>0</a>
        <c>0</c>
    </vitamins>
    <minerals>
        <ca>0</ca>
        <fe>0</fe>
    </minerals>
</food>
<food>
    <name>Truffles, Dark Chocolate</name>
    <mfr>Lyndon's</mfr>
    <serving units="g">39</serving>
    <calories total="220" fat="170"/>
    <total-fat>19</total-fat>
    <saturated-fat>14</saturated-fat>
    <cholesterol>25</cholesterol>
    <sodium>10</sodium>
    <carb>16</carb>
    <fiber>1</fiber>
    <protein>1</protein>
    <vitamins>
        <a>0</a>
        <c>0</c>
    </vitamins>
    <minerals>
        <ca>0</ca>
        <fe>0</fe>
    </minerals>
</food>
</nutrition>"""
    
    root = ET.fromstring(xml_data)
    linear_lines = []
    
    daily_values = root.find('daily-values')
    if daily_values is not None:
        daily_line_parts = ["Суточные нормы:"]
        
        total_fat_dv = daily_values.find('total-fat').text
        sat_fat_dv = daily_values.find('saturated-fat').text
        cholesterol_dv = daily_values.find('cholesterol').text
        sodium_dv = daily_values.find('sodium').text
        carb_dv = daily_values.find('carb').text
        fiber_dv = daily_values.find('fiber').text
        protein_dv = daily_values.find('protein').text
        
        daily_line_parts.append(f"Жиры: {total_fat_dv}г")
        daily_line_parts.append(f"Насыщенные жиры: {sat_fat_dv}г")
        daily_line_parts.append(f"Холестерин: {cholesterol_dv}мг")
        daily_line_parts.append(f"Натрий: {sodium_dv}мг")
        daily_line_parts.append(f"Углеводы: {carb_dv}г")
        daily_line_parts.append(f"Клетчатка: {fiber_dv}г")
        daily_line_parts.append(f"Белки: {protein_dv}г")
        
        daily_line = '; '.join(daily_line_parts)
        linear_lines.append(daily_line)
    
    food_items = root.findall('food')
    print(f"Найдено продуктов: {len(food_items)}")
    
    for i, food in enumerate(food_items):
        line_parts = []
        
        name = food.find('name').text.strip()
        mfr = food.find('mfr').text.strip()
        
        serving_elem = food.find('serving')
        serving_value = serving_elem.text.strip()
        serving_units = serving_elem.get('units', '')
        serving = f"{serving_value} {serving_units}"
        
        calories_elem = food.find('calories')
        total_calories = calories_elem.get('total', '0')
        fat_calories = calories_elem.get('fat', '0')
        calories = f"{total_calories} ({fat_calories} из жиров)"
        
        total_fat = food.find('total-fat').text.strip()
        saturated_fat = food.find('saturated-fat').text.strip()
        cholesterol = food.find('cholesterol').text.strip()
        sodium = food.find('sodium').text.strip()
        carb = food.find('carb').text.strip()
        fiber = food.find('fiber').text.strip()
        protein = food.find('protein').text.strip()
        
        vitamins_elem = food.find('vitamins')
        vitamin_a = vitamins_elem.find('a').text.strip()
        vitamin_c = vitamins_elem.find('c').text.strip()
        vitamins = f"A:{vitamin_a}%, C:{vitamin_c}%"
      
        minerals_elem = food.find('minerals')
        calcium = minerals_elem.find('ca').text.strip()
        iron = minerals_elem.find('fe').text.strip()
        minerals = f"Ca:{calcium}%, Fe:{iron}%"
        
        line_parts.append(f"Продукт: {name}")
        line_parts.append(f"Производитель: {mfr}")
        line_parts.append(f"Порция: {serving}")
        line_parts.append(f"Калории: {calories}")
        line_parts.append(f"Жиры: {total_fat}г")
        line_parts.append(f"Насыщенные жиры: {saturated_fat}г")
        line_parts.append(f"Холестерин: {cholesterol}мг")
        line_parts.append(f"Натрий: {sodium}мг")
        line_parts.append(f"Углеводы: {carb}г")
        line_parts.append(f"Клетчатка: {fiber}г")
        line_parts.append(f"Белок: {protein}г")
        line_parts.append(f"Витамины: {vitamins}")
        line_parts.append(f"Минералы: {minerals}")
        
        line = ', '.join(line_parts)
        linear_lines.append(line)
        
        print(f"Обработан продукт {i+1}: {name}")
    
    result_text = "\n".join(linear_lines)
    context['ti'].xcom_push(key='linear_text', value=result_text)
    
    print("\nРезультат преобразования:")
    print(result_text)
    print(f"\nВсего строк создано: {len(linear_lines)}")
    
    return "Преобразование XML завершено"

def save_xml(**context):
    linear_text = context['ti'].xcom_pull(
        task_ids='transform_xml', 
        key='linear_text'
    )
    
    output_dir = '/opt/airflow/output'
    os.makedirs(output_dir, exist_ok=True)

    filename = 'xml_nutrition.txt'
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(linear_text)
    
    print("Файл сохранён.")
    print(f"Путь: {filepath}")
    
    print(f"\nСодержимое файла ({len(linear_text.split(chr(10)))} строк):")
    print(linear_text)
    
    return f"Файл сохранен: {filepath}"

with DAG(
    'transform_xml',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
    },
    tags=['xml', 'nutrition', 'transform', 'file']
) as dag:
    
    transform_task = PythonOperator(
        task_id='transform_xml',
        python_callable=transform_xml,
        provide_context=True
    )
    
    save_task = PythonOperator(
        task_id='save_xml',
        python_callable=save_xml,
        provide_context=True
    )
    
    transform_task >> save_task
