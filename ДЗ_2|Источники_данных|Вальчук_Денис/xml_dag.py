from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import xml.etree.ElementTree as ET

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
    structured_data = {
        'daily_values': [],
        'products': []
    }
    
    daily_values = root.find('daily-values')
    if daily_values is not None:
        structured_data['daily_values'].extend([
            {'nutrient_name': 'Жиры', 'amount': f"{daily_values.find('total-fat').text}г"},
            {'nutrient_name': 'Насыщенные жиры', 'amount': f"{daily_values.find('saturated-fat').text}г"},
            {'nutrient_name': 'Холестерин', 'amount': f"{daily_values.find('cholesterol').text}мг"},
            {'nutrient_name': 'Натрий', 'amount': f"{daily_values.find('sodium').text}мг"},
            {'nutrient_name': 'Углеводы', 'amount': f"{daily_values.find('carb').text}г"},
            {'nutrient_name': 'Клетчатка', 'amount': f"{daily_values.find('fiber').text}г"},
            {'nutrient_name': 'Белки', 'amount': f"{daily_values.find('protein').text}г"}
        ])
        print(f"Найдено суточных норм: {len(structured_data['daily_values'])}")
    
    food_items = root.findall('food')
    print(f"Найдено продуктов: {len(food_items)}")
    
    for i, food in enumerate(food_items):
        name = food.find('name').text.strip()
        mfr = food.find('mfr').text.strip()
            
        serving_elem = food.find('serving')
        serving_value = serving_elem.text.strip()
        serving_units = serving_elem.get('units', '')
        serving = f"{serving_value} {serving_units}"
            
        calories_elem = food.find('calories')
        total_calories = int(calories_elem.get('total', '0'))
        fat_calories = int(calories_elem.get('fat', '0'))
            
        total_fat = float(food.find('total-fat').text.strip())
        saturated_fat = float(food.find('saturated-fat').text.strip())
        cholesterol = float(food.find('cholesterol').text.strip())
        sodium = float(food.find('sodium').text.strip())
        carb = float(food.find('carb').text.strip())
        fiber = float(food.find('fiber').text.strip())
        protein = float(food.find('protein').text.strip())
            
        vitamins_elem = food.find('vitamins')
        vitamin_a = int(vitamins_elem.find('a').text.strip())
        vitamin_c = int(vitamins_elem.find('c').text.strip())
            
        minerals_elem = food.find('minerals')
        calcium = int(minerals_elem.find('ca').text.strip())
        iron = int(minerals_elem.find('fe').text.strip())
            
        structured_data['products'].append({
            'product_name': name,
            'manufacturer': mfr,
            'serving_size': serving,
            'total_calories': total_calories,
            'calories_from_fat': fat_calories,
            'fat_g': total_fat,
            'saturated_fat_g': saturated_fat,
            'cholesterol_mg': cholesterol,
            'sodium_mg': sodium,
            'carbohydrates_g': carb,
            'fiber_g': fiber,
            'protein_g': protein,
            'vitamin_a_percent': vitamin_a,
            'vitamin_c_percent': vitamin_c,
            'calcium_percent': calcium,
            'iron_percent': iron
        })
            
    context['ti'].xcom_push(key='structured_data', value=structured_data)
    
    print(f"Суточных норм: {len(structured_data['daily_values'])}")
    print(f"Продуктов: {len(structured_data['products'])}")

def save_xml(**context):
    structured_data = context['ti'].xcom_pull(
        task_ids='transform_xml', 
        key='structured_data')
    
    print(f"Суточные нормы: {len(structured_data.get('daily_values', []))}")
    print(f"Продукты: {len(structured_data.get('products', []))}")
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            create table if not exists daily_nutrition (
                id serial primary key,
                nutrient_name text,
                amount text,
                unique(nutrient_name)
            );
        """)
        
        cursor.execute("""
            create table if not exists products (
                id serial primary key,
                product_name text,
                manufacturer text,
                serving_size text,
                total_calories integer,
                calories_from_fat integer,
                fat_g numeric(10,2),
                saturated_fat_g numeric(10,2),
                cholesterol_mg numeric(10,2),
                sodium_mg numeric(10,2),
                carbohydrates_g numeric(10,2),
                fiber_g numeric(10,2),
                protein_g numeric(10,2),
                vitamin_a_percent integer,
                vitamin_c_percent integer,
                calcium_percent integer,
                iron_percent integer
            );
        """)
        
        daily_values = structured_data.get('daily_values', [])
        if daily_values:
            insert_daily_sql = """
            insert into daily_nutrition (nutrient_name, amount)
            values (%s, %s);
            """
            
            daily_inserted = 0
            for nutrient in daily_values:
                cursor.execute(insert_daily_sql, (
                    nutrient.get('nutrient_name'),
                    nutrient.get('amount')
                ))
                daily_inserted += cursor.rowcount
            
            print(f"Суточные нормы сохранены: {daily_inserted} записей")
        
        products = structured_data.get('products', [])
        if products:
            insert_product_sql = """
            insert into products (
                product_name, manufacturer, serving_size, 
                total_calories, calories_from_fat,
                fat_g, saturated_fat_g, cholesterol_mg, sodium_mg,
                carbohydrates_g, fiber_g, protein_g,
                vitamin_a_percent, vitamin_c_percent, 
                calcium_percent, iron_percent
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            
            products_inserted = 0
            for product in products:
                cursor.execute(insert_product_sql, (
                    product.get('product_name'),
                    product.get('manufacturer'),
                    product.get('serving_size'),
                    product.get('total_calories', 0),
                    product.get('calories_from_fat', 0),
                    product.get('fat_g', 0.0),
                    product.get('saturated_fat_g', 0.0),
                    product.get('cholesterol_mg', 0.0),
                    product.get('sodium_mg', 0.0),
                    product.get('carbohydrates_g', 0.0),
                    product.get('fiber_g', 0.0),
                    product.get('protein_g', 0.0),
                    product.get('vitamin_a_percent', 0),
                    product.get('vitamin_c_percent', 0),
                    product.get('calcium_percent', 0),
                    product.get('iron_percent', 0)
                ))
                products_inserted += 1
            
            print(f"Продукты сохранены: {products_inserted} записей")
        
        conn.commit()
        
        cursor.execute("select count(*) from products")
        total_products = cursor.fetchone()[0]
        cursor.execute("select count(*) from daily_nutrition")
        total_nutrients = cursor.fetchone()[0]
        
        print(f"Продуктов сохранено: {products_inserted}")
        print(f"Суточных норм сохранено: {total_nutrients}")
        
    finally:
        cursor.close()
        conn.close()

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
        python_callable=transform_xml
    )
    
    save_task = PythonOperator(
        task_id='save_xml',
        python_callable=save_xml
    )
    
    transform_task >> save_task