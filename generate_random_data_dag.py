from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime


import boto3
import awswrangler as wr
import pandas as pd
from airflow.models import Variable

import random
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker

# Seed for reproducibility
random.seed(77)
fake = Faker()
fake.seed_instance(0)

# Product catalog
products = {
    "Fruits": {"Apple": 1.5, "Orange": 1.2,  "Mango": 1.9},
    "Meat": {"Beef": 5.0, "Chicken": 3.0, "Pork": 4.0, "Fish": 6.0}
}

def generate_fake_user():
    return fake.name(), fake.uuid4()

def generate_fake_transaction():
    transaction_id = fake.uuid4()
    category = random.choice(list(products.keys()))
    product = random.choice(list(products[category].keys()))
    price = products[category][product]
    volume_purchased = random.randint(1, 7)
    total_price = price * volume_purchased
    return transaction_id, product, category, price, volume_purchased, total_price

def generate_exact_rows(target_rows=100):
    data = []
    while len(data) < target_rows:
        user_name, user_id = generate_fake_user()
        transaction_date = fake.date_time_this_month()
        transaction_id = fake.uuid4()
        
        row = {
            'User Name': user_name,
            'User ID': user_id,
            'Transaction Date': transaction_date,
            'Transaction ID': transaction_id
        }
        
        # Add one row at a time to control count exactly
        product, category, price, volume, total = generate_fake_transaction()[1:]
        row.update({
            'Category': category,
            'Product': product,
            'Price': price,
            'Volume Purchased': volume,
            'Total Price': total
        })
        data.append(row)
    
    return pd.DataFrame(data)




with DAG(dag_id="random_data_dag", 
        start_date=datetime(2025,8,1),
        schedule="@daily",
        catchup=False) as dag:
    
    
    extract_random_data = PythonOperator(
        task_id="extract_random_data",
        python_callable= generate_exact_rows
    )