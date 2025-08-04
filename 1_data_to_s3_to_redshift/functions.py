import boto3
import awswrangler as wr
import pandas as pd
from airflow.models import Variable
import random
from faker import Faker
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()


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

def generate_exact_rows(target_rows=500005):
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
    
    return data

records = generate_exact_rows()
df = pd.DataFrame(records)

session = boto3.Session(aws_access_key_id=os.getenv('aws_access_key'),
                        aws_secret_access_key=os.getenv('aws_secret_key'),
                        region_name=os.getenv('region'))
def upload_to_aws_bucket():
    """
    Upload the generated DataFrame to S3 in Parquet format.
    """
    wr.s3.to_parquet(
        df=df,
        path="s3://wofais-aws-bucket/transactional_data/transactional_data.parquet",
        boto3_session=session,
        mode="overwrite",
        dataset=True
    )

upload_to_aws_bucket()