from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import boto3
import os
# ==========================================
# ⚙️ CONFIGURATIONS 
# ==========================================

API_KEY = os.getenv('WEATHER_API_KEY')
CITIES = ['Phrae', 'Nakhon Ratchasima', 'Bangkok', 'Chiang Mai', 'Phuket']

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')


def extract_and_load_to_minio(**kwargs):
    # สร้างตัวเชื่อมต่อกับ MinIO โดยใช้ไลบรารี boto3 (มาตรฐานเดียวกับ AWS S3)
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    # ดึงวันที่ที่ Airflow กำลังรันอยู่
    execution_date = kwargs['ds'] 
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for city in CITIES:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            data['ingestion_date'] = execution_date

            # ตั้งชื่อไฟล์แบบมีโครงสร้าง เช่น raw/2026-08-08/Phrae_20260808_164500.json
            file_name = f"raw/{execution_date}/{city}_{timestamp}.json"
            json_data = json.dumps(data).encode('utf-8')

            # โยนไฟล์ขึ้นโกดัง MinIO
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=file_name,
                Body=json_data,
                ContentType='application/json'
            )
            print(f"✅ สำเร็จ: อัปโหลดข้อมูลของเมือง {city} ลง MinIO ไฟล์ {file_name}")
        else:
            print(f"❌ ล้มเหลว: ไม่สามารถดึงข้อมูลของเมือง {city} ได้ (Error: {response.text})")

# ==========================================
# 📅 ตั้งค่ารอบการรันของ DAG
# ==========================================
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 1), # เริ่มต้นรันย้อนหลังตั้งแต่ต้นเดือน
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_api_to_minio_pipeline',
    default_args=default_args,
    description='ดึงข้อมูลสภาพอากาศและเก็บลง MinIO (Bronze Layer)',
    schedule_interval='@daily', # สั่งรันวันละ 1 ครั้ง
    catchup=False, # ปิดการรันย้อนหลังรัวๆ (รันแค่วันปัจจุบันพอ)
    tags=['ingestion', 'weather'],
) as dag:

    # Task 1: ดึงข้อมูล API
    fetch_weather_task = PythonOperator(
        task_id='fetch_and_upload_weather',
        python_callable=extract_and_load_to_minio,
        provide_context=True
    )

    # Task 2: รัน dbt (ย่อหน้าเข้ามาให้อยู่ใน Block เดียวกัน)
    run_dbt_task = BashOperator(
        task_id='run_dbt_models',
        # เพิ่ม --profiles-dir . เพื่อให้ dbt หาไฟล์ตั้งค่าเจอใน Docker
        bash_command='cd /opt/airflow/dbt_weather && dbt run --profiles-dir .', 
    )

    # ผูกให้ Task ทำงานต่อกัน (ย่อหน้าเข้ามาเช่นกัน)
    fetch_weather_task >> run_dbt_task