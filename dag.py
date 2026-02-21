import requests
import pandas
import pendulum 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
def get_load_raw_from_api(**context):
    api_key = "c9b00a099ae240efbad203747261902"
    city = 'Moscow'
    date = context['ds']
    url = f'http://api.weatherapi.com/v1/history.json?key={api_key}&q={city}&date={date}'
    response = requests.get(url=url)
    d = response.json()
    df2 = pandas.DataFrame([d['forecast']['forecastday'][0]['day']])
    filename = f'api/raw/from_api_weather_in_{city}_on_{date}.csv'
    if len(df2) > 0:
        hook = S3Hook(aws_conn_id='minio')
        hook.load_string(string_data=df2.to_csv(sep=';', index=False), key=filename, bucket_name="example")

        total = len(df2)
        print(f'======= Данные загружены за : {date}')
        print(f'======= кол-во строк: {total}')
    else:
        print('Ошибка!!! Данных нет')



default_args = {
    'owner': 'chelicks',
    'start_date': '2023-01-01',
    'retries': 2,

}

dag = DAG(
    dag_id='load_from_api_to_s3_weather',
    default_args=default_args,
    schedule='@monthly',
    catchup=True,
    description='API to S3 weather and astro',
    tags=['weather', 'astro', 's3', 'api', 'json']
)


upload_to_s3 = PythonOperator(
    task_id='oad_from_api_to_s3_weather',
    callable=get_load_raw_from_api,
    dag=dag
)

upload_to_s3
