from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv('.env')

class Config:
    api_key = os.environ.get('API_KEY')

def get_reviews_from_api():
    url = 'https://api.nytimes.com/svc/movies/v2/reviews/search.json'
    api_key = Config.api_key  # Access the API key from the Config class

    # Set the query parameters including the API key and search query
    params = {
        'api-key': api_key,
        'offset': 0  # Initial offset    
    }

    all_reviews = []

    while True:
        # Make a request to the API
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])

            # Append the current page of results to the overall results list
            all_reviews.extend(results)
            
            # Check if there are more pages of results
            if len(results) < 20:  # Assuming the default value of 20 rows per page
                break
            
            # Increment the offset for the next request
            params['offset'] += 20
            
        else:
            print('Request failed with status code:', response.status_code)
            break

    # Specify the file path to save the CSV file
    file_path = '/home/airflow/data/raw_review_from_api.csv'
    # Save the DataFrame as a CSV file
    df.to_csv(file_path, index=False)

def adjust_format():
    df = pd.read_csv('/home/airflow/data/review_from_api.csv')
    df = df.drop('multimedia', axis=1)
    df['opening_date'] = df['opening_date'].fillna('Unknown')
    df['mpaa_rating'] = df['mpaa_rating'].replace('', 'Unknown')
    df.to_csv('/home/airflow/data/full_review.csv', index=False)

# Define the DAG
default_args = {
    'owner': 'thanatkat',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'api_data_extraction',
    default_args=default_args,
    description='Extract data from API',
    schedule_interval=timedelta(days=1)
)

# Define the tasks
t1 = PythonOperator(
    task_id='get_review_data',
    python_callable=get_reviews_from_api,
    dag=dag
)

t2 = PythonOperator(
    task_id='adjust_format_data',
    python_callable=adjust_format,
    dag=dag
)

# Set the task dependencies
t1 >> t2