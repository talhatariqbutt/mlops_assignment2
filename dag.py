import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from urllib.parse import urljoin
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer

# Source websites to scrape
sources = {
    'dawn': 'https://www.dawn.com/',
    'bbc': 'https://www.bbc.com/'
}

# Data extraction task
def extract(sources, max_articles_per_site=300):
    data = []
    for source_url in sources:
        print("Extracting data from:", source_url)
        reqs = requests.get(source_url)
        soup = BeautifulSoup(reqs.text, 'html.parser')
        links = [link.get('href') for link in soup.find_all('a')]
        article_count = 0
        for link in links:
            abs_link = urljoin(source_url, link)  # Convert to absolute URL
            reqs = requests.get(abs_link)
            soup = BeautifulSoup(reqs.text, 'html.parser')
            articles = soup.find_all('article')
            for article in articles:
                if article_count >= max_articles_per_site:
                    break
                title = article.find('h3').text.strip() if article.find('h3') else None
                description = article.find('p').text.strip() if article.find('p') else None
                data.append({'source': source_url, 'title': title, 'description': description})
                article_count += 1
                print("Articles extracted:", article_count)
            if article_count >= max_articles_per_site:
                break
    df = pd.DataFrame(data)
    return df

# Data transformation task
def transform(df):
    def clean_text(text):
        if isinstance(text, str):
            # Remove special characters and digits
            text = re.sub(r'[^a-zA-Z\s]', '', text)
            # Convert text to lowercase
            text = text.lower()
            return text
        else:
            return ''

    def remove_stopwords(text):
        stop_words = set(stopwords.words('english'))
        word_tokens = word_tokenize(text)
        filtered_text = [word for word in word_tokens if word not in stop_words]
        return ' '.join(filtered_text)

    def lemmatize_text(text):
        lemmatizer = WordNetLemmatizer()
        word_tokens = word_tokenize(text)
        lemmatized_text = [lemmatizer.lemmatize(word) for word in word_tokens]
        return ' '.join(lemmatized_text)

    df['description'] = df['description'].apply(clean_text)
    df['description'] = df['description'].apply(remove_stopwords)
    df['description'] = df['description'].apply(lemmatize_text)
    return df

# Data loading task
def load(df, file_path, client_secrets_path, drive_folder_id):
    # Save preprocessed data to CSV
    preprocessed_csv_path = file_path + '.csv'
    df.to_csv(preprocessed_csv_path, index=False)
    
    # Save preprocessed data to JSON
    preprocessed_json_path = file_path + '.json'
    preprocessed_json = df.to_json(orient="records", indent=4)
    with open(preprocessed_json_path, "w") as json_file:
        json_file.write(preprocessed_json)
    
    # Authenticate with Google Drive
    gauth = GoogleAuth()
    gauth.LoadClientConfigFile(client_secrets_path)
    gauth.LocalWebserverAuth()
    
    # Initialize Google Drive client
    drive = GoogleDrive(gauth)
    
    # Upload preprocessed data CSV to Google Drive
    upload_file_to_drive(drive, preprocessed_csv_path, drive_folder_id)
    
    # Upload preprocessed data JSON to Google Drive
    upload_file_to_drive(drive, preprocessed_json_path, drive_folder_id)

def upload_file_to_drive(drive, file_path, drive_folder_id):
    file = drive.CreateFile({'parents': [{'id': drive_folder_id}]})
    file.SetContentFile(file_path)
    file.Upload()

default_args = {
    'owner': 'airflow-demo',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 5, 8),
}

dag = DAG(
    'mlops-dag',
    default_args=default_args,
    description='A simple ETL DAG',
    schedule_interval='@daily',
    catchup=False
)

# Define Python operators for the DAG tasks
task1 = PythonOperator(
    task_id="extract_data",
    python_callable=extract,
    op_kwargs={'sources': list(sources.values())},
    dag=dag
)

task2 = PythonOperator(
    task_id="transform_data",
    python_callable=transform,
    provide_context=True,
    dag=dag
)

task3 = PythonOperator(
    task_id="load_data",
    python_callable=load,
    op_args=[],
    op_kwargs={
        'file_path': r"C:\Users\ttsae\OneDrive\Desktop\mlops_assignment2\preprocessed_data.json",
        'client_secrets_path': r"C:\Users\ttsae\OneDrive\Desktop\mlops_assignment2\client_secret.json",
        'drive_folder_id': '1yPMylDg_HKlzKtVRUZlLsSe20NbBkh7u'
    },
    provide_context=True,
    dag=dag
)

# Set task dependencies
task1 >> task2 >> task3
