# mlops_assignment02

# Data Preprocessing Documentation

## Data Extraction
- Extracted articles from BBC and Dawn using BeautifulSoup and requests.
- Filtered out irrelevant links.

## Data Transformation
- Cleaned HTML tags, extra spaces, and standardized text formats.

## Data Loading
- Saved processed articles to a JSON file for further analysis.

# DVC Setup
1. Install DVC:
    ```bash
    pip install dvc
    ```
2. Initialize DVC in your repository:
    ```bash
    dvc init
    ```
3. Add remote storage (e.g., Google Drive):
    ```bash
    dvc remote add -d gdrive_remote gdrive://your_folder_id
    ```
4. Track data files with DVC:
    ```bash
    dvc add data/articles.json
    ```
5. Push data to the remote:
    ```bash
    dvc push
    ```

workflow summary:
# Workflow and Challenges

## Workflow
1. **Airflow DAG:** 
    - Implemented an ETL pipeline using Airflow to manage data extraction, transformation, and loading.

2. **DVC Integration:**
    - Leveraged DVC for data version control and reproducibility.
    - Setup Google Drive as remote storage.

## Challenges
- **Data Access Issues:** 
  - Encountered difficulties accessing certain websites due to inconsistent page structures.
  - Implemented error handling and retries.

- **Environment Setup:**
  - Faced compatibility issues running Airflow on Windows.
  - Resolved by using WSL2 for a POSIX-compliant environment.


how does it works:
# MLOps Project: ETL with Airflow and DVC

## Overview
- An ETL pipeline for data extraction, transformation, and loading, managed with Airflow and tracked via DVC.

## Setup Instructions
1. Clone the repository and install dependencies:
    ```bash
    git clone https://github.com/talhatariqbutt/mlops_assignment2.git
    cd mlops-assignment2
    pip install -r requirements.txt
    ```

2. Initialize Airflow:
    ```bash
    airflow db init
    ```

3. Run the web server and scheduler:
    ```bash
    airflow webserver -p 8080
    airflow scheduler
    ```

## Usage
- Trigger the `mlops_etl_dag` manually via the Airflow UI or let it run based on its schedule.

## Contact
- For questions or contributions, contact i202652@nu.edu.pk
