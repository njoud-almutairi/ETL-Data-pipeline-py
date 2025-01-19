from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import boto3 
import json
import pandas as pd
from io import StringIO

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # DAG tasks do not depend on past runs
    'start_date': datetime(2025, 1, 17),  # Start date for the DAG
    'email': [],  # Email notifications (empty list means none)
    'email_on_failure': False,  # Disable email on failure
    'email_on_retry': False,  # Disable email on retry
    'retries': 2,  # Number of retry attempts
    'retry_delay': timedelta(minutes=2)  # Delay between retries
}

# Function to extract data from an S3 bucket
def extract():
    s3_client = boto3.client(
        's3', 
        region_name="us-east-1", 
        aws_access_key_id='your access key id',
        aws_secret_access_key='your secret key'
    )
    # Fetch the object from S3
    response = s3_client.get_object(
        Bucket='eventtdata', 
        Key='events_data.json',
    )['Body']
    # Load the data as JSON
    data = json.load(response)
    print(data)  # Print data for debugging
    return json.dumps(data)  # Return the data as a JSON string

# Function to transform the extracted data
def transformation(data):
    if isinstance(data, str):
        data = json.loads(data)  # Parse JSON string back to a dictionary

    # Extract and transform the events data
    events = data['contents']['events']
    transformed_data = []
    for event in events:
        start_time = event.get("event_date", {}).get("start_time")
        end_time = event.get("event_date", {}).get("end_time")
        time_range = f"{start_time} - {end_time}" if start_time and end_time else None

        # Append the transformed data
        transformed_data.append({
            "أسم الفعالية": event.get("title"),  # Event title
            "أسم المالك": event.get("ownername"),  # Owner name
            "الرابط": event.get("link"),  # Link
            "اللغة": event.get("lang"),  # Language
            "اسم المدينة": event.get("city", {}).get("name") if event.get("city") else None,  # City name
            "تاريخ البداية": event.get("event_date", {}).get("start_date"),  # Start date
            "وقت المناسبة": time_range,  # Time range
            "وقت البداية": event.get("event_date", {}).get("start_time"),  # Start time
            "تاريخ النهاية": event.get("event_date", {}).get("end_date"),  # End date
            "وقت النهاية": event.get("event_date", {}).get("end_time"),  # End time
            "الفئة العمرية": event.get("age_group", {}).get("name") if event.get("age_group") else None,  # Age group
            "فترة الفعالية": event.get("event_period", {}).get("name") if event.get("event_period") else None,  # Event period
            "فئة الحضور": event.get("attendance_type", {}).get("name") if event.get("attendance_type") else None,  # Attendance type
            "نوع الدخول": event.get("event_price", {}).get("name") if event.get("event_price") else None,  # Entry type
            "الموقع": event.get("location"),  # Location
            "منظم الفعالية": event.get("event_organizer"),  # Organizer
            "نوع الفعالية": event.get("type_of_event", {}).get("name") if event.get("type_of_event") else None,  # Event type
            "فعالية خاصة": event.get("event_special"),  # Special event
        })

    # Convert transformed data to a DataFrame
    df_transformed = pd.DataFrame(transformed_data)

    # Remove rows and columns with all NaN values
    df_transformed.dropna(how='all', inplace=True)
    df_transformed.dropna(how='all', axis=1, inplace=True)

    print("Transformed Data:", transformed_data)  # Debugging print
    return json.dumps(df_transformed.to_dict(orient="records"))  # Return as JSON string

# Function to load transformed data back to S3
def load_to_s3(data):
    if not data:
        raise ValueError("No data received for S3 upload.")

    try:
        # Validate and parse the JSON data
        records = json.loads(data)
        if not records:
            raise ValueError("Data is empty after parsing JSON.")

        # Convert data to CSV using Pandas
        df = pd.DataFrame(records)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Create S3 client
        s3_client = boto3.client(
            's3',
            region_name="us-east-1",
            aws_access_key_id='your access key id--',
            aws_secret_access_key='your secret key--'
        )

        # Generate a unique file name and upload to S3
        now = datetime.now().strftime("%Y%m%d%H%M%S")
        file_name = f"transformed_events_{now}.csv"
        s3_client.put_object(
            Bucket='eventtdata',
            Key=f'transformed/{file_name}',
            Body=csv_buffer.getvalue()
        )
        print(f"Transformed data saved to S3 as transformed/{file_name}")
    except Exception as e:
        print(f"Error during S3 upload: {e}")
        raise

# Define the Airflow DAG
with DAG(
    'event_dag',  # DAG ID
    default_args=default_args,  # Default arguments
    schedule_interval='@daily',  # Run the DAG daily
    catchup=False  # Do not backfill for missed runs
) as dag:

    # Task to extract data from S3
    task1 = PythonOperator(
        task_id="Extract",
        python_callable=extract
    )

    # Task to transform the extracted data
    task2 = PythonOperator(
        task_id="Transformation",
        python_callable=transformation,
        op_kwargs={
            "data": "{{ ti.xcom_pull(task_ids='Extract') }}"  # Pass output from task1
        },
    )

    # Task to load transformed data back to S3
    task3 = PythonOperator(
        task_id="Load",
        python_callable=load_to_s3,
        op_kwargs={
            "data": "{{ ti.xcom_pull(task_ids='Transformation') }}"  # Pass output from task2
        }
    )

    # Define task dependencies
    task1 >> task2 >> task3
