from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os
import pymongo
from pymongo import MongoClient, errors
import json
import re
import logging
import mysql.connector
from sqlalchemy import create_engine
from clickhouse_driver import Client
from datetime import datetime
import requests
import pymysql
from clickhouse_driver.errors import Error as ClickHouseError


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

dag = DAG(
    'etl',
    default_args=default_args,
    description='total DAG technical test GTech Digital Asia',
    schedule_interval='@once',
    catchup=False
)

def read_and_process_data(file_path):
    response = requests.get(file_path)
    if response.status_code == 200:
        data = response.text

        # Mengganti tanda kutip tunggal dengan tanda kutip ganda di key dan value
        fixed_data = re.sub(r"(\s*)'([^']*?)'(\s*:\s*|\s*,\s*)", r'\1"\2"\3', data)
        fixed_data = re.sub(r"(\s*:\s*)'([^']*?)'(\s*,\s*|\s*})", r'\1"\2"\3', fixed_data)

        # Mengganti ISODate dengan string tanggal yang sesuai
        fixed_data = re.sub(r"ISODate\((\".*?\")\)", r"\1", fixed_data)

        # Menghapus koma tambahan di akhir objek atau array
        fixed_data = re.sub(r',(\s*[}\]])', r'\1', fixed_data)

    logging.info("Fixed Data:", fixed_data)

    try:

        data_dict = json.loads(fixed_data)
        logging.info("JSON berhasil dimuat!")
        df = pd.DataFrame(data_dict)
        df_json_bersih = pd.json_normalize(df['data']).explode('loyaltyCardIds').reset_index(drop=True)
        date_columns = ['createdAt', 'modifiedAt', 'birthDate', 'enrollmentDate']
        for col in date_columns:
            df_json_bersih[col] = pd.to_datetime(df_json_bersih[col], format='%Y-%m-%dT%H:%M:%S.%fZ', errors='coerce')

        #menghapus kolom _id agar bisa dimasukkan ke MongoDB
        df_json_bersih.drop(columns=['_id'], inplace=True)

        # Menghilangkan format +62- dari kolom 'phone' untuk proses selanjutnya
        df_json_bersih['phone'] = df_json_bersih['phone'].str.replace(r'^\+62-', '', regex=True)
        return df_json_bersih

    except json.JSONDecodeError as e:
        logging.error(f"Error loading JSON: {e}")
        return None

def kirim_dataframe_ke_mongodb(db_name="tech_test", collection_name="data", uri="mongodb://mongodb_bigdata:27017/"):
    try:
        client = MongoClient(uri)
        db = client[db_name]
        collection = db[collection_name]

        dataframe = read_and_process_data('https://raw.githubusercontent.com/fachriomee/dataset/main/Use.Case.Sample.Data.json')
        data_dict = dataframe.to_dict("records")
        
        # filter duplikasi insert di mongo
        for record in data_dict:
            if collection.count_documents({"loyaltyCardIds": record["loyaltyCardIds"]}, limit=1) == 0:
                collection.insert_one(record)
                logging.info(f"Data {record['loyaltyCardIds']} berhasil dimasukkan.")
            else:
                logging.info(f"Data {record['loyaltyCardIds']} sudah ada di MongoDB, dilewati.")
        
    except errors.ConnectionFailure as e:
        logging.error(f"Gagal terhubung ke MongoDB: {e}")
    except errors.PyMongoError as e:
        logging.error(f"Terjadi kesalahan saat memproses data di MongoDB: {e}")
    except Exception as e:
        logging.error(f"Kesalahan umum terjadi: {e}")

load_to_mongo = PythonOperator(
    task_id='load_to_mongo',
    python_callable=kirim_dataframe_ke_mongodb,
    provide_context=True,
    dag=dag,
)

def pandas_data_type_to_sql(data_type):
    dtype_mapping = {
        'int64': 'BIGINT',
        'float64': 'FLOAT',
        'object': 'VARCHAR(255)',  # Anda mungkin perlu menyesuaikan panjang VARCHAR sesuai kebutuhan
        'datetime64[ns]': 'DATETIME',
        'bool': 'BOOLEAN',
    }
    return dtype_mapping.get(str(data_type), 'VARCHAR(255)')

def create_database_and_insert_dataframe(host, port, user, password, existing_database, new_database_name, table_name, dataframe):
    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=existing_database
    )
    cursor = conn.cursor()
    
    try:
        create_database_query = f"CREATE DATABASE IF NOT EXISTS {new_database_name};"
        cursor.execute(create_database_query)
        logging.info(f"Database '{new_database_name}' created successfully.")
    except mysql.connector.Error as e:
        logging.info(f"Error creating database: {e}")
    finally:
        cursor.close()
        conn.close()

    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=new_database_name
    )
    cursor = conn.cursor()
    
    try:
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        for col in dataframe.columns:
            data_type = dataframe[col].dtype
            mysql_data_type = pandas_data_type_to_sql(data_type.name)
            create_table_query += f"`{col}` {mysql_data_type}, "
        create_table_query = create_table_query.rstrip(', ') + ');'
        cursor.execute(create_table_query)
        logging.info(f"Table '{table_name}' created successfully.")
        conn.commit()
        
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{new_database_name}')
        
        dataframe.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Data inserted into '{table_name}' successfully.")
        
    except mysql.connector.Error as e:
        logging.info(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

def etl_mysql():
    data = pd.read_excel('https://raw.githubusercontent.com/fachriomee/dataset/main/Use.Case.Sample.Data.xlsx')
    
    etl = create_database_and_insert_dataframe(
        host='docker-mysql-1',
        port=3306,
        user='user',
        password='password',
        existing_database='mysql',  
        new_database_name='tech_test',
        table_name='data',
        dataframe=data  
    return etl

load_to_mysql = PythonOperator(
    task_id='load_to_mysql',
    python_callable=etl_mysql,
    provide_context=True,
    dag=dag,
)

mongo_client = MongoClient('mongodb://mongodb_bigdata:27017/')
mongo_db = mongo_client['tech_test']
mongo_collection = mongo_db['data']
clickhouse_client = Client(host='clickhouse_server', port='9000')

def get_data_from_mongo(**kwargs):
    mongo_data = list(mongo_collection.find({}, {'_id': 0}))  # Eksklusikan _id
    df_mongo = pd.DataFrame(mongo_data)
    kwargs['ti'].xcom_push(key='mongo_data', value=df_mongo)

def get_data_from_mysql(**kwargs):
    mysql_conn_params = {
        'host': 'docker-mysql-1',
        'port': 3306,
        'user': 'user',
        'password': 'password',
        'database': 'tech_test'
    }

    conn = pymysql.connect(**mysql_conn_params)

    query = "SELECT * FROM tech_test.data"
    df_mysql = pd.read_sql(query, conn)
    conn.close()
    kwargs['ti'].xcom_push(key='mysql_data', value=df_mysql)

def send_to_clickhouse(**kwargs):
    df_mongo = kwargs['ti'].xcom_pull(key='mongo_data')
    df_mysql = kwargs['ti'].xcom_pull(key='mysql_data')
    
    df_mongo = pd.DataFrame(df_mongo)
    df_mysql = pd.DataFrame(df_mysql)
    df_mongo = df_mongo.astype(str)
    df_mysql = df_mysql.astype(str)

    df_mongo = df_mongo.rename(columns={'phone': 'mobile', 'birthDate':'date_of_birth', 'createdAt':'create_time', 'modifiedAt':'update_time'})

    # Full outer join
    df_joined = pd.merge(df_mongo, df_mysql, how='outer', on = ['gender', 'mobile', 'date_of_birth', 'create_time', 'update_time', 'city', 'country'])
    df_joined = df_joined.astype(str)

    # Koneksi ke ClickHouse
    clickhouse_client = Client(host='clickhouse_server', port='9000')

    buat_tabel_clickhouse = """
    CREATE TABLE IF NOT EXISTS tech_test.data
    (
    loyaltyCardIds String,
    create_time String,
    update_time String,
    sourceReference String,
    mobile String,
    gender String,
    date_of_birth String,
    enrollmentDate String,
    city String,
    country String,
    id String,
    blood_type String,
    province String,
    email String
    ) ENGINE = MergeTree()
    ORDER BY date_of_birth;
    """

    try:
        clickhouse_client.execute(buat_tabel_clickhouse)
        logging.info("buat_tabel_clickhouse Data loaded into ClickHouse successfully")
    except ClickHouseError as e:
        logging.error(f"buat_tabel_clickhouse error occurred: {e}")
        # Handle specific ClickHouse errors here if needed
    except Exception as e:
        logging.error(f"buat_tabel_clickhouse General error occurred: {e}")
        # Handle other exceptions
    finally:
        # Clean-up or close connections if necessary
        logging.info("Finalizing database operation")


    # Fungsi untuk cek duplikasi dan insert data ke ClickHouse
    def cek_duplikasi_dan_insert_data(clickhouse_client, table_name, data_dict):
        id_value = data_dict['date_of_birth']
        query = f"SELECT COUNT(*) FROM {table_name} WHERE date_of_birth = '{id_value}'"
        result = clickhouse_client.execute(query)
        if result[0][0] > 0:  # Mengakses elemen pertama dari tuple pertama
            return {'message': f"Data dengan date_of_birth {id_value} sudah ada, tidak melakukan insert."}
        
        # Untuk insert data jika tidak terdapat duplikasi
        columns = ', '.join(data_dict.keys())
        formatted_values = [f"'{str(v)}'" for v in data_dict.values()]
        values = ', '.join(formatted_values)
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        
        # Melakukan insert
        clickhouse_client.execute(insert_query)
        return {'message': f"Data berhasil dimasukkan ke tabel {table_name}."}
    
    try:
        df_joined = df_joined.to_dict('records')
        for record in df_joined:
            result = cek_duplikasi_dan_insert_data(clickhouse_client, 'tech_test.data', record)
            logging.info(result['message'])
    except Exception as e:
        logging.error(f"Error occurred: {e}")
    finally:
        logging.info("Finalizing database operation")

get_mongo_data_task = PythonOperator(
    task_id='get_mongo_data',
    python_callable=get_data_from_mongo,
    provide_context=True
)

get_mysql_data_task = PythonOperator(
    task_id='get_mysql_data',
    python_callable=get_data_from_mysql,
    provide_context=True
)

join_and_send_to_clickhouse_task = PythonOperator(
    task_id='join_and_send_to_clickhouse',
    python_callable=send_to_clickhouse,
    provide_context=True
)

load_to_mongo >> get_mongo_data_task >> join_and_send_to_clickhouse_task
load_to_mysql >> get_mysql_data_task >> join_and_send_to_clickhouse_task

