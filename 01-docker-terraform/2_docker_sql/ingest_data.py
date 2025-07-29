import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq
from time import time
import yaml
import os
import requests
import gzip
from io import BytesIO

def load_config(path):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def insert_parquet_to_postgres(engine, file_path, table_name, batch_size=100_000):
    reader = pq.ParquetFile(file_path)
    for batch in reader.iter_batches(batch_size=batch_size):
        df = batch.to_pandas()
        t_start = time()
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        t_end = time()
        print(f"✅ Inserted Parquet batch into {table_name} in {t_end - t_start:.2f} seconds.")

def insert_parquet_gz_to_postgres(engine, file_path, table_name, batch_size=100_000):
    with gzip.open(file_path, 'rb') as f:
        data = f.read()
    reader = pq.ParquetFile(BytesIO(data))
    for batch in reader.iter_batches(batch_size=batch_size):
        df = batch.to_pandas()
        t_start = time()
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        t_end = time()
        print(f"✅ Inserted Parquet GZ batch into {table_name} in {t_end - t_start:.2f} seconds.")

def insert_csv_to_postgres(engine, file_path, table_name, batch_size=100_000, compression=None):
    chunks = pd.read_csv(file_path, chunksize=batch_size, compression=compression)
    for chunk in chunks:
        t_start = time()
        chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        t_end = time()
        print(f"✅ Inserted CSV chunk into {table_name} in {t_end - t_start:.2f} seconds.")

def download_file(url, target_path):
    if os.path.exists(target_path):
        print(f"📦 File already exists: {target_path}, skipping download.")
        return
    print(f"⬇️  Downloading {url}")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(target_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"✅ Downloaded to {target_path}")

def insert_file(engine, file_path, table_name, batch_size):
    _, ext = os.path.splitext(file_path)

    if ext == '.parquet':
        insert_parquet_to_postgres(engine, file_path, table_name, batch_size)
    elif ext == '.csv':
        insert_csv_to_postgres(engine, file_path, table_name, batch_size)
    elif ext == '.parquet.gz':
        insert_parquet_gz_to_postgres(engine, file_path, table_name, batch_size)
    elif ext == '.gz' and file_path.endswith('.csv.gz'):
        insert_csv_to_postgres(engine, file_path, table_name, batch_size, compression='gzip')
    else:
        print(f"⚠️ Unsupported file type: {file_path}")
        return
    # Clean up
    os.remove(file_path)
    print(f"🧹 Removed {file_path}")

def main():
    config = load_config("config.yaml")

    user = config['postgres']['user']
    password = config['postgres']['password']
    host = config['postgres']['host']
    port = config['postgres']['port']
    db = config['postgres']['db']
    batch_size = config.get('batch_size', 100_000)
    source_file = config['source_file']

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    sources = pd.read_csv(source_file)

    for _, row in sources.iterrows():
        url = row['url']
        table_name = row['table']
        file_name = url.split('/')[-1]
        file_path = os.path.join(os.getcwd(), file_name)

        print(f"🚀 Processing: {file_name} → {table_name}")
        download_file(url, file_path)
        insert_file(engine, file_path, table_name, batch_size)

if __name__ == "__main__":
    main()