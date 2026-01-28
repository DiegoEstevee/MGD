import os
import re
import boto3
from botocore.exceptions import ClientError

# ========= CONFIG PROYECTO =========
BASE_PATH = "output_to_upload/crypto=chainlink/exchange=binance/dataset=nifty_data"
BUCKET_PREFIX = "tradedata-linkusd"  
OBJECT_NAME = "data.csv"
# ===================================

def sanitize_bucket_name(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^a-z0-9-]", "-", name)
    name = re.sub(r"-{2,}", "-", name).strip("-")
    return name[:63]

session = boto3.session.Session()
s3 = session.client("s3")

def create_bucket(bucket_name: str) -> None:
    """Crea bucket en la región actual (ojo: puede estar bloqueado por SCP)."""
    try:
        s3.head_bucket(Bucket=bucket_name)
        return  
    except ClientError:
        pass

    
    s3.create_bucket(Bucket=bucket_name)
    

def upload_csv(bucket_name: str, csv_path: str) -> None:
    s3.upload_file(Filename=csv_path, Bucket=bucket_name, Key=OBJECT_NAME)


for year_dir in sorted(os.listdir(BASE_PATH)):
    if not year_dir.startswith("year="):
        continue
    year = year_dir.split("=")[1]
    year_path = os.path.join(BASE_PATH, year_dir)

    for month_dir in sorted(os.listdir(year_path)):
        if not month_dir.startswith("month="):
            continue
        month = month_dir.split("=")[1]

        csv_path = os.path.join(year_path, month_dir, OBJECT_NAME)
        if not os.path.isfile(csv_path):
            print(f" No existe {csv_path}, salto")
            continue

        yyyymm = f"{year}{month}"
        bucket_name = sanitize_bucket_name(f"{BUCKET_PREFIX}-{yyyymm}")

        try:
            create_bucket(bucket_name)
            upload_csv(bucket_name, csv_path)
            print(f"OK: s3://{bucket_name}/{OBJECT_NAME}")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "Unknown")
            msg = e.response.get("Error", {}).get("Message", str(e))
            print(f" Falló {bucket_name}: {code} - {msg}")
            raise
