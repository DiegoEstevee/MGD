import os
import re
import boto3
import pandas as pd
from botocore.exceptions import ClientError

# ========= CONFIG =========
BASE_PATH = r"C:\Users\estev\Documents\tercero\0_segundo_cuatri\big_data\Sprint\MGD\output_to_upload\crypto=chainlink\exchange=binance\dataset=nifty_data"
BUCKET_NAME = "Tradedata-linkusd"
OBJECT_NAME = "data.csv"
AWS_REGION = "eu-south-2"
# =========================


def sanitize_bucket_name(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^a-z0-9-]", "-", name)
    name = re.sub(r"-{2,}", "-", name).strip("-")
    return name[:63]


BUCKET_NAME = sanitize_bucket_name(BUCKET_NAME)


session = boto3.session.Session(region_name=AWS_REGION)
s3 = session.client("s3", region_name=AWS_REGION)


try:
    s3.head_bucket(Bucket=BUCKET_NAME)
    print(f"Bucket ya existe: {BUCKET_NAME}")
except ClientError:
    s3.create_bucket(
        Bucket=BUCKET_NAME,
        CreateBucketConfiguration={"LocationConstraint": AWS_REGION}
    )
    print(f"Bucket creado: {BUCKET_NAME}")


for year_dir in sorted(os.listdir(BASE_PATH)):
    if not year_dir.startswith("year="):
        continue

    year_path = os.path.join(BASE_PATH, year_dir)
    yearly_dfs = []

    for month_dir in sorted(os.listdir(year_path)):
        if not month_dir.startswith("month="):
            continue

        csv_path = os.path.join(year_path, month_dir, OBJECT_NAME)
        if not os.path.isfile(csv_path):
            print(f"No existe {csv_path}, se salta")
            continue

        df = pd.read_csv(csv_path)
        yearly_dfs.append(df)

    if not yearly_dfs:
        print(f"No hay datos para {year_dir}, se salta")
        continue

 
    year_df = pd.concat(yearly_dfs, ignore_index=True)

 
    tmp_csv = f"/tmp/{year_dir}.csv" if os.name != "nt" else f"{year_dir}.csv"
    year_df.to_csv(tmp_csv, index=False)

    key = f"{year_dir}/{OBJECT_NAME}"
    s3.upload_file(tmp_csv, BUCKET_NAME, key)

    print(f" Subido: s3://{BUCKET_NAME}/{key}")

    os.remove(tmp_csv)
