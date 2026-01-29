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

# --------- crear bucket ----------
try:
    s3.head_bucket(Bucket=BUCKET_NAME)
    print(f"Bucket ya existe: {BUCKET_NAME}")
except ClientError:
    s3.create_bucket(
        Bucket=BUCKET_NAME,
        CreateBucketConfiguration={"LocationConstraint": AWS_REGION}
    )
    print(f"Bucket creado: {BUCKET_NAME}")

# --------- por AÑO: leer TODOS los meses disponibles ----------
for year_dir in sorted(os.listdir(BASE_PATH)):
    if not year_dir.startswith("year="):
        continue

    year_path = os.path.join(BASE_PATH, year_dir)
    monthly_files = []

    for month_dir in sorted(os.listdir(year_path)):
        if month_dir.startswith("month="):
            csv_path = os.path.join(year_path, month_dir, OBJECT_NAME)
            if os.path.isfile(csv_path):
                monthly_files.append(csv_path)

    if not monthly_files:
        print(f" {year_dir}: no hay meses con {OBJECT_NAME}")
        continue

    # Leer y concatenar
    dfs = [pd.read_csv(f) for f in monthly_files]
    year_df = pd.concat(dfs, ignore_index=True)

    # (Opcional pero recomendado) ordenar y quitar duplicados
    if "datetime" in year_df.columns:
        year_df["datetime"] = pd.to_datetime(year_df["datetime"], errors="coerce")
        year_df = year_df.dropna(subset=["datetime"]).sort_values("datetime")
        year_df = year_df.drop_duplicates(subset=["datetime"], keep="first")

    # Guardar CSV temporal (en la carpeta actual)
    tmp_csv = f"{year_dir}.csv"
    year_df.to_csv(tmp_csv, index=False)

    # Subir a S3 como un CSV anual
    key = f"{year_dir}/{OBJECT_NAME}"
    s3.upload_file(tmp_csv, BUCKET_NAME, key)
    print(f"{year_dir}: subido {len(year_df)} filas -> s3://{BUCKET_NAME}/{key}")

    os.remove(tmp_csv)
