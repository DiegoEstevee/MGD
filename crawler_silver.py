import time
import boto3

AWS_REGION = "eu-south-2"

GROUP = "imat3a04"
DATABASE_NAME = "tradedata_silver"
CRAWLER_NAME = f"crawler_{GROUP}_silver"
TABLE_PREFIX = "silver_"

S3_BUCKET = "tradedata-linkusd"
S3_PREFIX = "silver/"

GLUE_ROLE_ARN = "arn:aws:iam::715841369216:role/Job-Role_MGD"

glue = boto3.client("glue", region_name=AWS_REGION)

def ensure_database(db_name: str):
    try:
        glue.get_database(Name=db_name)
        print(f"[OK] Database ya existe: {db_name}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": db_name})
        print(f"[CREATED] Database creada: {db_name}")

def upsert_crawler():
    s3_path = f"s3://{S3_BUCKET}/{S3_PREFIX}"

    crawler_def = {
        "Name": CRAWLER_NAME,
        "Role": GLUE_ROLE_ARN,
        "DatabaseName": DATABASE_NAME,
        "Targets": {"S3Targets": [{"Path": s3_path}]},
        "TablePrefix": TABLE_PREFIX,
        "SchemaChangePolicy": {
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "DEPRECATE_IN_DATABASE",
        },
        "RecrawlPolicy": {"RecrawlBehavior": "CRAWL_EVERYTHING"},
        "Configuration": """
        {
          "Version": 1.0,
          "CrawlerOutput": {
            "Partitions": { "AddOrUpdateBehavior": "InheritFromTable" }
          }
        }
        """.strip(),
    }

    try:
        glue.get_crawler(Name=CRAWLER_NAME)
        glue.update_crawler(**crawler_def)
        print(f"[UPDATED] Crawler actualizado: {CRAWLER_NAME}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_crawler(**crawler_def)
        print(f"[CREATED] Crawler creado: {CRAWLER_NAME}")

def start_and_wait():
    crawler = glue.get_crawler(Name=CRAWLER_NAME)["Crawler"]
    if crawler["State"] == "RUNNING":
        print(f"[INFO] Crawler ya está RUNNING: {CRAWLER_NAME}")
        return

    glue.start_crawler(Name=CRAWLER_NAME)
    print(f"[STARTED] Crawler arrancado: {CRAWLER_NAME}")

    while True:
        crawler = glue.get_crawler(Name=CRAWLER_NAME)["Crawler"]
        state = crawler["State"]
        last_crawl = crawler.get("LastCrawl", {})
        status = last_crawl.get("Status")

        print(f"[WAIT] State={state} LastCrawlStatus={status}")

        if state == "READY":
            if status == "SUCCEEDED":
                print("[DONE] Crawler terminó correctamente (SUCCEEDED)")
            else:
                print(f"[DONE] Crawler terminó con estado: {status}")
            break

        time.sleep(10)

def main():
    ensure_database(DATABASE_NAME)
    upsert_crawler()
    start_and_wait()

if __name__ == "__main__":
    main()