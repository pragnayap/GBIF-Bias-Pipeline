# Databricks notebook source
# MAGIC %pip install "pymongo[srv]" dnspython
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Credentials stored in Databricks Secrets scope "gbif"
# Setup: databricks secrets create-scope gbif
#        databricks secrets put --scope gbif --key mongo_uri --string-value "..."
#        databricks secrets put --scope gbif --key atlas_public_key --string-value "..."
#        databricks secrets put --scope gbif --key atlas_private_key --string-value "..."
#        databricks secrets put --scope gbif --key atlas_project_id --string-value "..."

MONGO_URI         = dbutils.secrets.get(scope="gbif", key="mongo_uri")
ATLAS_PUBLIC_KEY  = dbutils.secrets.get(scope="gbif", key="atlas_public_key")
ATLAS_PRIVATE_KEY = dbutils.secrets.get(scope="gbif", key="atlas_private_key")
ATLAS_PROJECT_ID  = dbutils.secrets.get(scope="gbif", key="atlas_project_id")

# COMMAND ----------

import requests

def whitelist_current_ip():
    current_ip = requests.get("https://api.ipify.org", timeout=5).text.strip()
    print(f"Current Databricks IP: {current_ip}")

    url     = f"https://cloud.mongodb.com/api/atlas/v1.0/groups/{ATLAS_PROJECT_ID}/accessList"
    payload = [{"ipAddress": current_ip, "comment": "Databricks serverless auto"}]

    resp = requests.post(
        url,
        auth=requests.auth.HTTPDigestAuth(ATLAS_PUBLIC_KEY, ATLAS_PRIVATE_KEY),
        json=payload,
        timeout=15,
    )

    if resp.status_code in (200, 201):
        print(f"✓ IP {current_ip} added to Atlas whitelist")
    elif resp.status_code == 409:
        print(f"✓ IP {current_ip} already whitelisted")
    else:
        print(f"⚠ Atlas API response {resp.status_code}: {resp.text}")

whitelist_current_ip()

# COMMAND ----------

from pymongo import MongoClient
from pymongo.server_api import ServerApi

try:
    client = MongoClient(
        MONGO_URI,
        server_api=ServerApi("1"),
        serverSelectionTimeoutMS=15_000,
        tlsAllowInvalidCertificates=True,
        tlsAllowInvalidHostnames=True,
    )
    client.admin.command("ping")
    col   = client["gbif_birds"]["raw_occurrences"]
    count = col.count_documents({})
    print(f"✓ Connected — {count:,} records in collection")
    client.close()
except Exception as e:
    print(f"✗ Failed: {e}")

# COMMAND ----------

from pymongo import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd

client = MongoClient(
    MONGO_URI,
    server_api=ServerApi("1"),
    tlsAllowInvalidCertificates=True,
    serverSelectionTimeoutMS=15_000,
)

col   = client["gbif_birds"]["raw_occurrences"]
count = col.count_documents({})
print(f"Records in MongoDB: {count:,}")

records = list(col.find({}, {
    "_id": 0,
    "gbifID": 1, "species": 1, "genus": 1, "family": 1,
    "order": 1, "class": 1, "kingdom": 1,
    "decimalLatitude": 1, "decimalLongitude": 1,
    "countryCode": 1, "stateProvince": 1,
    "eventDate": 1, "year": 1, "month": 1, "day": 1,
    "recordedBy": 1, "institutionCode": 1,
    "basisOfRecord": 1, "occurrenceStatus": 1,
}))
client.close()
print(f"✓ Loaded {len(records):,} records into Python")

# COMMAND ----------

pdf = pd.DataFrame(records)

for f in ["decimalLatitude", "decimalLongitude"]:
    pdf[f] = pd.to_numeric(pdf[f], errors="coerce")
for f in ["year", "month", "day"]:
    pdf[f] = pd.to_numeric(pdf[f], errors="coerce")

pdf = pdf.where(pd.notnull(pdf), other=None)

print(f"✓ pandas DataFrame: {pdf.shape[0]:,} rows × {pdf.shape[1]} cols")
print(pdf[["species", "countryCode", "year", "decimalLatitude"]].head(3))

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS gbif")

df_bronze = spark.createDataFrame(pdf)

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gbif.bronze_occurrences")

print(f"✓ Bronze table written: {df_bronze.count():,} rows")
spark.table("gbif.bronze_occurrences").show(5)
