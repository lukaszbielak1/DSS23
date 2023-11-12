# Databricks notebook source
from pyspark.dbutils import DBUtils
from dataclasses import dataclass

@dataclass(frozen=True)
class Bronze_Silver_Config:
    constraints: dict
    domain: str
    is_active: bool
    bronze_path: str
    silver_path: str
    checkpoint_path: str
    schema: str
    silver_table_name: str
    bronze_file_format: str

class Datalake:
    def __init__(self,spark):
        self.spark = spark
        self.dbutils = DBUtils(spark)
        self.storage_account_name = self.dbutils.secrets.get(scope="key vault",key="datalake-sa-name")
        self.bronze_continer_name = "bronze_container_name"
        self.silver_continer_name = self.dbutils.secrets.get(scope="key vault",key="datalake-silver-container-name")
        self.gold_continer_name = self.dbutils.secrets.get(scope="key vault",key="datalake-gold-container-name")
        self.config_continer_name = self.dbutils.secrets.get(scope="key vault",key="datalake-config-container-name")
        self.bronze_path = "bronze_path"
        self.silver_path = f"""abfss://{self.silver_continer_name}@{self.storage_account_name}.dfs.core.windows.net"""
        self.config_path = f"""abfss://{self.config_continer_name}@{self.storage_account_name}.dfs.core.windows.net"""
        
        
    def setup_config(self):
        app_id = self.dbutils.secrets.get(scope="key vault",key="databricks-spn-app-id")
        app_key = self.dbutils.secrets.get(scope="key vault",key="databricks-spn-app-key")
        tenant_id = self.dbutils.secrets.get(scope="key vault",key="tenant-id")
        
        #Add Config
        self.spark.conf.set(f"fs.azure.account.auth.type.{self.storage_account_name}.dfs.core.windows.net", "OAuth")
        self.spark.conf.set(f"fs.azure.account.oauth.provider.type.{self.storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        self.spark.conf.set(f"fs.azure.account.oauth2.client.id.{self.storage_account_name}.dfs.core.windows.net", app_id)
        self.spark.conf.set(f"fs.azure.account.oauth2.client.secret.{self.storage_account_name}.dfs.core.windows.net", app_key)
        self.spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{self.storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

    def get_bronze_to_silver_config(self):
        cfg = self.spark.read.option("multiline","true").json(f"{self.config_path}/bronze/")
        return [Bronze_Silver_Config({k: v for k, v in x[0].asDict().items() if v},
                                        x[1],
                                        x[2],
                                        f"{self.bronze_path}/{x[1]}{x[3]}",
                                        f"{self.silver_path}/{x[1]}/{x[5]}",
                                        f"{self.silver_path}/{x[1]}/checkpoints/{x[5]}",
                                        x[4],
                                        x[5],
                                        x[6]) for x in cfg.collect()]

# COMMAND ----------

dl_conn = Datalake(spark)
dl_conn.setup_config()
config = dl_conn.get_bronze_to_silver_config()
silver_container_path = Datalake(spark).silver_path
bronze_path = config[0].bronze_path
silver_path = config[0].silver_path
checkpoint_path = config[0].checkpoint_path
constraints = config[0].constraints
schema = config[0].schema
silver_table_name = config[0].silver_table_name
file_format = config[0].bronze_file_format
quarantine_rules = " AND ".join(str(constr) for constr in constraints.values())
adls_path = "adls_path"

# COMMAND ----------

import json

# COMMAND ----------

jsonData = """{
  "before": null,
  "after": {
    "CustomerID": 30147,
    "NameStyle": false,
    "Title": null,
    "FirstName": "Beautiful",
    "MiddleName": null,
    "LastName": "Pare",
    "Suffix": null,
    "CompanyName": null,
    "SalesPerson": null,
    "EmailAddress": "Beautiful.Pare@dss23.com",
    "Phone": null,
    "PasswordHash": "***",
    "PasswordSalt": "***",
    "rowguid": "DAF1B0F5-609B-4C0D-A9DC-C26E88006922",
    "ModifiedDate": 1698085925673
  },
  "source": {
    "version": "1.9.6.Final",
    "connector": "sqlserver",
    "name": "cdcdatabase",
    "ts_ms": 1698085925693,
    "snapshot": "false",
    "db": "AdventureWorksLT",
    "sequence": null,
    "schema": "SalesLT",
    "table": "Customer",
    "change_lsn": "00000069:00000690:0004",
    "commit_lsn": "00000069:00000690:0009",
    "event_serial_no": 1
  },
  "op": "c",
  "ts_ms": 1698085928730,
  "transaction": null
}

"""

df = spark.read.json(sc.parallelize([jsonData]))

json_schema = df.schema
