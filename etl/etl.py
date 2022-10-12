import wget
import pandas as pd
from os.path import dirname, join
import sqlalchemy
from sqlalchemy import create_engine

DOWNLOAD_URL = "https://drive.google.com/u/0/uc?id=1t_0Wt5Vbs44oZoTrG0Hwg9OZxNkoo5m4&export=download"
POSTGRES_DB = 'postgresql://admin:admin@localhost:5432/voucher_api'

print("STARTING ETL JOB!")
print("RETRIEVING FILE FROM URL: " + DOWNLOAD_URL)
file = wget.download(DOWNLOAD_URL)
df = pd.read_parquet(file)
print("RETRIEVING FILE SUCCESS!")

print(df.shape)
print(df.head)
print(df.isna().sum())

print("START DATA CLEANING")
pd.set_option('mode.chained_assignment', None)
df_peru = df[df.country_code == 'Peru']
df_peru.drop(df_peru[df_peru.voucher_amount.isna()].index, inplace=True)
df_peru.drop(df_peru[df_peru.total_orders.isna()].index, inplace=True)
df_peru.drop(df_peru[df_peru.total_orders == ''].index, inplace=True)
df_peru.drop(df_peru[df_peru.total_orders == '0.0'].index, inplace=True)
df_peru.drop(df_peru[df_peru.last_order_ts < df_peru.first_order_ts].index, inplace=True)
print("FINISH DATA CLEANING")
pd.reset_option("mode.chained_assignment")
#print(df_peru)

print("CREATE OR REPLACE TABLE FROM DATA")
engine = create_engine(POSTGRES_DB)

# df.to_sql('vouchers'
# 	, engine
# 	, index=False
# 	, if_exists='replace'
# 	, schema='raw'
# 	, dtype={"timestamp": sqlalchemy.DateTime
# 		,"country_code": sqlalchemy.Text
# 		,"last_order_ts": sqlalchemy.DateTime
# 		,"first_order_ts": sqlalchemy.DateTime
# 		,"total_orders": sqlalchemy.Numeric
# 		,"voucher_amount": sqlalchemy.Float
# })
df_peru.to_sql('vouchers'
	, engine
	, index=False
	, if_exists='replace'
	, schema='production'
	, dtype={"timestamp": sqlalchemy.DateTime
		,"country_code": sqlalchemy.Text
		,"last_order_ts": sqlalchemy.DateTime
		,"first_order_ts": sqlalchemy.DateTime
		,"total_orders": sqlalchemy.Numeric
		,"voucher_amount": sqlalchemy.Float
})

print("OPENING model_stg.sql")
with open('etl/model_stg.sql', 'r') as sql_file:
    model_stg_sql = sql_file.read()

print("OPENING model_prod.sql")
with open('etl/model_prod.sql', 'r') as sql_file:
    model_prod_sql = sql_file.read()

print("EXECUTING SQL")
print(engine.execute(model_stg_sql))
print(engine.execute(model_prod_sql))

