import pandas as pd
from os.path import dirname, join
import sqlalchemy
from sqlalchemy import create_engine


POSTGRES_DB = 'postgresql://admin:admin@localhost:5432/voucher_api'

def test_pandas_to_db():
	df = pd.read_csv('pandas_to_csv_2.csv')
	engine = create_engine(POSTGRES_DB)
	df.to_sql('test'
		, engine
		, index=False
		, if_exists='replace'
		, schema='stage')
	rows = engine.execute("SELECT count(*) FROM stage.test").fetchone()[0]
	assert (int(rows or 0) > 0)