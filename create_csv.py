#!/usr/bin/python3

from utils import db_utils
import pandas as pd

conn = db_utils.pg_conn()
df = pd.read_sql("SELECT * FROM warrants_viz", conn)
df.to_csv('/tmp/search_warrants.csv', index=False)
