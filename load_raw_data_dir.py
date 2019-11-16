#!/usr/bin/python3

from utils import db_utils
import os

conn = db_utils.pg_conn()

datadir = 'dumps/'

metadata_fp = 'datafiles.metadata'

for filename in os.listdir(datadir):
    fp = '{}/{}'.format(datadir, filename)
    table_name, column_names = db_utils.insert_csv(fp, conn=conn)

    conn.commit()
