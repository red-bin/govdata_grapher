#!/usr/bin/python3

import docopt
import io
import json
import pandas as pd
import psycopg2cffi
import re

from utils import db_utils

from jinjasql import JinjaSql
from project_conf import conf as proj_conf

conn = db_utils.pg_conn()
curs = conn.cursor()

with open('project.conf', 'r') as fh:
    project_conf = json.load(fh)

print("Getting line count")
with open(project_conf['data_path'], 'r') as fh:
    for c,line in enumerate(fh):
        pass
    line_count = int(c+1)
    sample_size = int(line_count * .1)

print('Line #: ', line_count)
print('Sample size: ', sample_size)

columns_and_types = []
for table_name, raw_col_name in db_utils.table_columns():
    if not table_name.startswith('raw_'):
        continue

    replaces = [('#', 'num'), ('[ \t.-]', '_')]
    col_name = raw_col_name.lower().replace(' ', '_')
    
    for from_str, to_str in replaces:
        col_name = re.sub(from_str, to_str, col_name)

    column_type = db_utils.get_column_type(col_name, table_name, conn=conn)
    print('column: {}, type: {}'.format(col_name, column_type))

    columns_and_types.append((col_name, column_type))

create_str = "CREATE TABLE {} (\n".format(project_conf['project_name'])

for column_name, column_type in columns_and_types:
    create_str += '  {} {},\n'.format(column_name, column_type)

create_str = '{}\n)'.format(create_str[:-2]) #remove last comma and ends query
print(create_str)

curs.execute(create_str)
conn.commit()

print("Entering data from {} into {} table".format(project_conf['data_path'], project_conf['project_name']))
curs.execute("""COPY {} FROM '{}' WITH (FORMAT CSV, DELIMITER ',', QUOTE'"', HEADER);""".format(project_conf['project_name'], project_conf['data_path']))
conn.commit()
