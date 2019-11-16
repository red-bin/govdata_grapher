#!/usr/bin/python3

from project_conf import conf as proj_conf

import pandas as pd
import numpy as np

import re

from jinjasql import JinjaSql
from strsimpy.levenshtein import Levenshtein
import psycopg2
from psycopg2 import extensions as ext 
from psycopg2.extras import execute_values

from scipy.stats import entropy

def pg_conn():
    conn_str = "dbname={db_name} host={db_host} user={db_user} password={db_pass}".format(**proj_conf.db_info)
    conn = psycopg2.connect(conn_str)

    return conn

def table_df(table_name, sqlstr=None, conn=None):
    if not conn:
        conn = pg_conn()

    if not sqlstr:
        sqlstr = "SELECT * FROM {}".format(table_name)

    df = pd.read_sql(sqlstr, conn)
    return df

def get_column_type(column_name, table_name, schema='public', conn=None):
    """Tests to find data type"""
    if not conn:
        conn = pg_conn()

    curs = conn.cursor()

    #rows = #[str(r).strip() for r in rows if r]

    j = JinjaSql()

    print('column name', column_name)
    templ = """SELECT distinct({{column_name|sqlsafe}}::{{test_type|sqlsafe}} )
               FROM {{schema|sqlsafe}}.{{table_name|sqlsafe}} TABLESAMPLE SYSTEM ({{percent|sqlsafe}})
               WHERE {{column_name|sqlsafe}} is not NULL AND {{column_name|sqlsafe}} != ''"""

    possible_types = ['BOOLEAN', 'TIMESTAMP', 'TIME', 'DATE', 'INT2', 'INT4', 'INT8', 'FLOAT4', 'FLOAT8', 'TEXT']

    working_types = []
    for test_type in possible_types:
        vals = dict(column_name=column_name, test_type=test_type, percent=10, table_name=table_name, schema=schema)
        try:
            query, bind_params = j.prepare_query(templ, vals)
        except Exception as err:
            print("Jinja unable to parse {} for type: {}".format(templ, vals))
            print("  ", err)

        try:
            curs.execute(query, list(bind_params))
            sample_good = True
        except Exception as err:
            print(err)
            sample_good = False
            conn.rollback()

        if sample_good:
            vals['percent'] = 100 #try everything!
            query, bind_params = j.prepare_query(templ, vals)
            try:
                curs.execute(query, list(bind_params))
                return test_type
                #working_types.append(test_type)
            except:
                conn.rollback()
                pass

    if working_types:
        return working_types[0]

    else:
        return None

def table_columns(schema, conn=None):
    if not conn:
        conn = pg_conn()

    curs = conn.cursor()

    sqlstr = """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}' ;
    """.format(**dict(schema=schema))

    curs.execute(sqlstr) 
    results = curs.fetchall()

    return results

#def same_name_cols(schema):
    sqlstr = """
        SELECT i1.table_name, i1.column_name, i2.table_name, i2.column_name
        FROM information_schema.columns i1, information_schema.columns i2 
        WHERE i1.column_name = i2.column_name 
        AND i1.table_name != i2.table_name 
        AND i1.table_schema = 'rawfiles' 
        AND i2.table_schema = 'rawfiles'
    """

def create_table(table_name, column_names, filepath=None, schema='public', conn=None):
    if not conn:
        conn = pg_conn()

    curs = conn.cursor()

    j = JinjaSql()
    create_str = "CREATE TABLE IF NOT EXISTS {{schema|sqlsafe}}.{{table_name|sqlsafe}} ("

    for col_name in column_names:
        create_str += ' {} TEXT,\n'.format(col_name)

    create_str = '{}\n)'.format(create_str[:-2])
    query, bind_params = j.prepare_query(create_str, {'table_name': table_name, 'schema': schema})

    curs.execute(query, tuple(bind_params))
    conn.commit()

def sanitize_column_name(column_name):
    print(column_name)
    new_name = re.sub('[:._\- ]+', '_',  column_name)
    new_name = re.sub('__', '_', new_name)

    new_name = new_name.replace('&', 'and')
    new_name = new_name.replace('#', 'num')
    new_name = new_name.lower()

    return new_name

def sanitize_table_name(table_name):
    new_name = re.sub('[:._\- ]+', '_',  table_name)
    new_name = re.sub('__', '_', new_name)

    new_name = new_name.replace('&', 'and')
    new_name = new_name.replace('#', 'num')
    new_name = new_name.lower()

    return new_name

def insert_raw_delimited(table_name, vals, header=None, conn=None,  threadcount=12, schema='rawfiles'):
    if not conn:
        conn = pg_conn()

    curs = conn.cursor()

    create_table(table_name, header, schema=schema, conn=conn)

    col_fmt = ext.quote_ident(','.join(header), curs)
    table_fmt = ext.quote_ident(table_name, curs)

    insert_sqlstr = "INSERT INTO {}.{} VALUES %s" 
    insert_sqlstr = insert_sqlstr.format(schema, table_name)

    print(insert_sqlstr)

    csv_batchsize = 1000000
    sql_batchsize = 10000

    count = 0
    batch = []
    execute_values(curs, insert_sqlstr, vals, page_size=sql_batchsize)
    conn.commit()

    return table_name, 

def sample_rows(table_name, sample_percent=5, seed=100, conn=None):
    if not conn:
        conn = pg_conn()

    curs = conn.cursor()
    sqlstr = "SELECT * FROM {table_name} TABLESAMPLE SYSTEM({sample_percent}) REPEATABLE({seed})"
    vals  = dict(col_name=col_name, table_name=table_name, 
              sample_percent=sample_percent, seed=seed)
    sqlstr = sqlstr.format(**vals)

    curs.execute(sqlstr)
    rows = curs.fetchall()

    return rows

def sample_col(table_name, col_name, schema, sample_percent=5, seed=100, conn=None):
    if not conn:
        conn = pg_conn()

    curs = conn.cursor()
    sqlstr = "SELECT {col_name} FROM {schema}.{table_name} TABLESAMPLE SYSTEM({sample_percent}) REPEATABLE({seed})"
    vals  = dict(col_name=col_name, table_name=table_name, schema=schema,
              sample_percent=sample_percent, seed=seed)
    sqlstr = sqlstr.format(**vals)

    curs.execute(sqlstr)
    rows = [r[0] for r in curs.fetchall()]

    return rows

def insert_csv(filepath, conn=None):
    from multiprocessing import Pool
    p = Pool(12)
    if not conn:
        conn = pg_conn()

    curs = conn.cursor()

    import csv
    csv.field_size_limit(922337203)

    print("Inserting data from", filepath)
    if not filepath.endswith('.csv'):
        return None

    file_base = filepath.split('/')[-1].replace('.csv','')
    table_name = 'raw_{}'.format(file_base)
    print("Creating new table: %s" % table_name)

    fh = open(filepath, 'r')
    r = csv.reader(fh)

    header_row = r.__next__()
    print(header_row)
    header = list(map(sanitize_column_name, header_row))
    insert_raw_delimited(table_name, r, header, conn)

    return table_name, list(zip(header_row, header))

def cols_with_same_name(schema, col_name=None, table_name=None, conn=None):
    """returns (matching_column, tablename1, tablename2)""" 
    #TODO turn into stored func
    if not conn:
        conn = pg_conn()

    curs = conn.cursor()

    sqlstr = """
        SELECT i2.column_name, i1.table_name, i2.table_name
        FROM information_schema.columns i1, information_schema.columns i2 
        WHERE i1.column_name = i2.column_name 
        AND i1.table_name != i2.table_name 
        AND i1.table_schema = '{schema}'
        AND i2.table_schema = '{schema}'
    """

    if col_name:
        sqlstr += "AND i1.column_name = '{col}'\n"

    if table_name:
        sqlstr += "AND i1.table_name = '{table}'\n"

    sqlstr = sqlstr.format(**dict(schema=schema, col=col_name, table=table_name))
    curs.execute(sqlstr) 

    same_named = []
    for col_name, table1, table2 in curs.fetchall():
        if (col_name, table2, table1) in same_named:
            continue 

        same_named.append((col_name, table1, table2))

    return same_named
        
def tables_with_same_col(table_name, col_name, conn=None):
    cols = table_columns()

def col_freqs(table_name, col_name, limit=None, schema='public', ignore_blank=False, conn=None):
    if not conn:
        conn = pg_conn()

    curs = conn.cursor()

    sqlstr = """
        SELECT count(*), {col_name} 
        FROM {schema}.{table_name}
        {ignore_blank} 
        GROUP BY {col_name} 
        ORDER BY count DESC"""

    if limit:
        sqlstr += " LIMIT {limit}"

    if ignore_blank:
        ignore_blank = "WHERE {col_name} IS NOT NULL".format(col_name=col_name)
    else:
        ignore_blank = ""

    vals = dict(col_name=col_name, 
            table_name=table_name,
            limit=limit,
            ignore_blank=ignore_blank,
            schema=schema)

    sqlstr = sqlstr.format(**vals)

    curs.execute(sqlstr)
    freqs = list(curs.fetchall())

    return freqs

def tag_col(table_name, col_name, schema='public', conn=None):
    print("Tagging {}.{}.{}".format(schema, table_name, col_name))
    if not conn:
        conn = pg_conn()

    curs = conn.cursor()

    sample_data = sample_col(table_name, col_name, schema=schema, sample_percent=5, conn=conn)
    freqs = col_freqs(table_name, col_name, schema=schema, conn=conn)

    top_ten_freqs = freqs[:10] #top ten most freq

    sample_uniqs = list(set(sample_data))

    uniq_entropies = {}
    for uniq in sample_uniqs:
        if not uniq:
            continue

        uniq_entropies[uniq] = entropy(bytearray(uniq.encode("unicode_escape")))

    median_entropy = np.median(list(uniq_entropies.values()))

    print('median_entropy', uniq_entropies)

    shared_colname_tables = []
    same_named = cols_with_same_name(schema=schema, col_name=col_name, table_name=table_name)
    for _, _, shared_table in same_named:
        shared_colname_tables.append(shared_table)

    #print("calculating levenstein distance")
    #levenstein = Levenshtein()
    #distances = []
    #for cell1 in sample_uniqs:
    #    for cell2 in sample_uniqs:
    #        if cell1 == cell2:
    #            continue
    #        lev_distance = levenstein.distance(cell1, cell2)
    #        distances.append(lev_distance)
    #median_distance = np.median(distances)

    #distinct counts
    sqlstr = "SELECT count(*), {col_name} from {schema}.{table_name} group by {col_name} order by count"
    curs.execute(sqlstr.format(**dict(col_name=col_name, table_name=table_name, schema=schema)))

    distinct_counts = curs.fetchall()
    uniqs_count = len(distinct_counts)
    distincts = [i[0] for i in distinct_counts]

    #avoids more sql
    null_count = sum([i[0] for i in distinct_counts if not i[0]])

    pg_col_type = get_column_type(col_name, table_name, schema=schema)

    is_address = False #TODO: implement this

    tags = dict(
        col_name=col_name,
        pg_col_type=pg_col_type, 
        null_count=null_count, 
        uniqs_count=uniqs_count,
        #semantic_type=semantic_type,
        shared_colname_tables=shared_colname_tables,
        median_entropy=median_entropy,
        top_ten_freqs=freqs[:10],
        is_address=is_address)

    return tags

#TODO: make this work for an arbitrary # of table/col names. Possibly use columnar stores
def join_tables_by_col(table_names, col_name, join_type='FULL OUTER', schema='public', only_count=False, conn=None):
    assert len(table_names) >= 2, 'table_names length must be > 2'

    if not conn:
        conn = pg_conn()

    curs = conn.cursor()

    if only_count:
        sqlstr = "SELECT count(*) FROM {schema}.{first_table} s\n"
    else:
        sqlstr = "SELECT * FROM {schema}.{first_table} s\n"

    sqlstr = sqlstr.format(schema=schema, col_name=col_name, first_table=table_names[0])

    for table_name in table_names[1:]:
        join_str = "FULL OUTER JOIN {schema}.{table_name} a ON (s.{col_name} = a.{col_name})\n"
        join_str = join_str.format(schema=schema, table_name=table_name, col_name=col_name)

        sqlstr += join_str

    print(sqlstr)
    curs.execute(sqlstr)

    return list(curs.fetchall())
