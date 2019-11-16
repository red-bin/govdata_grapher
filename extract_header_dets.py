#!/usr/bin/python3

import openpyxl
from multiprocessing import Pool
from utils import db_utils

fp = '/opt/test_data/P456008 13094-FOIA-P456008-TRRdata Responsive Record Produced By R&A.xlsx'

#all of this can probably be done in sql.

file_metadata = {}
col_metadata = {
        'row_count': None,
        'distinct_count': None,
        'median_string_dist': None,
        'type': None
}

print("Loading workbook..")
wb = openpyxl.load_workbook(fp)
print("Done loading workbook")

for worksheet in wb.worksheets:
    worksheet_title = worksheet.title

    rows = worksheet.rows

    column_names = [r.value for r in rows.__next__()]
    if None in column_names:
        print('Worksheet with None as column: ', worksheet_title, column_names)
        continue

    table_name = db_utils.sanitize_table_name(worksheet_title)
    table_col_names = [db_utils.sanitize_column_name(c) for c in column_names]
    worksheet_rows = ([cell.value for cell in row] for row in rows)
    db_utils.insert_raw_delimited(table_name=table_name, vals=worksheet_rows, header=table_col_names)

tagged_tables = {}
for table, col in db_utils.table_columns(schema='rawfiles'):
    print("tagging", table, col)
    tagged_col = db_utils.tag_col(table, col, 'rawfiles')

    if table not in tagged_tables:
        tagged_tables[table] = []

    tagged_tables[table].append(tagged_col)

table_tags = {}
joined_table_sets = []
for table_name, col_tags in tagged_tables.items():
    shared_tables = set()
    for col_tag in col_tags:
        [shared_tables.add(c) for c in col_tag['shared_colname_tables']]

    table_tags[table_name] = dict(tables_w_shared_colnames=shared_tables)

joined_table_sets = []
for table_name, table_tags in table_tags.items():
    does_share = False
    for joined_idx in range(len(joined_table_sets)):
        if joined_table_sets[joined_idx].intersection(table_tags['tables_w_shared_colnames']):
            joined_table_sets[joined_idx].add(table_name)
            does_share = True

    if not does_share:
        joined_table_sets.append({table_name})

#TODO: fix this to work with multiple schemas
#gets cols which are shared across all tables by grabbing intersection of all tables which 
smallest_shared_col = set.intersection(*[set([c['col_name'] for c in cols]) for table,cols in tagged_tables.items() if table in joined_table_sets[0]])

if not smallest_shared_col:
    print("There are no likely primary keys!")

if len(smallest_shared_col) == 1:
    primary_key = smallest_shared_col.pop()
    print("Primary key for tables, {}: {}".format(', '.join(joined_table_sets[0]), primary_key))
else:
    print("There are more than one possible primary keys:", smallest_shared_col)

primary_tables = []
for table_name, col_tags in tagged_tables.items():
    if table_name not in joined_table_sets[0]:
        continue

    for col in col_tags:
        #redo this sensibly..
        if col['col_name'] == primary_key and col['top_ten_freqs'][0][0] == 1:
            primary_tables.append(table_name)

#get count of fully expanded rows by key
join_type = 'FULL OUTER'
foj_count = db_utils.join_tables_by_col(primary_tables, primary_key, only_count=True, join_type=join_type, schema='rawfiles')[0][0]
if foj_count == [c['uniqs_count'] for c in tagged_tables[primary_tables[0]] if c['col_name'] == primary_key]:
    first_table = primary_tables[0]
    combined_csv_data = join_tables_by_col(tagged_tables.keys(), primary_key, schema='rawfiles', join_type='LEFT OUTER', only_count=True)
    print(combined_csv_data)
