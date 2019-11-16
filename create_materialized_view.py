#!/usr/bin/python3

from jinjasql import JinjaSql

from data_conf import data_conf
from project_conf import conf as proj_conf
from utils.db_utils import pg_conn

columns_templ = """
  {{project_name|sqlsafe}}.{{date_field|sqlsafe}},
  {{project_name|sqlsafe}}.grid_id,
  {{project_name|sqlsafe}}.is_business_district,
"""

left_joins = ""

for selector in data_conf.multi_selectors:
    column_name = selector['column_name']
    table_name = table_name='{}_tbl'.format(column_name)
   
    #add columns from multi_select 
    sql_vals = dict(
        column_name=column_name,
        table_name=table_name,
        project_name=proj_conf.project_name
    )
 
    sql_str = '{table_name}.{column_name},\n'.format(**sql_vals)
    columns_templ += sql_str

    join_templ = """
      LEFT OUTER JOIN {table_name} 
      ON {table_name}.id = {project_name}.{column_name}
    """
    left_joins += join_templ.format(**sql_vals)

columns_templ = columns_templ[:-2] #remove unnecessary comma

mv_templ = """
CREATE MATERIALIZED VIEW {project_name}_mv AS
SELECT 
{columns_templ}
FROM {project_name}
{left_joins}
ORDER BY {date_field} ;
"""

mv_sql_vals = dict(
    project_name=proj_conf.project_name,
    date_field=data_conf.primary_date,
    columns_templ=columns_templ,
    left_joins=left_joins
)

mv_templ = mv_templ.format(**mv_sql_vals)

j = JinjaSql()

conn = pg_conn()
curs = conn.cursor()

query, bind_params = j.prepare_query(mv_templ, mv_sql_vals)
print(query)
curs.execute(query, list(set(bind_params)))
conn.commit()
