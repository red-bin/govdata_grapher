#!/usr/bin/python3

import csv
import json
from utils import db_utils
from geocodio import GeocodioClient
from psycopg2.extras import execute_values

def get_client():
    auth_id = "nope"
    client = GeocodioClient(auth_id)

    return client

def insert_geocoded_file(fp, orig_addr_col, lng_col, lat_col, geocoder):
    uniqs = {}
    fh = open(fp, 'r')

    for row in csv.DictReader(fh):
        addr = row[orig_addr_col].upper()
        uniqs[addr] = (row[lng_col], row[lat_col])

    values = []
    for addr, lnglat in uniqs.items():
        lng, lat = lnglat
        row_vals = (fp, geocoder, addr, lng, lat)
        values.append(row_vals)

    conn = db_utils.pg_conn()
    curs = conn.cursor()

    sqlstr = """INSERT INTO geocoding.conversions 
                   (source, geocoder, orig_address, lng, lat) 
                VALUES %s"""

    execute_values(curs, sqlstr, values)
    conn.commit()

def get_ungeocoded_addrs(table_name, addr_column):
    sqlstr = """
        SELECT DISTINCT(r.requested_address) 
        FROM {table_name} r 
        LEFT OUTER JOIN geocoding.conversions g 
        ON (r.{addr_column} = g.orig_address) 
        WHERE g.orig_address IS NULL
        AND g.geocoded_address IS NULL ;"""
    sqlstr = sqlstr.format(**dict(addr_column=addr_column, table_name=table_name))

    conn = db_utils.pg_conn()
    curs = conn.cursor()

    curs.execute(sqlstr) 
    addresses = [i[0] for i in curs.fetchall()]

    return addresses

def best_geocodio_result(addr_results, cutoff=.9):
    highest_result = None
    for result in addr_results:
        accuracy = result['accuracy']
        if accuracy < cutoff:
            continue

        if not highest_result:
            highest_result = result

        elif accuracy > highest_result['accuracy']:
            highest_result = result

        elif accuracy == highest_result['accuracy']:
            pass

    return highest_result
 
def insert_geocodio_results(geocodio_results, source):
    sqlvals = []
    for geocoded in geocodio_results:
        input_addr = geocoded['input']['formatted_address']

        addr_results = geocoded['results']
        best_result = best_geocodio_result(addr_results)
    
        if not best_result:
            continue
    
        latlng = best_result['location']
        geocoded_address = best_result['formatted_address']
    
        #normalize back to original address
        orig_addr = input_addr.replace(', Chicago, IL', '').upper()
        lng = latlng['lng']
        lat = latlng['lat']

        sqlvals.append((source, 'geocodio', orig_addr, geocoded_address, lng, lat))

    conn = db_utils.pg_conn()
    curs = conn.cursor()

    sqlstr = """INSERT INTO geocoding.conversions 
                   (source, geocoder, orig_address, geocoded_address, lng, lat) 
                VALUES %s"""

    execute_values(curs, sqlstr, list(set(sqlvals)))
    conn.commit()

geocoded_files = [
    {
        'fp': 'P494685_Martinez_Freddy_Chicago_SWs.geocoded.csv',
        'orig_addr_col': 'Address Merged',
        'geocoder':'geocodio',
        'lng_col': 'lng',
        'lat_col': 'lat',
    }
]

for gfile in geocoded_files:
    insert_geocoded_file(**gfile)

filename = '14418__P518853_Chapman_Search_Warrant.csv'
old_addr_col = 'Requested Address'

client = get_client()

table_name = 'raw_14418__p518853_chapman_search_warrant'
addr_column = 'requested_address'

ungeocoded_addrs = get_ungeocoded_addrs(table_name, addr_column)

#geocoded_results = client.geocode(ungeocoded_addrs)

with open('geocoded_results.json', 'r') as fh:
    geocoded_results = json.load(fh)

insert_geocodio_results(geocoded_results, filename)
