#!/usr/bin/python3

import csv
import sys

csv.field_size_limit(922337203)

r = csv.reader(open('/opt/data/cjp_tables/newsarticles_article.csv', 'r'))

articles = [l for l in r] 
