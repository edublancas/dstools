import pandas as pd
from string import Template
import os
from sqlalchemy import create_engine
from lib_cinci.db import uri
from lib_cinci.config import load
import logging
import logging.config
import re
import logging
import logging.config
from lib_cinci.config import load

#Config logger
logging.config.dictConfig(load('logger_config.yaml'))
logger = logging.getLogger()

#This file provides generic functions
#to generate spatiotemporal features
def format_column_names(columns, prefix=None):
    #Get rid of non alphanumeric characters
    #and capital letters
    f = lambda s: re.sub('[^0-9a-zA-Z]+', '_', s).lower()
    names = columns.map(f)
    names = pd.Series(names).map(lambda s: '{}_{}'.format(prefix, s)) if prefix else names
    return names

#Utility function to see which tables already exist in schema
def tables_in_schema(con, schema):
    q = '''
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema=%s;
    '''
    cur = con.cursor()
    cur.execute(q, [schema])
    tuples = cur.fetchall()
    names = [t[0] for t in tuples]
    return names

def columns_for_table_in_schema(con, table, schema):
    q = '''
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s
        AND table_name   = %s;
    '''
    cur = con.cursor()
    cur.execute(q, [schema, table])
    tuples = cur.fetchall()
    #names = [t[0] for t in tuples]
    return tuples
