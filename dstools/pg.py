#Code for exploring PostgresSQL schema

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


def parse_feature_pattern(pattern):
    '''
    Parse feature pattern - based on a string with the format
    table.column, table.%, table.col_% find all columns matching
    Returns a tuple
    '''
    #print 'Parsing pattern: %s' % pattern
    table, column_pattern = pattern.split('.')
    query = ("SELECT table_name, column_name FROM information_schema.columns "
             "WHERE table_schema='features' AND table_name=%s AND "
             "column_name LIKE %s;")

    #Create psycopg2 connection object
    conn = connect(host=main['db']['host'], user=main['db']['user'],
                   password=main['db']['password'], database=main['db']['database'],
                   port=main['db']['port'])
    cur = conn.cursor()
    #Query db and get results
    cur.execute(query, (table, column_pattern))
    results = cur.fetchall()
    #Close connection
    cur.close()
    conn.close()
    return results

def tables_and_columns_for_schema(schema):
    query = ("SELECT table_name, column_name FROM information_schema.columns "
             "WHERE table_schema=%s;")
    #Create psycopg2 connection object
    conn = connect(host=main['db']['host'], user=main['db']['user'],
                   password=main['db']['password'], database=main['db']['database'],
                   port=main['db']['port'])
    cur = conn.cursor()
    #Query db and get results
    cur.execute(query, (schema,))
    results = cur.fetchall()
    #Close connection
    cur.close()
    conn.close()
    return results
