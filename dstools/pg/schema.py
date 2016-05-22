# Code for exploring PostgresSQL schema
# Parameters in some of this functions are being passed in SQL queries,
# this makes them vulverable to SQL injection, if this goes into production
# local SQL verification will be needed


# Utility function to see which tables already exist in schema
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
    return tuples


def tables_and_columns_for_schema(schema):
    query = ("SELECT table_name, column_name FROM information_schema.columns "
             "WHERE table_schema=%s;")
    # Create psycopg2 connection object
    conn = connect(host=main['db']['host'], user=main['db']['user'],
                   password=main['db']['password'], database=main['db']['database'],
                   port=main['db']['port'])
    cur = conn.cursor()
    # Query db and get results
    cur.execute(query, (schema,))
    results = cur.fetchall()
    # Close connection
    cur.close()
    conn.close()
    return results

def existing_feature_schemas():
    engine = create_engine(uri)
    schemas = "SELECT schema_name AS schema FROM information_schema.schemata"
    schemas = pd.read_sql(schemas, con=engine)["schema"]
    schemas = [s for s in schemas.values if s.startswith("features")]
    return schemas
