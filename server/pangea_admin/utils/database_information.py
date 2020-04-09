import sqlalchemy 
def get_anything(pg_uri, query):
    try:
        engine = sqlalchemy.create_engine(pg_uri)
    except:
        return 
    conn = engine.connect()
    result = conn.execute(sqlalchemy.text(query))
    result = result.cursor.fetchall()
    conn.close()
    return result
    
def get_schemas(pg_uri, geo=False):
    complemento =""
    if geo:
        complemento = " AND udt_name = 'geometry'" 
    query = "SELECT DISTINCT table_schema\
        FROM information_schema.columns\
        WHERE table_schema NOT LIKE 'pg_%' AND table_schema != 'information_schema'" + complemento
    result = get_anything(pg_uri, query)
    schemas = [ i[0] for i in result]
    return schemas if schemas else None
    
def get_tables(pg_uri, schema, geo=False):
    query = "SELECT DISTINCT table_name\
                FROM information_schema.columns\
                WHERE table_schema = '{0}' AND udt_name {1} 'geometry'".format(schema, '=')
    if not geo:
        query = "SELECT DISTINCT table_name\
                    FROM information_schema.columns\
                    WHERE table_schema = 'imported_data' and table_name not in (%s)" % query
    result = get_anything(pg_uri, query)
    tables = [ i[0] for i in result]
    return tables if tables else None

def get_colunms(pg_uri, schema, table):
    query = "SELECT column_name, udt_name\
            FROM information_schema.columns\
            WHERE table_schema = '{0}' AND table_name = '{1}'".format(schema, table)
    colunms = [{"column": i[0], "type": i[1]} for i in get_anything(pg_uri, query)]
    return colunms if colunms else None

def getDistinctValues(pg_uri, schema, table, field):    
      
    query = "SELECT {0}\
            FROM {1}.{2}\
            GROUP BY {0}\
            ORDER BY {0}".format(field, schema, table)
    result = [ i for i in getAnything(pg_uri, query)]
    return result if result else None
