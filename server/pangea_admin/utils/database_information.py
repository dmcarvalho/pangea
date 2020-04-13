from sqlalchemy import create_engine, text as sqlalchemy_text
from sqlalchemy.orm import sessionmaker

def get_anything(pg_uri, query):
    try:
        engine = create_engine(pg_uri)
    except:
        return
    conn = engine.connect()
    result = conn.execute(sqlalchemy_text(query))
    result = result.cursor.fetchall()
    conn.close()
    return result


def execute_anything(pg_uri, query):
    result = None
    try:
        engine = create_engine(pg_uri)
    except:
        return
    try:
        with engine.connect().execution_options(autocommit=True) as conn:
            result = conn.execute(sqlalchemy_text(query))
            if result.cursor:
                result = result.cursor.fetchall()
            else:
                result = None
    except Exception as e:
        raise('Ocorreu um erro: %s' % e )
    finally:
        conn.close()

    return result


def get_schemas(pg_uri, geo=False):
    complemento = ""
    if geo:
        complemento = " AND udt_name = 'geometry'"
    query = "SELECT DISTINCT table_schema\
        FROM information_schema.columns\
        WHERE table_schema NOT LIKE 'pg_%' AND table_schema != 'information_schema'" + complemento
    result = get_anything(pg_uri, query)
    schemas = [i[0] for i in result]
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
    tables = [i[0] for i in result]
    return tables if tables else None


def get_colunms(pg_uri, schema, table):
    query = "SELECT column_name, udt_name\
            FROM information_schema.columns\
            WHERE table_schema = '{0}' AND table_name = '{1}'".format(schema, table)
    colunms = [{"column": i[0], "type": i[1]}
               for i in get_anything(pg_uri, query)]
    return colunms if colunms else None


def get_geometry_column(pg_uri, schema, table):
    query = "SELECT column_name FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = '{1}' AND udt_name = 'geometry'".format(
        schema, table)
    geom_column_name = get_anything(pg_uri, query)[0][0]
    return geom_column_name


def get_geometry_type(pg_uri, schema, table, geom_column_name):
    query = 'SELECT DISTINCT geometrytype({2}) from {0}.{1}'.format(
        schema, table, geom_column_name)
    geom_type = get_anything(pg_uri, query)[0][0]
    return geom_type


def getDistinctValues(pg_uri, schema, table, field):
    query = "SELECT {0}\
            FROM {1}.{2}\
            GROUP BY {0}\
            ORDER BY {0}".format(field, schema, table)
    result = [i for i in get_anything(pg_uri, query)]
    return result if result else None


def _create_topology(pg_uri, topology_name, srid):
    query = "SELECT topology.CreateTopology('{0}', {1});".format(
        topology_name, srid)
    topology_id = execute_anything(pg_uri, query)[0][0]
    return topology_name, topology_id


def _drop_topology(pg_uri, topology_name):
    query_create_topology = "SELECT topology.DropTopology('{0}');".format(
        topology_name)
    topology_name = execute_anything(pg_uri, query)[0][0]
    return topology_name


def _create_layer_topology(pg_uri, topology_name, schema_name, table_name, topo_geom_column_name, geom_type):
    query = "SELECT topology.AddTopoGeometryColumn('{0}', '{1}', '{2}', '{3}', '{4}')".format(
        topology_name, schema_name, table_name, topo_geom_column_name, geom_type)
    topology_layer_id = execute_anything(pg_uri, query)[0][0]
    return topology_layer_id


def _populate_topology(pg_uri, schema_name, table_name, topo_geom_column_name, geom_column_name, topology_name, topology_layer_id):
    query = "UPDATE {0}.{1} SET {2} = topology.toTopoGeom({3}, '{4}', {5})".format(
        schema_name, table_name, topo_geom_column_name, geom_column_name, topology_name, topology_layer_id)
    execute_anything(pg_uri, query)
    return True
    # -- Populate the layer and the topology
    # ; -- 8.75 seconds

    # -- Simplify all edges up to 10000 units
    # SELECT SimplifyEdgeGeom('france_dept_topo', edge_id, 10000) FROM france_dept_topo.edge; -- 3.86 seconds

    # -- Convert the TopoGeometries to Geometries for visualization
    # ALTER TABLE france_dept ADD geomsimp GEOMETRY;
    # UPDATE france_dept SET geomsimp = topogeom::geometry; -- 0.11 seconds
