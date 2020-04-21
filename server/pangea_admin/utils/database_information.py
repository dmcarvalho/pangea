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
    if len(result) > 0:
        return result
    return None


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
        raise(e)
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


def _create_layer_topology(pg_uri, topology_name, imported_data_schema, table_name, topo_geom_column_name, geom_type):
    query = "SELECT topology.AddTopoGeometryColumn('{0}', '{1}', '{2}', '{3}', '{4}')".format(
        topology_name, imported_data_schema, table_name, topo_geom_column_name, geom_type)
    topology_layer_id = execute_anything(pg_uri, query)[0][0]
    return topology_layer_id


def _has_topology(pg_uri, topology_name):
    query = "SELECT * FROM information_schema.schemata where schema_name = '{0}'".format(topology_name)
    if get_anything(pg_uri, query):
        return True
    return False


def _populate_topology(pg_uri, imported_data_schema, table_name, topo_geom_column_name, geom_column_name, topology_name, topology_layer_id):
    query = "UPDATE {0}.{1} SET {2} = topology.toTopoGeom({3}, '{4}', {5})".format(
        imported_data_schema, table_name, topo_geom_column_name, geom_column_name, topology_name, topology_layer_id)
    execute_anything(pg_uri, query)
    return True

def _pre_process_layer(pg_uri, imported_data_schema, layers_published_schema, table_name, geocod, geocod_column, zoom_min, zoom_max, columns):
    try:
        query = '\
        CREATE TABLE {0}.{1} as SELECT\
            pangea_admin_generalizationparams.zoom_level,\
            {1}.{3}::bigint,\
            {7}\
            st_transform(topology.ST_Simplify({1}.{4}, pangea_admin_generalizationparams.factor), 3857) as {4}\
        FROM\
            public.pangea_admin_generalizationparams,\
            {2}.{1}\
        WHERE\
            pangea_admin_generalizationparams.zoom_level >= {5} AND\
            pangea_admin_generalizationparams.zoom_level <= {6};'.format(layers_published_schema,
                                                                        table_name,
                                                                        imported_data_schema,
                                                                        geocod,
                                                                        geocod_column,
                                                                        zoom_min,
                                                                        zoom_max,
                                                                        columns)
        execute_anything(pg_uri, query)
        return True
    except Exception as e:
        raise(e)


def _get_layers(pg_uri, host):
    query = "\
            select\
                json_agg(json_build_object( \
                    pangea_admin_layer.name, json_build_object( \
                            'description', pangea_admin_layer.description, 'fields',json_build_object( \
                                'geocod', pangea_admin_layer.geocod_column, 'dimension', \
                                pangea_admin_basicterritoriallevellayer.dimension_column, \
                                'attributes', columns.fields, \
                                'geom', 'pangea_admin_basicterritoriallevellayer.topo_geom_column_name, \
                                'geom_type', pangea_admin_basicterritoriallevellayer.geom_type, \
                                'srid', pangea_admin_basicterritoriallevellayer.srid, \
                                'enconding', 'utf8'),\
                            'zoom_min', pangea_admin_layer.zoom_min_id, \
                            'zoom_max', pangea_admin_layer.zoom_max_id,\
                            'host', '%s' ||  pangea_admin_layer.name || '/{z}/{x}/{y}.mvt')))\
            from\
                public.pangea_admin_layer,\
                public.pangea_admin_basicterritoriallevellayer,\
                    public.pangea_admin_layerstatus,\
                (\
                select\
                    pangea_admin_layer.id,\
                    json_agg(pangea_admin_column.alias) as fields\
                from\
                    public.pangea_admin_layer,\
                    public.pangea_admin_column\
                where\
                    pangea_admin_column.layer_id = pangea_admin_layer.id\
                group by\
                    pangea_admin_layer.id\
                order by\
                    pangea_admin_layer.id ) as columns\
            where\
                pangea_admin_basicterritoriallevellayer.layer_ptr_id = pangea_admin_layer.id  AND\
                pangea_admin_layerstatus.layer_id = pangea_admin_layer.id and   pangea_admin_layerstatus.status = 8 AND \
                columns.id = pangea_admin_layer.id;" % (host)
    layers = execute_anything(pg_uri, query)[0][0]
    return layers

def get_mvt(pg_uri, params):
    query = "\
    select\
        st_asmvt(q, '{layer_name}', 4096, 'geom', '{geocod}') as mvt \
    from\
        (with t as (select  TileBBox({z},{x},{y}) as box) \
        select\
            {geocod},\
            {fields},\
        st_asmvtgeom({table_name}.{geom}, t.box, 4096, 256, true) as geom\
        from\
        {schema_name}.{table_name}, t\
        where\
            zoom_level = {z}\
        and t.box && {table_name}.{geom} \
        and st_intersects(t.box, {table_name}.{geom})) as q;".format(**params)
    mvt = get_anything(pg_uri, query)[0][0]
    return mvt
