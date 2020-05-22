from sqlalchemy import create_engine, text as sqlalchemy_text
from sqlalchemy.orm import sessionmaker
from django.conf import settings

engine = create_engine(settings.PANGEA_DB_URI, pool_recycle=3600)
conn = engine.connect()

def get_anything(query):
    result = conn.execute(sqlalchemy_text(query))
    result = result.cursor.fetchall()
    if len(result) > 0:
        return result
    return None


def execute_anything(query):
    result = None
    try:
        engine = create_engine(settings.PANGEA_DB_URI)
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


def get_schemas(geo=False):
    complemento = ""
    if geo:
        complemento = " AND udt_name = 'geometry'"
    query = "SELECT DISTINCT table_schema\
        FROM information_schema.columns\
        WHERE table_schema NOT LIKE 'pg_%' AND table_schema != 'information_schema'" + complemento
    result = get_anything(query)
    schemas = [i[0] for i in result]
    return schemas if schemas else None


def get_tables(schema, geo=False):
    query = "SELECT DISTINCT table_name\
                FROM information_schema.columns\
                WHERE table_schema = '{0}' AND udt_name {1} 'geometry'".format(schema, '=')
    if not geo:
        query = "SELECT DISTINCT table_name\
                    FROM information_schema.columns\
                    WHERE table_schema = 'imported_data' and table_name not in (%s)" % query
    result = get_anything(query)
    tables = []
    if result:
        tables = [i[0] for i in result]
    return tables


def get_colunms(schema, table):
    query = "SELECT column_name, udt_name\
            FROM information_schema.columns\
            WHERE table_schema = '{0}' AND table_name = '{1}'".format(schema, table)
    colunms = [{"column": i[0], "type": i[1]}
               for i in get_anything(query)]
    return colunms if colunms else None


def get_geometry_column(schema, table):
    query = "SELECT column_name FROM information_schema.columns WHERE table_schema = '{0}' AND table_name = '{1}' AND udt_name = 'geometry'".format(
        schema, table)
    geom_column_name = get_anything(query)[0][0]
    return geom_column_name


def get_geometry_type(schema, table, geom_column_name):
    query = 'SELECT DISTINCT geometrytype({2}) from {0}.{1}'.format(
        schema, table, geom_column_name)
    geom_type = get_anything(query)[0][0]
    return geom_type


def getDistinctValues(schema, table, field):
    query = "SELECT {0}\
            FROM {1}.{2}\
            GROUP BY {0}\
            ORDER BY {0}".format(field, schema, table)
    result = [i for i in get_anything(query)]
    return result if result else None


def _create_topology(topology_name, srid):
    query = "SELECT topology.CreateTopology('{0}', {1});".format(
        topology_name, srid)
    topology_id = execute_anything(query)
    return topology_name, topology_id


def _drop_topology(topology_name):
    query = "SELECT topology.DropTopology('{0}');".format(
        topology_name)
    topology_name = execute_anything(query)[0][0]
    return topology_name

def _drop_table(schema_name, table_name):
    query = "DROP TABLE IF EXISTS {0}.{1};".format(
        schema_name, table_name)
    return execute_anything(query)


def _create_layer_topology(params):
    """
    topology_name, imported_data_schema, table_name, topo_geom_column_name, geom_type
    """
    query = "SELECT topology.AddTopoGeometryColumn('{topology_name}',\
                '{imported_data_schema}', '{table_name}', \
                '{topo_geom_column_name}', '{geom_type}')".format(**params)
    
    topology_layer_id = execute_anything(query)[0][0]
    return topology_layer_id


def _has_topology(topology_name):
    query = "SELECT * FROM information_schema.schemata where schema_name = '{0}'".format(topology_name)
    if get_anything(query):
        return True
    return False


def _populate_topology(params):
    """
    imported_data_schema, table_name, topo_geom_column_name, geom_column_name, topology_name, topology_layer_id
    """
    query = "UPDATE {imported_data_schema}.{table_name} \
            SET {topo_geom_column_name} = topology.toTopoGeom({geom_column_name}, '{topology_name}', {topology_layer_id})".format(**params)
    execute_anything(query)
    return True

def create_index(params, zoom_index=True):
    try:
        query = "CREATE INDEX {table_name}_geom_idx \
                    ON {layers_published_schema}.{table_name} \
                    USING GIST (geom);".format(**params)
        execute_anything(query)

        query = "CREATE INDEX {table_name}_{geocod}_idx \
                    ON {layers_published_schema}.{table_name} \
                    USING btree ({geocod})".format(**params)  

        execute_anything(query)  
        if zoom_index:
            query = "CREATE INDEX {table_name}_zoom_level_idx \
                        ON {layers_published_schema}.{table_name} \
                        USING btree (zoom_level)".format(**params)
            execute_anything(query)  
        return True
    except Exception as e:
        raise(e)    

def _pre_process_basic_territorial_level_layer(params):
    """
    imported_data_schema, layers_published_schema, table_name, geocod, topo_geom_column_name, zoom_min, zoom_max, columns
    """
    try:
        query = '\
        CREATE TABLE {layers_published_schema}.{table_name} as SELECT\
            pangea_admin_generalizationparams.zoom_level,\
            {table_name}.{geocod}::bigint,\
            {columns}\
            st_transform(topology.ST_Simplify({table_name}.{topo_geom_column_name}, pangea_admin_generalizationparams.factor), 3857) as geom\
        FROM\
            public.pangea_admin_generalizationparams,\
            {imported_data_schema}.{table_name}\
        WHERE\
            pangea_admin_generalizationparams.zoom_level >= {zoom_min} AND\
            pangea_admin_generalizationparams.zoom_level <= {zoom_max};'.format(**params)
        execute_anything(query)
        create_index(params)           
        return True
    except Exception as e:
        raise(e)


def _pre_process_basic_territorial_level_layer_whithout_topology(params):
    """
    imported_data_schema, layers_published_schema, table_name, geocod, topo_geom_column_name, zoom_min, zoom_max, columns
    """
    try:
        query = '\
        CREATE TABLE {layers_published_schema}.{table_name} as SELECT ogc_fid, {geocod}, {columns} \
            st_transform({table_name}.{geom_column}, 3857) as geom\
        FROM\
            {imported_data_schema}.{table_name};'.format(**params)
        execute_anything(query)
        create_index(params, False)           
        return True
    except Exception as e:
        raise(e)


def _pre_process_composed_territorial_level_layer(params):
    """
    """
    try:
        query = '\
            CREATE TABLE {layers_published_schema}.{table_name} as SELECT\
                pangea_admin_generalizationparams.zoom_level,\
                {table_name}.{geocod}, \
                {columns}\
                st_transform(st_union(topology.ST_Simplify({base_table_name}.{topo_geom_column_name}, pangea_admin_generalizationparams.factor)), 3857) as geom\
            FROM \
                {imported_data_schema}.{table_name}, \
                {imported_data_schema}.{base_table_name},\
                pangea_admin_generalizationparams\
            WHERE \
                {table_name}.{composition_column} = {base_table_name}.{base_geocod} AND\
                pangea_admin_generalizationparams.zoom_level >= {zoom_min} and pangea_admin_generalizationparams.zoom_level < {zoom_max}\
            group by   pangea_admin_generalizationparams.zoom_level,{table_name}.{geocod} {colunms_group_by};'.format(**params)
        execute_anything(query)
        create_index(params)           
        return True
    except Exception as e:
        raise(e)

def _pre_process_choroplethlayer_level_layer_whithout_topology(params):
    """
    """
    try:
        query = '\
            CREATE TABLE {layers_published_schema}.{table_name} AS\
            SELECT \
                {base_table_name}.ogc_fid,\
                {table_name}.{geocod}, \
                {columns}\
                {base_table_name}.geom\
            FROM \
                {imported_data_schema}.{table_name} INNER JOIN {layers_published_schema}.{base_table_name}\
            ON\
                {table_name}.{geocod} = {base_table_name}.{base_geocod};'.format(**params)
        execute_anything(query)
        create_index(params, False)           
        return True
    except Exception as e:
        raise(e)

def _pre_process_choroplethlayer_level_layer(params):
    """
    """
    try:
        query = '\
            CREATE TABLE {layers_published_schema}.{table_name} AS\
            SELECT \
                {base_table_name}.zoom_level, \
                {table_name}.{geocod}, \
                {columns}\
                {base_table_name}.geom\
            FROM \
                {imported_data_schema}.{table_name} INNER JOIN {layers_published_schema}.{base_table_name}\
            ON\
                {table_name}.{geocod} = {base_table_name}.{base_geocod};'.format(**params)
        execute_anything(query)
        create_index(params)           
        return True
    except Exception as e:
        raise(e)


def _get_layers(host):
    query = "\
            select json_agg(layer) from (select\
            json_build_object( \
			pangea_admin_layer.id, json_build_object( \
			'name', pangea_admin_layer.name,\
                        'description', pangea_admin_layer.description, 'fields',json_build_object( \
                            'gid', pangea_admin_layer.geocod_column, 'dimension', \
                            l.dimension_column, \
                            'attributes', columns_.fields, \
                            'geom', 'geom', \
                            'geom_type', l.geom_type, \
                            'srid', l.srid, \
                            'enconding', 'utf8'),\
                        'zoom_min', pangea_admin_layer.zoom_min_id, \
                        'zoom_max', pangea_admin_layer.zoom_max_id,\
                        'host', '%s' ||  pangea_admin_layer.name || '/{z}/{x}/{y}.mvt')) as layer\
            from\
            public.pangea_admin_layer \
            inner join ( select b.layer_ptr_id, b.dimension_column, b.geom_type, b.srid\
			from public.pangea_admin_basicterritoriallevellayer b\
			union\
		select c.layer_ptr_id, b.dimension_column, b.geom_type, b.srid\
		from public.pangea_admin_composedterritoriallevellayer c,\
		     public.pangea_admin_basicterritoriallevellayer b\
		where c.is_a_composition_of_id = b.layer_ptr_id\
		union\
		select c.layer_ptr_id, b.dimension_column, b.geom_type, b.srid\
		from public.pangea_admin_choroplethlayer as c\
		inner join (select b.layer_ptr_id, b.dimension_column,	b.geom_type, b.srid\
		from public.pangea_admin_basicterritoriallevellayer b\
		union\
		select c.layer_ptr_id, b.dimension_column, b.geom_type, b.srid\
		from public.pangea_admin_composedterritoriallevellayer c,\
		public.pangea_admin_basicterritoriallevellayer b\
		where c.is_a_composition_of_id = b.layer_ptr_id) as b on \
		c.layer_id = b.layer_ptr_id ) as l on l.layer_ptr_id = pangea_admin_layer.id \
            inner join public.pangea_admin_layerstatus on pangea_admin_layerstatus.layer_id = pangea_admin_layer.id\
            left join \
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
                pangea_admin_layer.id ) as columns_\
            on columns_.id = pangea_admin_layer.id\
            where\
            pangea_admin_layerstatus.status = 8\
	    order by  pangea_admin_layer.id) as foo;" % (host)
    layers = get_anything(query)[0][0]
    return layers

def get_mvt(params):
    query = "\
    select\
        st_asmvt(q, '{layer_name}', 4096, 'geom', '{geocod}') as mvt \
    from\
        (with t as (select  TileBBox({z},{x},{y}) as box) \
        select\
            {geocod},\
            {fields}\
        st_asmvtgeom({table_name}.geom, t.box, 4096, 256, true) as geom\
        from\
        {schema_name}.{table_name}, t\
        where\
            zoom_level = {z}\
        and t.box && {table_name}.geom \
        and st_intersects(t.box, {table_name}.geom)) as q;".format(**params)
    mvt = get_anything(query)[0][0]
    return mvt

def get_mvt_whithout_topology(params):
    query = "\
    select\
        st_asmvt(q, '{layer_name}', 4096, 'geom', 'ogc_fid') as mvt \
    from\
        (with t as (select  TileBBox({z},{x},{y}) as box) \
        select\
            ogc_fid,\
            {geocod},\
            {fields}\
        st_asmvtgeom({table_name}.geom, t.box, 4096, 256, true) as geom\
        from\
        {schema_name}.{table_name}, t\
        where\
            t.box && {table_name}.geom \
            and st_intersects(t.box, {table_name}.geom)) as q;".format(**params)
    mvt = get_anything(query)[0][0]
    return mvt    