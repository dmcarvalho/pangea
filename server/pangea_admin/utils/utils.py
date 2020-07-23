import unidecode
from .database_information import get_colunms

def generate_safe_name(name):
    return unidecode.unidecode('{0}'.format(name)).replace(' ', '_').replace('-', '_').lower()

def build_query_for_array_string(column, values, tp = None):
    return '%s  && ARRAY[%s]::%s[]' % (column, ', '.join(["'%s'" % i for i in values]), tp)

def build_query_for_array_numeric(column, values, tp = None):
    return '%s && ARRAY[%s]::%s[]' % (column, ', '.join(values), tp)

def build_query_for_string(column, values, tp = None):
    return '%s IN (%s)' % (column, ', '.join(["'%s'" % i for i in values]))

def build_query_for_numeric(column, values, tp = None):
    return '%s IN (%s)' % (column, ', '.join(values))

build_query_for = {'_char': build_query_for_array_string,
            '_text': build_query_for_array_string,
            '_varchar': build_query_for_array_string,
            '_float4': build_query_for_array_numeric,
            '_float8': build_query_for_array_numeric,
            '_int2': build_query_for_array_numeric,
            '_int4': build_query_for_array_numeric,
            '_bool': build_query_for_array_numeric,
            '_int8': build_query_for_array_numeric,
            'char': build_query_for_string,
            'varchar': build_query_for_string,
            'text': build_query_for_string,
            'timestamp': build_query_for_string,
            'timestamptz': build_query_for_string,
            'time': build_query_for_string,
            'int8': build_query_for_numeric,
            'bool': build_query_for_numeric,
            'float8': build_query_for_numeric,
            'int4': build_query_for_numeric,
            'numeric': build_query_for_numeric,
            'float4': build_query_for_numeric,
            'int2': build_query_for_numeric}


def query_params_processor(schema_name, table_name, params):
    colunms = {i['column']: i['type'] for i in get_colunms(schema_name, table_name)}
    valid_keys = [i for i in params.keys() if i in colunms]
    query = []
    for i in valid_keys:
        q = build_query_for[colunms[i]](i, params.getlist(i), colunms[i][1:]) 
        query.append(q)
    return 'AND ' + ' AND '.join(query) if query else ''