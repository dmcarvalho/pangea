from django.conf import settings
from .database_information import _pre_process_basic_territorial_level_layer,\
    _pre_process_composed_territorial_level_layer, _pre_process_choroplethlayer_level_layer
from ..models import LayerStatus

def pre_process_basic_territorial_level_layer(layer):
    if layer.status < LayerStatus.Status.TOPOLOGY_CREATED:
        return {"error": "Before this action you must create a topology!"}
    if layer.status >= LayerStatus.Status.LAYER_PRE_PROCESSED:
        return {'error': 'This action has been executed!'}
    zoom_min = layer.zoom_min.zoom_level
    zoom_max = layer.zoom_max.zoom_level
    geocod = layer.geocod_column
    topo_geom_column_name = layer.topo_geom_column_name
    table_name = layer.table_name
    colunms = []
    for column in layer.column_set.all():
        colunms.append('%s.%s as %s,' % (layer.table_name, column.name, column.alias))
    columns = ' '.join(colunms)

    if not zoom_min or not zoom_max or not geocod:
        return {"error": "Before this action you must define zoom parameters and geocod_column!"}
    try:
        params = {
            "imported_data_schema": settings.PANGEA_IMPORTED_DATA_SCHEMA,
            "layers_published_schema": settings.PANGEA_LAYERS_PUBLISHED_SCHEMA,
            "table_name": table_name,
            "geocod": geocod,
            "topo_geom_column_name": topo_geom_column_name,
            "zoom_min": zoom_min,
            "zoom_max": zoom_max,
            "columns": columns
        }
        _pre_process_basic_territorial_level_layer(params)
        layer_status = {'layer': layer,
                        'status': LayerStatus.Status.LAYER_PRE_PROCESSED}
        LayerStatus.objects.create(**layer_status)

        return {'result': "Success"}
    except Exception as e:
        if e.orig.pgcode == '42P07':
            return {'error': 'This action has been executed!'}
        return {'error': e.orig.pgerror}

def pre_process_composed_territorial_level_layer(layer):
    base_layer = layer.is_a_composition_of
    if base_layer.status < LayerStatus.Status.TOPOLOGY_CREATED:
        return {"error": "Before this action you must create a topology in a base layer: %s!" % layer.name}
    if layer.status >= LayerStatus.Status.LAYER_PRE_PROCESSED:
        return {'error': 'This action has been executed!'}

    colunms = []
    colunms_group_by = []
    for column in layer.column_set.all():
        colunms.append('%s.%s as %s' % (layer.table_name, column.name, column.alias))
        colunms_group_by.append('%s.%s' % (layer.table_name, column.name))

    columns = ', '.join(colunms)
    colunms_group_by = ', '.join(colunms_group_by)
   
    zoom_min = layer.zoom_min.zoom_level
    zoom_max = layer.zoom_max.zoom_level
    geocod = layer.geocod_column
    if not zoom_min or not zoom_max or not geocod:
        return {"error": "Before this action you must define zoom parameters and geocod_column!"}
    try:
        params = {
            "imported_data_schema": settings.PANGEA_IMPORTED_DATA_SCHEMA,
            "layers_published_schema": settings.PANGEA_LAYERS_PUBLISHED_SCHEMA,
            "table_name": layer.table_name,
            "geocod": geocod,
            "composition_column": layer.composition_column,
            "columns": columns,
            "colunms_group_by":colunms_group_by,

            "base_table_name": base_layer.table_name,
            "topo_geom_column_name": base_layer.topo_geom_column_name,
            "base_geocod": base_layer.geocod_column,

            "zoom_min": zoom_min,
            "zoom_max": zoom_max,
        }
        _pre_process_composed_territorial_level_layer(params)
        layer_status = {'layer': layer,
                        'status': LayerStatus.Status.LAYER_PRE_PROCESSED}
        LayerStatus.objects.create(**layer_status)

        return {'result': "Success"}
    except Exception as e:
        if e.orig.pgcode == '42P07':
            return {'error': 'This action has been executed!'}
        return {'error': e.orig.pgerror}

def pre_process_choroplethlayer_level_layer(layer):

    if layer.status >= LayerStatus.Status.LAYER_PRE_PROCESSED:
        return {'error': 'This action has been executed!'}


    base_layer = layer.layer
    if base_layer.status < LayerStatus.Status.TOPOLOGY_CREATED:
        return {"error": "Before this action you must create a topology in a base layer: %s!" % layer.name}
    if base_layer.status < LayerStatus.Status.LAYER_PRE_PROCESSED:
        return {'error': 'Before this action you must preprocess a base layer: %s!'}        

    colunms = []
    for column in layer.column_set.all():
        colunms.append('%s.%s as %s' % (layer.table_name, column.name, column.alias))
    columns = ', '.join(colunms)

    base_colunms = []
    for column in base_layer.column_set.all():
        base_colunms.append('%s.%s as %s' % (base_layer.table_name, column.name, column.alias))
    base_colunms = ', '.join(base_colunms)    
   

    geocod = layer.geocod_column
    if not geocod:
        return {"error": "Before this action you must define geocod_column!"}
    try:
        params = {
            "layers_published_schema": settings.PANGEA_LAYERS_PUBLISHED_SCHEMA,
            "imported_data_schema": settings.PANGEA_IMPORTED_DATA_SCHEMA,

            "table_name": layer.table_name,
            "geocod": geocod,
            "columns": columns,

            "base_table_name": base_layer.table_name,
            "base_colunms": base_colunms,
            "base_geocod": base_layer.geocod_column,
        }
        _pre_process_choroplethlayer_level_layer(params)
        layer_status = {'layer': layer,
                        'status': LayerStatus.Status.LAYER_PRE_PROCESSED}
        LayerStatus.objects.create(**layer_status)

        return {'result': "Success"}
    except Exception as e:
        if e.orig.pgcode == '42P07':
            return {'error': 'This action has been executed!'}
        return {'error': e.orig.pgerror}    