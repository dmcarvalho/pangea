from django.http import JsonResponse
from django.contrib.auth.decorators import login_required

from .utils.database_information import get_schemas, get_tables, get_colunms,\
    _create_topology, _create_layer_topology,\
    _populate_topology, _drop_topology, _pre_process_layer, _get_layers, get_mvt
from django.conf import settings
from .models import LayerStatus, BasicTerritorialLevelLayer


@login_required
def _get_tables(request):
    print(settings.PANGEA_DB_URI)
    print(settings.PANGEA_IMPORTED_DATA_SCHEMA)
    response = get_tables(settings.PANGEA_DB_URI, settings.PANGEA_IMPORTED_DATA_SCHEMA)
    return JsonResponse(response, safe=False)


@login_required
def _get_geo_tables(request):
    response = get_tables(settings.PANGEA_DB_URI, settings.PANGEA_IMPORTED_DATA_SCHEMA, True)
    return JsonResponse(response, safe=False)
    

@login_required
def _get_colunms(request, table):
    response = get_colunms(settings.PANGEA_DB_URI, settings.PANGEA_IMPORTED_DATA_SCHEMA, table)
    return JsonResponse(response, safe=False)


# @login_required
def create_topology(request, layer_id):
    topology_id, topology_name = '-1', 'erro'
    layers = BasicTerritorialLevelLayer.objects.filter(id=layer_id)
    if len(layers) == 1:
        layer = layers[0]
        topology_name = '{0}_topology'.format(layer.name)
        topo_geom_column_name = 'topo_{0}'.format(layer.geom_column)
        if layer.status >= LayerStatus.Status.TOPOLOGY_CREATED:
            return JsonResponse({"error": "This layer already has a topology"}, safe=False)
        try:
            topology_name, topology_id = _create_topology(settings.PANGEA_DB_URI, topology_name, layer.srid)
            topology_layer_id = _create_layer_topology(settings.PANGEA_DB_URI, topology_name, layer.schema_name, layer.table_name, topo_geom_column_name, layer.geom_type)
            _populate_topology(settings.PANGEA_DB_URI, layer.schema_name, layer.table_name, topo_geom_column_name, layer.geom_column, topology_name, topology_layer_id)

            layer.topology_name = topology_name
            layer.topology_layer_id = topology_layer_id
            layer.topo_geom_column_name = topo_geom_column_name
            layer.save()

            layer_status = {'layer': layer,
                           'status': LayerStatus.Status.TOPOLOGY_CREATED}
            LayerStatus.objects.create(**layer_status)    

            return JsonResponse({topology_id:topology_name}, safe=False)
        except Exception as e:
            layer.topology_name = ''
            layer.topology_layer_id = ''
            layer.topo_geom_column_name = ''
            layer.save()            
            _drop_topology(settings.PANGEA_DB_URI, topology_name)
            return JsonResponse({"error": e}, safe=False)
    else:
        return JsonResponse({"error": "Layer not Found"}, safe=False)


def pre_process_layer(request, layer_id):
    layers = BasicTerritorialLevelLayer.objects.filter(id=layer_id)
    if len(layers) == 1:
    
        layer = layers[0]
        if layer.status <  LayerStatus.Status.TOPOLOGY_CREATED:
            return JsonResponse({"error": "Before this action you must create a topology!"}, safe=False)
        if layer.status == LayerStatus.Status.LAYER_PRE_PROCESSED:
            return JsonResponse({'error': 'This action has been executed!'}, safe=False)
     
        zoom_min = layer.zoom_min
        zoom_max = layer.zoom_max
        geocod = layer.geocod_column
        geom_column = layer.topo_geom_column_name
        table_name = layer.table_name
        colunms = []
        for column in layer.column_set.all():
            colunms.append('%s as %s,' % (column.name, column.alias))
        columns = ' '.join(colunms)

        if not zoom_min or not zoom_max or not geocod:
            return JsonResponse({"error": "Before this action you must define zoom parameters and geocod_column!"}, safe=False)
        try:
            _pre_process_layer(settings.PANGEA_DB_URI,
                            settings.PANGEA_IMPORTED_DATA_SCHEMA,
                            settings.PANGEA_LAYERS_PUBLISHED_SCHEMA,
                            table_name, 
                            geocod, 
                            geom_column, 
                            zoom_min, 
                            zoom_max, 
                            columns)
            layer_status = {'layer': layer,
                           'status': LayerStatus.Status.LAYER_PRE_PROCESSED}
            LayerStatus.objects.create(**layer_status)    
                            
            return JsonResponse({'result':"Success"}, safe=False)
        except Exception as e:
            if e.orig.pgcode == '42P07':
                return JsonResponse({'error': 'This action has been executed!'}, safe=False)
            return JsonResponse({'error':e.orig.pgerror}, safe=False)
    else:
        return JsonResponse({"error": "Layer not Found"}, safe=False)


def publish_layer(request, layer_id):
    layers = BasicTerritorialLevelLayer.objects.filter(id=layer_id)
    if len(layers) == 1:
        layer = layers[0]
        if layer.status == LayerStatus.Status.LAYER_PUBLISHED:
            return JsonResponse({"error": "This action has been executed!"}, safe=False)
        if layer.status != LayerStatus.Status.LAYER_PRE_PROCESSED:
            return JsonResponse({"error": "Before this action you must preprocess this layer!"}, safe=False)
        layer_status = {'layer': layer,
                        'status': LayerStatus.Status.LAYER_PUBLISHED}
        LayerStatus.objects.create(**layer_status)
        return JsonResponse({'result':"Success"}, safe=False)


def get_layers(request):
    result = _get_layers(settings.PANGEA_DB_URI, 'http://kicahost/')
    return JsonResponse(result, safe=False)
from django.http import HttpResponse
import gzip


def mvt(request, layer_name, z, x, y):
    layers = BasicTerritorialLevelLayer.objects.filter(name=layer_name)
    if len(layers) == 1:
        layer = layers[0]
        if layer.status == 8:
            params =  {
                "layer_name": layer.name,
                "geocod": layer.geocod_column,
                "z": z,
                "x": x,
                "y": y,
                "fields": layer.fields,
                "table_name": layer.table_name,
                "geom": layer.topo_geom_column_name,
                "schema_name": settings.PANGEA_LAYERS_PUBLISHED_SCHEMA,
            }
            result = get_mvt(settings.PANGEA_DB_URI, params)
            response = HttpResponse(gzip.compress(result), content_type='application/x-protobuf')
            response['Content-Encoding'] = 'gzip'
            return response
