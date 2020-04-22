from django.conf import settings
from django.http import JsonResponse, HttpResponse
from django.contrib.auth.decorators import login_required

import gzip

from .utils.database_information import get_schemas, get_tables, get_colunms,\
    _create_topology, _create_layer_topology,\
    _populate_topology, _drop_topology, _get_layers, get_mvt

from .utils.preprocessor import pre_process_basic_territorial_level_layer, pre_process_composed_territorial_level_layer, pre_process_choroplethlayer_level_layer
from .models import LayerStatus, Layer, BasicTerritorialLevelLayer, ComposedTerritorialLevelLayer


#@login_required
def _get_tables(request):
    response = get_tables(settings.PANGEA_IMPORTED_DATA_SCHEMA)
    return JsonResponse(response, safe=False)


#@login_required
def _get_geo_tables(request):
    response = get_tables(settings.PANGEA_IMPORTED_DATA_SCHEMA, True)
    return JsonResponse(response, safe=False)


#@login_required
def _get_colunms(request, table):
    response = get_colunms(settings.PANGEA_IMPORTED_DATA_SCHEMA, table)
    return JsonResponse(response, safe=False)


## @login_required
def create_topology(request, layer_id):
    topology_id, topology_name = '-1', 'erro'
    layers = BasicTerritorialLevelLayer.objects.filter(id=layer_id)
    if len(layers) == 1:
        layer = layers[0]
        topology_name = 'topology_{0}'.format(layer.name)
        topo_geom_column_name = 'topo_{0}'.format(layer.geom_column)
        if layer.status >= LayerStatus.Status.TOPOLOGY_CREATED:
            return JsonResponse({"error": "This layer already has a topology"}, safe=False)
        try:
            topology_name, topology_id = _create_topology(
                topology_name, layer.srid)

            params = {"topology_name": topology_name,
                      "imported_data_schema": layer.schema_name,
                      "table_name": layer.table_name,
                      "topo_geom_column_name": topo_geom_column_name,
                      "geom_type": layer.geom_type
                      }

            topology_layer_id = _create_layer_topology(params)

            params.update(
                {
                    "geom_column_name": layer.geom_column,
                    "topology_layer_id": topology_layer_id,
                }
            )
            _populate_topology(params)

            layer.topology_name = topology_name
            layer.topology_layer_id = topology_layer_id
            layer.topo_geom_column_name = topo_geom_column_name
            layer.save()

            layer_status = {'layer': layer,
                            'status': LayerStatus.Status.TOPOLOGY_CREATED}
            LayerStatus.objects.create(**layer_status)

            return JsonResponse({topology_id: topology_name}, safe=False)
        except Exception as e:
            layer.topology_name = ''
            layer.topology_layer_id = ''
            layer.topo_geom_column_name = ''
            layer.save()
            _drop_topology(topology_name)
            return JsonResponse({"error": e}, safe=False)
    else:
        layers = Layer.objects.filter(id=layer_id)
        if len(layers) == 1:
            return JsonResponse({"error": "It's not necessary create a topology for this kind of layer!"}, safe=False)
        return JsonResponse({"error": "Layer not Found"}, safe=False)


def pre_process_layer(request, layer_id):
    layers = Layer.objects.filter(id=layer_id)
    response = None
    if len(layers) == 1:
        layer = layers[0]
        if hasattr(layer, 'composedterritoriallevellayer'):
            layer = layer.composedterritoriallevellayer
            response = pre_process_composed_territorial_level_layer(layer)

        elif hasattr(layer, 'basicterritoriallevellayer'):
            layer = layer.basicterritoriallevellayer
            response = pre_process_basic_territorial_level_layer(layer)
        elif hasattr(layer, 'choroplethlayer'):
            layer = layer.choroplethlayer
            response = pre_process_choroplethlayer_level_layer(layer)
    else:
        response = {"error": "Layer not Found"}
    return JsonResponse(response, safe=False)


def publish_layer(request, layer_id):
    layers = Layer.objects.filter(id=layer_id)
    if len(layers) == 1:
        layer = layers[0]
        if layer.status == LayerStatus.Status.LAYER_PUBLISHED:
            return JsonResponse({"error": "This action has been executed!"}, safe=False)
        if layer.status != LayerStatus.Status.LAYER_PRE_PROCESSED:
            return JsonResponse({"error": "Before this action you must preprocess this layer!"}, safe=False)
        layer_status = {'layer': layer,
                        'status': LayerStatus.Status.LAYER_PUBLISHED}
        LayerStatus.objects.create(**layer_status)
        return JsonResponse({'result': "Success"}, safe=False)
    else:
        return JsonResponse({"error": "Layer not Found"}, safe=False)


def get_layers(request):
    result = _get_layers('http://kicahost/')
    return JsonResponse(result, safe=False)


def mvt(request, layer_name, z, x, y):
    layers = Layer.objects.filter(name=layer_name)
    if len(layers) == 1:
        layer = layers[0]        
        if layer.status == 8:
            if layer.zoom_min.zoom_level <= int(z) and int(z) <= layer.zoom_max.zoom_level:
                params = {
                    "layer_name": layer.name,
                    "geocod": layer.geocod_column,
                    "z": z,
                    "x": x,
                    "y": y,
                    "fields": layer.fields + ',' if len(layer.fields) > 0 else '',
                    "table_name": layer.table_name,
                    "schema_name": settings.PANGEA_LAYERS_PUBLISHED_SCHEMA,
                }
                result = get_mvt(params)
                response = HttpResponse(gzip.compress(
                    result), content_type='application/x-protobuf')
                response['Content-Encoding'] = 'gzip'
                return response
    response = HttpResponse(gzip.compress(b''), content_type='application/x-protobuf')
    response['Content-Encoding'] = 'gzip'
    return response    