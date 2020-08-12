from django.conf import settings
from django.http import JsonResponse, HttpResponse
from django.contrib.auth.decorators import login_required

import gzip

from .utils.database_information import get_schemas, get_tables, get_colunms,\
    _create_topology, _create_layer_topology, _drop_table, _has_topology,\
    _populate_topology, _drop_topology, _get_layers, get_mvt, get_mvt_whithout_topology, get_bbox,\
    get_label_whithout_topology, get_label_mvt
from .utils.preprocessor import pre_process_basic_territorial_level_layer, pre_process_composed_territorial_level_layer, pre_process_choroplethlayer_level_layer
from .utils.utils import query_params_processor

from .models import LayerStatus, Layer, BasicTerritorialLevelLayer, ComposedTerritorialLevelLayer, ChoroplethLayer


@login_required
def _get_tables(request):
    response = get_tables(settings.PANGEA_IMPORTED_DATA_SCHEMA)
    return JsonResponse(response, safe=False)


@login_required
def _get_geo_tables(request):
    response = get_tables(settings.PANGEA_IMPORTED_DATA_SCHEMA, True)
    return JsonResponse(response, safe=False)


@login_required
def _get_colunms(request, table):
    response = get_colunms(settings.PANGEA_IMPORTED_DATA_SCHEMA, table)
    return JsonResponse(response, safe=False)


@login_required
def create_topology(request, layer_id):
    topology_id, topology_name = '-1', 'erro'
    layers = BasicTerritorialLevelLayer.objects.filter(id=layer_id)
    if len(layers) == 1:
        layer = layers[0]
        topology_name = 'topology_{0}'.format(layer.name)
        topo_geom_column_name = 'topo_{0}'.format(layer.geom_column)
        
        if layer.force_whithout_topology:
            return JsonResponse({"message": "This layer is marked to not create topology."}, safe=False)

        if layer.status >= LayerStatus.Status.TOPOLOGY_CREATED:
            return JsonResponse({"message": "This layer already has a topology"}, safe=False)
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
            layer.topology_name = None
            layer.topology_layer_id = None
            layer.topo_geom_column_name = None
            layer.save()
            _drop_topology(topology_name)
            return JsonResponse({"error": e}, safe=False)
    else:
        layers = Layer.objects.filter(id=layer_id)
        if len(layers) == 1:
            return JsonResponse({"message": "It's not necessary create a topology for this kind of layer!"}, safe=False)
        return JsonResponse({"message": "Layer not Found"}, safe=False)

@login_required
def pre_process_layer(request, layer_id):
    layers = Layer.objects.filter(id=layer_id)
    response = None
    if len(layers) == 1:
        layer = layers[0]
        if hasattr(layer, 'basicterritoriallevellayer'):
            layer = layer.basicterritoriallevellayer
            response = pre_process_basic_territorial_level_layer(layer)
            
        elif hasattr(layer, 'composedterritoriallevellayer'):
            layer = layer.composedterritoriallevellayer
            response = pre_process_composed_territorial_level_layer(layer)

        elif hasattr(layer, 'choroplethlayer'):
            layer = layer.choroplethlayer
            response = pre_process_choroplethlayer_level_layer(layer)
    else:
        response = {"message": "Layer not Found"}
    return JsonResponse(response, safe=False)



@login_required
def undo_process(request, layer_id):
    layers = Layer.objects.filter(id=layer_id) 
    response = 'None'
    if len(layers) == 1:
        layer = layers[0]
        choropleth = ChoroplethLayer.objects.filter(layer=layer.id)
        composed = ComposedTerritorialLevelLayer.objects.filter(is_a_composition_of=layer.id)
        dependents = [{i.id:i.name } for i in choropleth if i.status >= LayerStatus.Status.LAYER_PRE_PROCESSED] +\
                [ {i.id: i.name} for i in composed if i.status >= LayerStatus.Status.LAYER_PRE_PROCESSED]
        if ( dependents ):
            return JsonResponse({'message':'First off all, you must delete these layers', 'layers': dependents}, safe=False)
        elif (layer.status < LayerStatus.Status.LAYER_PRE_PROCESSED):
            return JsonResponse({'message':"There is nothing to do. This Layer isn't processed yet."}, safe=False)
        else:

            _drop_table(settings.PANGEA_LAYERS_PUBLISHED_SCHEMA, layer.table_name)

            if _has_topology(layer.name):
                _drop_topology(layer.name)

            LayerStatus.objects.filter(layer=layer, status__gte=LayerStatus.Status.LAYER_PRE_PROCESSED).delete() 
            response = {"message": "Undo process complete"}           
    else:
        response = {"message": "Layer not Found"}
    return JsonResponse(response, safe=False)


@login_required
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
    scheme = request.is_secure() and "https" or "http"
    host = f'{scheme}://{request.get_host()}/tile/'    
    result = _get_layers(host)
    return JsonResponse(result, safe=False)


def mvt(request, layer_name, z, x, y):
    def force_whithout_topology(layer):
        if layer.force_whithout_topology:
            return True
        elif hasattr(layer, 'choroplethlayer'):
            layer = layer.choroplethlayer
            return layer.layer.force_whithout_topology
        else:
            return False

    layers = Layer.objects.filter(name=layer_name)
    if len(layers) == 1:
        layer = layers[0]        
        if layer.status == 8:
            looking_for = query_params_processor(settings.PANGEA_LAYERS_PUBLISHED_SCHEMA,
                                        layer.table_name, 
                                        request.GET)
            z_min = layer.zoom_min.zoom_level 
            z_max = layer.zoom_max.zoom_level
            zoom_level = z if z_min <= int(z) and int(z) <= z_max else z_min if z_min > int(z) else z_max
            params = {
                "layer_name": layer.name,
                "geocod": layer.geocod_column,
                "z": z,
                "x": x,
                "y": y,
                "fields": layer.fields + ',' if len(layer.fields) > 0 else '',
                "table_name": layer.table_name,
                "schema_name": settings.PANGEA_LAYERS_PUBLISHED_SCHEMA,
                "zoom_level": zoom_level,
                'looking_for': looking_for
            }
            if force_whithout_topology(layer):
                result = get_mvt_whithout_topology(params)
            else:
                result = get_mvt(params)
            response = HttpResponse(gzip.compress(
                result), content_type='application/x-protobuf')
            response['Content-Encoding'] = 'gzip'
            return response
    response = HttpResponse(gzip.compress(b''), content_type='application/x-protobuf')
    response['Content-Encoding'] = 'gzip'
    return response    


def bbox(request, layer_name):
    def force_whithout_topology(layer):
        if layer.force_whithout_topology:
            return True
        elif hasattr(layer, 'choroplethlayer'):
            layer = layer.choroplethlayer
            return layer.layer.force_whithout_topology
        else:
            return False

    layers = Layer.objects.filter(name=layer_name)
    if len(layers) == 1:
        layer = layers[0]        
        if layer.status == 8:
            looking_for = query_params_processor(settings.PANGEA_LAYERS_PUBLISHED_SCHEMA,
                                        layer.table_name, 
                                        request.GET, False)
            looking_for
            params = {
                "layer_name": layer.name,
                "table_name": layer.table_name,
                "schema_name": settings.PANGEA_LAYERS_PUBLISHED_SCHEMA,
                'looking_for': looking_for
            }
            result = get_bbox(params)           
            return JsonResponse(result, safe=False)
    return JsonResponse(None, safe=False)    



def label(request, layer_name, z, x, y):
    def force_whithout_topology(layer):
        if layer.force_whithout_topology:
            return True
        elif hasattr(layer, 'choroplethlayer'):
            layer = layer.choroplethlayer
            return layer.layer.force_whithout_topology
        else:
            return False

    layers = Layer.objects.filter(name=layer_name)
    if len(layers) == 1:
        layer = layers[0]        
        if layer.status == 8:
            looking_for = query_params_processor(settings.PANGEA_LAYERS_PUBLISHED_SCHEMA,
                                        layer.table_name, 
                                        request.GET)
            bbox = request.GET.get('bbox')
            if not bbox:
                bbox = '-179.99,-89.99,179.99,89.99'
            z_min = layer.zoom_min.zoom_level 
            z_max = layer.zoom_max.zoom_level
            zoom_level = z if z_min <= int(z) and int(z) <= z_max else z_min if z_min > int(z) else z_max
            params = {
                "layer_name": layer.name,
                "geocod": layer.geocod_column,
                "z": z,
                "x": x,
                "y": y,
                "fields": layer.fields + ',' if len(layer.fields) > 0 else '',
                "table_name": layer.table_name,
                "schema_name": settings.PANGEA_LAYERS_PUBLISHED_SCHEMA,
                "zoom_level": zoom_level,
                'looking_for': looking_for,
                'bbox': bbox
            }
            if force_whithout_topology(layer):
                result = get_label_whithout_topology(params)
            else:
                result = get_label_mvt(params)
            response = HttpResponse(gzip.compress(
                result), content_type='application/x-protobuf')
            response['Content-Encoding'] = 'gzip'
            return response
    response = HttpResponse(gzip.compress(b''), content_type='application/x-protobuf')
    response['Content-Encoding'] = 'gzip'
    return response    

