from django.http import JsonResponse
from django.contrib.auth.decorators import login_required

from .utils.database_information import get_schemas, get_tables, get_colunms,_create_topology, _create_layer_topology, _populate_topology, _drop_topology
from django.conf import settings
from .models import BasicTerritorialLevelLayer


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
        topology_name = '{0}_topology'.format(table_name)
        topo_geom_column_name = 'topo_{0}'.format(layer.geom_column)
    else:
        return JsonResponse({'error':'layer not found'}, safe=False)
    try:
        topology_name, topology_id = _create_topology(settings.PANGEA_DB_URI, layer.name, layer.srid)
        topology_layer_id = _create_layer_topology(settings.PANGEA_DB_URI, topology_name, layer.schema_name, layer.table_name, topo_geom_column_name, layer.geom_type)
        _populate_topology(settings.PANGEA_DB_URI, layer.schema_name, layer.table_name, topo_geom_column_name, layer.geom_column, topology_name, topology_layer_id)

        layer.topology_name = topology_name
        layer.topology_layer_id = topology_layer_id
        layer.topo_geom_column_name = topo_geom_column_name
        layer.save()
    except Exception as e:
        _drop_topology(settings.PANGEA_DB_URI, topology_name)
        print(e)

    return JsonResponse({topology_id:topology_name}, safe=False)
