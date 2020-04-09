from django.http import JsonResponse
from django.contrib.auth.decorators import login_required

from .utils.database_information import get_schemas, get_tables, get_colunms
from django.conf import settings


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


