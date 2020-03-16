from django.urls import include, path
from rest_framework import routers
from . import views, views_aux


router = routers.DefaultRouter()
router.register(r'users', views.UserViewSet)
router.register(r'groups', views.GroupViewSet)
router.register(r'permissions', views.PermissionViewSet)

router.register(r'importedtabulardata', views.ImportedTabularDataViewSet)
router.register(r'importedgeographicdata', views.ImportedGeographicDataViewSet)

router.register(r'datastatus', views.DataStatusViewSet)
router.register(r'generalizationparams', views.GeneralizationParamsViewSet)
router.register(r'layermetadata', views.LayerMetadataViewSet)
router.register(r'column', views.ColumnViewSet)



# Wire up our API using automatic URL routing.
# Additionally, we include login URLs for the browsable API.
urlpatterns = [
    path('', include(router.urls)),
    path(r'api-auth/', include('rest_framework.urls', namespace='rest_framework')),
    path('tables/', views_aux._get_tables),
    path('geotables/', views_aux._get_geo_tables) ,
    path('<table>/columns/', views_aux._get_colunms) 
] 



'''
    path(r'^getTables/$', 'spada.views.ajax_tabela_request'),
    path(r'^getGeoTables/$', 'spada.views.ajax_geo_tabela_request'),
    path(r'^getColumns/(?P<table>[\w\ ]*)/$', 'spada.views.ajax_coluna_request'),
    path(r'^geocolunas/(?P<esquema>\w*)/(?P<tabela>[\w\ ]*)/$', 'spada.views.ajax_geo_coluna_request'),
    path(r'^colunas_id/(?P<identificador>\d+)/$', 'spada.views.ajax_coluna_id_request'),
'''