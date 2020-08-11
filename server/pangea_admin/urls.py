from django.urls import include, path
from rest_framework import routers
from . import views, views_aux


router = routers.DefaultRouter()
router.register(r'users', views.UserViewSet)
router.register(r'groups', views.GroupViewSet)
router.register(r'permissions', views.PermissionViewSet)

router.register(r'columns', views.ColumnViewSet)
router.register(r'layerstatus', views.LayerStatusViewSet)

router.register(r'basicterritoriallevel', views.BasicTerritorialLevelLayerViewSet)
router.register(r'composedbasicterritoriallevel', views.ComposedTerritorialLevelLayerViewSet)
router.register(r'choroplethlayer', views.ChoroplethLayerViewSet)



# Wire up our API using automatic URL routing.
# Additionally, we include login URLs for the browsable API.
urlpatterns = [
    path(r'api/', include(router.urls)),
    path(r'api-auth/', include('rest_framework.urls', namespace='rest_framework')),
    path('api/tables/', views_aux._get_tables),
    path('api/geotables/', views_aux._get_geo_tables) ,
    path('api/<table>/columns/', views_aux._get_colunms),
    path('api/createtopology/<layer_id>/', views_aux.create_topology),
    path('api/preprocesslayer/<layer_id>/', views_aux.pre_process_layer),
    path('api/publishlayer/<layer_id>/', views_aux.publish_layer),
    path('api/undoprocess/<layer_id>/', views_aux.undo_process),
    path('', views_aux.get_layers),
    path('tile/<layer_name>/<z>/<x>/<y>.mvt', views_aux.mvt),
    path('bbox/<layer_name>', views_aux.bbox),
    path('label/<layer_name>/<z>/<x>/<y>.mvt', views_aux.label),




]   
