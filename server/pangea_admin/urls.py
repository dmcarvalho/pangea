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


# Wire up our API using automatic URL routing.
# Additionally, we include login URLs for the browsable API.
urlpatterns = [
    path('', include(router.urls)),
    path(r'api-auth/', include('rest_framework.urls', namespace='rest_framework')),
    path('tables/', views_aux._get_tables),
    path('geotables/', views_aux._get_geo_tables) ,
    path('<table>/columns/', views_aux._get_colunms),
    path('createtopology/<layer_id>/', views_aux.create_topology),
    path('preprocesslayer/<layer_id>/', views_aux.pre_process_layer),
    path('publishlayer/<layer_id>/', views_aux.publish_layer),


]   
