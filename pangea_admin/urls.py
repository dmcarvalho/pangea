from django.conf import settings
from django.conf.urls.static import static

from django.urls import include, path
from rest_framework import routers
from . import views



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
    path('api-auth/', include('rest_framework.urls', namespace='rest_framework'))
] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)