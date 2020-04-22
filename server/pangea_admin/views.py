from rest_framework import viewsets
from rest_framework import permissions

from django.contrib.auth.models import User, Group, Permission
from .serializers import UserSerializer, GroupSerializer, PermissionSerializer

from .models import LayerStatus, Column, BasicTerritorialLevelLayer, ComposedTerritorialLevelLayer, ChoroplethLayer

from .serializers import LayerStatusSerializer, ColumnSerializer, BasicTerritorialLevelLayerSerializer, ComposedTerritorialLevelLayerSerializer, ChoroplethLayerSerializer


class UserViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows users to be viewed or edited.
    """
    queryset = User.objects.all().order_by('-date_joined')
    serializer_class = UserSerializer
    permission_classes = [permissions.IsAuthenticated]


class GroupViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows groups to be viewed or edited.
    """
    queryset = Group.objects.all()
    serializer_class = GroupSerializer
    permission_classes = [permissions.IsAdminUser]


class PermissionViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows groups to be viewed or edited.
    """
    queryset = Permission.objects.all()
    serializer_class = PermissionSerializer
    permission_classes = [permissions.IsAdminUser]


class LayerStatusViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows groups to be viewed or edited.
    """
    queryset = LayerStatus.objects.all()
    serializer_class = LayerStatusSerializer
    permission_classes = [permissions.IsAdminUser]

class ColumnViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows groups to be viewed or edited.
    """
    queryset = Column.objects.all()
    serializer_class = ColumnSerializer
    permission_classes = [permissions.IsAdminUser, permissions.DjangoModelPermissions]


class BasicTerritorialLevelLayerViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows groups to be viewed or edited.
    """
    queryset = BasicTerritorialLevelLayer.objects.all()
    serializer_class = BasicTerritorialLevelLayerSerializer
    permission_classes = [permissions.IsAdminUser, permissions.DjangoModelPermissions]


class ComposedTerritorialLevelLayerViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows groups to be viewed or edited.
    """
    queryset = ComposedTerritorialLevelLayer.objects.all()
    serializer_class = ComposedTerritorialLevelLayerSerializer
    permission_classes = [permissions.IsAdminUser, permissions.DjangoModelPermissions]
    

class ChoroplethLayerViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows groups to be viewed or edited.
    """
    queryset = ChoroplethLayer.objects.all()
    serializer_class = ChoroplethLayerSerializer
    permission_classes = [permissions.IsAdminUser, permissions.DjangoModelPermissions]

