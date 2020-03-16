from django.contrib.auth.models import User, Group, Permission
from rest_framework import serializers
from .models import ImportedTabularData, ImportedGeographicData, DataStatus, GeneralizationParams, LayerMetadata, Column
from pangea.settings import PANGEA_DB_URI, PANGEA_DB_URI_OGR_STYLE, PANGEA_IMPORTED_DATA_SCHEMA
from .utils.importers import import_csv_2_pg, import_ogr_2_pg
import copy
GENERIC_ERROR = "An error occurred while processing your request. Maybe, it can help you: %s"

class PermissionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Permission
        fields = ['id', 'url', 'name']

class GroupSerializer(serializers.ModelSerializer):
    class Meta:
        model = Group
        fields = ['url', 'name']

class UserSerializer(serializers.ModelSerializer):
    user_permissions = PermissionSerializer(many=True, read_only=True)
    groups = GroupSerializer(many=True, read_only=True)
    class Meta:
        model = User
        fields = ['id', 'url', 'username', 'email', 'groups', 'user_permissions']


class DataStatusSerializer(serializers.ModelSerializer):
    class Meta:
        model = DataStatus
        fields = ['id', 'url', 'imported_data', 'status', 'date']


class ImportedTabularDataSerializer(serializers.ModelSerializer):
    datastatus_set = DataStatusSerializer(many=True, read_only=True)

    class Meta:
        model = ImportedTabularData
        fields = ['id', 'url', 'file_path', 'table_name', 'description', 'metadata_url', 'encoding', 'delimiter', 'quotechar', 'decimal', 'datastatus_set']

    def create(self, validated_data):
        pandas_params = {
            "encoding": validated_data['encoding'],
            "delimiter": validated_data['delimiter'],
            "decimal": validated_data['decimal']
        }
        if 'quotechar' in validated_data['quotechar'] :
            pandas_params["quotechar"] = validated_data['quotechar']
            pandas_params["quoting"] = 0 #QUOTE_MINIMAL

        csv_byteIO = copy.copy(validated_data['file_path'].file)
        table_name = validated_data['table_name']
        try:            
            import_csv_2_pg(csv_byteIO, PANGEA_DB_URI, PANGEA_IMPORTED_DATA_SCHEMA, table_name, pandas_params)
        except Exception as e:
            raise serializers.ValidationError(GENERIC_ERROR % e)
        imported_data = ImportedTabularData.objects.create(**validated_data)
        data_status = {'imported_data': imported_data,
                        'status': DataStatus.Status.IMPORTED}
        DataStatus.objects.create(**data_status)
        return imported_data


class ImportedGeographicDataSerializer(serializers.ModelSerializer):
    datastatus_set = DataStatusSerializer(many=True, read_only=True)

    class Meta:
        model = ImportedGeographicData
        fields = ['id', 'url', 'file_path', 'table_name', 'description', 'metadata_url', 'encoding', 'srid', 'datastatus_set']

    def create(self, validated_data):
        ogr_params = {
            "encoding": validated_data['encoding'],
            "srid": validated_data['srid']
        }
        table_name = validated_data['table_name']
        try:     
            imported_data = ImportedGeographicData.objects.create(**validated_data)
            data_status = {'imported_data': imported_data,'status': DataStatus.Status.IMPORTED}
            DataStatus.objects.create(**data_status)
            ok = import_ogr_2_pg(imported_data.file_path.path, PANGEA_DB_URI_OGR_STYLE, PANGEA_IMPORTED_DATA_SCHEMA, table_name, ogr_params)
            if not ok:
                raise Exception ('Ocorreu um erro')
        except Exception as e:
            imported_data.delete()
            raise serializers.ValidationError(GENERIC_ERROR % e)

        return imported_data


class GeneralizationParamsSerializer(serializers.ModelSerializer):
    class Meta:
        model = GeneralizationParams
        fields = ['zoom_level', 'url', 'factor']


class LayerMetadataSerializer(serializers.ModelSerializer):
    class Meta:
        model = LayerMetadata
        fields = ['id', 'url', 'layer_name', 'data_imported', 'schema_name', 
        'table_name', 'geocod_column', 'dimension_column', 
        'topo_geom_column', 'is_a_composition_of', 
        'composition_column', 'zoom_min', 'zoom_max']
        

class ColumnSerializer(serializers.ModelSerializer):
    class Meta:
        model = Column
        fields = ['id', 'url', 'layer_metadata', 'schema_name', 'table_name', 
        'name', 'alias']