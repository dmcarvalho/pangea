from django.contrib.gis.db import models
from pangea import settings

# Create your models here.

from django.core.files.storage import FileSystemStorage

fs = FileSystemStorage(location=settings.MEDIA_ROOT)


class ImportedData(models.Model):
    file_path = models.FileField(storage=fs)
    table_name = models.CharField(max_length=200)
    encoding = models.CharField(max_length=50, default='utf8')   
    description = models.TextField(null=True, blank=True)
    metadata_url = models.URLField(null=True, blank=True)

    def __str__(self):
        return self.table_name



class ImportedGeographicData(ImportedData):
    srid = models.IntegerField(null=True, blank=True)


class ImportedTabularData(ImportedData):
    delimiter = models.CharField(max_length=1, default=',')
    quotechar = models.CharField(max_length=1, null=True, blank=True)
    decimal = models.CharField(max_length=1, default='.')



class DataStatus(models.Model):
    class Status(models.IntegerChoices):
        IMPORTED = 0
        TOPOLOGY_CREATED = 1
        LAYER_CREATED_WITHOUT_TOPOLOGY = 2
        LAYER_CREATED_WITH_TOPOLOGY = 3
        LAYER_PUBLISHED_WITHOUT_TOPOLOGY = 4
        LAYER_PUBLISHED_WITH_TOPOLOGY = 5

    imported_data = models.ForeignKey(ImportedData, on_delete=models.CASCADE)
    status = models.IntegerField(choices=Status.choices, default=0)
    date = models.DateTimeField(auto_now=True)


class GeneralizationParams(models.Model):
    zoom_level = models.IntegerField(primary_key=True)
    factor = models.FloatField()


class LayerMetadata(models.Model):
    layer_name = models.CharField(max_length=200)
    data_imported = models.ForeignKey(ImportedData, on_delete=models.CASCADE)
    schema_name =  models.CharField(max_length=200)
    table_name =  models.CharField(max_length=200)
    geocod_column =  models.CharField(max_length=200)
    dimension_column =  models.CharField(max_length=200)
    topo_geom_column =  models.CharField(max_length=200)

    is_a_composition_of = models.ForeignKey('self', on_delete=models.CASCADE)
    composition_column = models.CharField(max_length=200)
    zoom_min = models.ForeignKey(GeneralizationParams, on_delete=models.CASCADE, related_name='zoom_min')
    zoom_max = models.ForeignKey(GeneralizationParams, on_delete=models.CASCADE, related_name='zoom_max')


class Column(models.Model):
    layer_metadata = models.ForeignKey(LayerMetadata, on_delete=models.CASCADE)
    schema_name =  models.CharField(max_length=200)
    table_name =  models.CharField(max_length=200)    
    name = models.CharField(max_length=200)
    alias = models.CharField(max_length=200)