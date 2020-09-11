from django.contrib.gis.db import models
from pangea import settings

# Create your models here.
from .utils.utils import generate_safe_name

from .utils.database_information import _drop_table, _has_topology, _drop_topology
import os

from django.core.files.storage import FileSystemStorage

fs = FileSystemStorage(location=settings.MEDIA_ROOT)


class GeneralizationParams(models.Model):
    zoom_level = models.IntegerField(primary_key=True)
    factor = models.FloatField()

    def __str__(self):
        return '%s - %s - meters' % (self.zoom_level, round(self.factor * 111319.490793, 2))


class Layer(models.Model):
    name = models.CharField(max_length=200, unique=True)    
    description = models.TextField(null=True, blank=True)
    metadata = models.URLField(null=True, blank=True)

    force_whithout_topology = models.BooleanField(default=False)

    _file = models.FileField(storage=fs)
    schema_name = models.CharField(max_length=200, null=True, blank=True)
    table_name = models.CharField(max_length=200, null=True, blank=True)
    geocod_column =  models.CharField(max_length=200, null=True, blank=True)

    encoding = models.CharField(max_length=50, default='utf8', null=True, blank=True)   

    zoom_min = models.ForeignKey(GeneralizationParams, on_delete=models.CASCADE, related_name='zoom_min', null=True, blank=True)
    zoom_max = models.ForeignKey(GeneralizationParams, on_delete=models.CASCADE, related_name='zoom_max', null=True, blank=True)
    
    def __str__(self):
        return self.name



    def delete(self, *args, **kwargs):
        try:

            layers = ChoroplethLayer.objects.filter(layer=self.id)
            for i in layers:
                i.delete()

            layers = ComposedTerritorialLevelLayer.objects.filter(is_a_composition_of=self.id)
            for i in layers:
                i.delete()

            if _has_topology(self.name):
                _drop_topology(self.name)
            
            if os.path.exists(self._file.path):
                os.remove(self._file.path)
            
            _ = _drop_table(self.schema_name, self.table_name)

            _ = _drop_table(settings.PANGEA_LAYERS_PUBLISHED_SCHEMA, self.table_name)
        except Exception as e:
            raise(e)
        super(Layer, self).delete(*args, **kwargs)  




    @property
    def status(self):
        status = self.layerstatus_set.all().latest('date')
        return status.status

    @property
    def fields(self):
        columns = self.column_set.all().values_list('alias', flat=True)
        return ', '.join(columns)

class Column(models.Model):
    layer = models.ForeignKey(Layer, on_delete=models.CASCADE)
    name = models.CharField(max_length=200)
    alias = models.CharField(max_length=200)

    def save(self, *args, **kwargs):
        if self.alias == '' or self.alias == None:
            self.alias = generate_safe_name(self.name)
        else:
            self.alias = generate_safe_name(self.alias)

        super(Column, self).save(*args, **kwargs)    


class LayerStatus(models.Model):
    class Status(models.IntegerChoices):
        IMPORTED = 1
        TOPOLOGY_CREATED = 2
        LAYER_PRE_PROCESSED = 4
        LAYER_PUBLISHED = 8
    layer = models.ForeignKey(Layer, on_delete=models.CASCADE)
    status = models.IntegerField(choices=Status.choices, default=0)
    date = models.DateTimeField(auto_now=True)


class BasicTerritorialLevelLayer(Layer):   
    srid = models.IntegerField()   
    dimension_column =  models.CharField(max_length=200, null=True, blank=True)
    geom_column =  models.CharField(max_length=200, null=True, blank=True)
    geom_type = models.CharField(max_length=200, null=True, blank=True)
    topology_name = models.CharField(max_length=200, null=True, blank=True)
    topology_layer_id = models.IntegerField(null=True, blank=True)
    topo_geom_column_name = models.CharField(max_length=200, null=True, blank=True)    


class ComposedTerritorialLevelLayer(Layer):
    is_a_composition_of = models.ForeignKey(BasicTerritorialLevelLayer, on_delete=models.CASCADE)

    delimiter = models.CharField(max_length=1, default=',')
    quotechar = models.CharField(max_length=1, null=True, blank=True)
    decimal = models.CharField(max_length=1, default='.')
    composition_column = models.CharField(max_length=200)


class ChoroplethLayer(Layer):
    layer = models.ForeignKey(Layer, on_delete=models.CASCADE, related_name='_base_layer')

    delimiter = models.CharField(max_length=1, default=',')
    quotechar = models.CharField(max_length=1, null=True, blank=True)
    decimal = models.CharField(max_length=1, default='.')
