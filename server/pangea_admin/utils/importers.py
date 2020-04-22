import os
import pandas as pd
from sqlalchemy import create_engine
from .ogr2ogr import main
from zipfile import ZipFile
from pangea.settings import TEMP_DIR
import shutil

geo_formats = ['.kml', '.kmz', '.shp', '.geojson']

import random
import string

def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


def import_csv_2_pg(csv_file, pg_uri, schema, table, pandas_params):
    try:
        df = pd.read_csv(csv_file, **pandas_params)
        engine = create_engine(pg_uri)
        result = df.to_sql(table, con=engine, schema=schema,if_exists='replace')
        return True
    except Exception as e:
        raise(e)

 
def import_ogr_2_pg(ogr_file, pg_uri_ogr_style, schema, table, ogr_params):
    _, file_extension = os.path.splitext(ogr_file)
    tmp_dir = os.path.join(TEMP_DIR, randomString())
    new_ogr_file_path = None
    if(file_extension.lower() == '.zip'):
        with ZipFile(ogr_file, 'r') as zipObj:
            zipObj.extractall(tmp_dir)
            # r=root, d=directories, f = files
            for r, d, f in os.walk(tmp_dir):
                for _file in f:
                    _, _file_extension = os.path.splitext(_file)
                    if _file_extension in geo_formats:
                        new_ogr_file_path = os.path.join(r, _file)
                        break
        if new_ogr_file_path:
            ogr_file = new_ogr_file_path
        else:
            shutil.rmtree(tmp_dir)
            raise Exception('The ziped file must contains a file in one of this formats: {0}'.format(','.join(geo_formats)))
    srid = ogr_params['srid']
    encoding = ogr_params['encoding']
    result = main(['', '-nlt', 'PROMOTE_TO_MULTI', '-lco', 'OVERWRITE=YES', '-lco', 'SCHEMA={0}'.format(schema), '-f', 'PostgreSQL', pg_uri_ogr_style, ogr_file, '-nln', table, '-t_srs', 'EPSG:{0}'.format(srid)])
    shutil.rmtree(tmp_dir)
    return result