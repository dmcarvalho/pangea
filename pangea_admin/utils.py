import pandas as pd
from sqlalchemy import create_engine


def import_csv_2_pg(csv_byteIO, pg_uri, schema, table, pandas_params):
    df = pd.read_csv(csv_byteIO, **pandas_params)
    engine = create_engine(pg_uri)
    result = df.to_sql(table, con=engine, schema=schema,if_exists='replace')
    csv_byteIO.close()

def import_ogr_2_pg(ogr_file, pg_uri_ogr_style, table, ogr_params):
    pass