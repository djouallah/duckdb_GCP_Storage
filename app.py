
import streamlit as st
import gcsfs
import duckdb,json
from google.oauth2 import service_account

try :

    con = duckdb.connect(database='db',read_only=True)
    df = con.execute('''
                       Select * from Import
                                     ''').fetch_df()

    st.write(df)
    con.close()

except :
 st.write("data refreshing")


################################################### Download Data from BigQuery#####################################################
# Retrieve and convert key file content.
key_json = json.loads(st.secrets["key"], strict=False)
GOOGLE_APPLICATION_CREDENTIALS = service_account.Credentials.from_service_account_info(key_json)

from pyarrow import parquet
fs = gcsfs.GCSFileSystem()

def Read_GCP() :
                 dx = parquet.ParquetDataset("gs://test_delta1/scada",
                            filesystem=fs, 
                            validate_schema=False, 
                           filters=[('Date','=', '2022-07-14')])
                 df = dx.read().to_pandas()
                 con = duckdb.connect(database='db')
                 con.execute("create or replace table Import as SELECT * FROM df").close()

Read_GCP()


