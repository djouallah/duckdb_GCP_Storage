import streamlit as st
from  deltalake import DeltaTable
import os
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
os.environ["SERVICE_ACCOUNT"] = json.loads(st.secrets["key_GCP"], strict=False)

from pyarrow import parquet

@st.experimental_memo(ttl=4000)
def Read_GCP(path) :
                tb = DeltaTable(path).to_pyarrow_dataset()
                return tb
                 

scada=Read_GCP("gs://test_delta1/scada")
con = duckdb.connect(database='db')
con.execute("create or replace table Import as SELECT * FROM scada").close()
