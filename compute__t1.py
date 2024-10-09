from core import Pipeline

import pandas as pd
import requests
import datetime
def main(t0):
    t0['META_UPDATE_TIMESTAMP']= str(datetime.datetime.now())
    return t0

p=Pipeline('sample.xml')

t0 = [i.get_dataframe() for i in p.tables if i.id == 't0'][0]

t1 = main(t0)

curr_table=[i for i in p.tables if i.id=='t1'][0]
 


[i.connection for i in p.tables if i.id == 't1'][0].Session()

curr_table.connection.df_to_table(t1, curr_table.table, curr_table.database, curr_table.schema, curr_table.materialization, schema_change_behavior=curr_table.schema_change, primary_key=curr_table.primary_key)