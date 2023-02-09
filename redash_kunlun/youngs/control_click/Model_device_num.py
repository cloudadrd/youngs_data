from clickhouse_driver import Client
from sqlalchemy import create_engine
import pandas as pd
from ck_util import clickhouse_util
import datetime


def execute_sql(date,client):
    sql_str="select '{date}' as date,arrayElement(key,1) as country,(case when arrayElement(key,2)='android' then 1 else 2 end) as platform,arrayElement(key,3) as pkg_name,num from (select splitByChar('_',key) as key,count(*) as num from new_model_data_20211118 group by key)ck;"\
        .format(date=date)

    result = client.execute(sql_str)
    df = pd.DataFrame(list(result),columns=['date','country','platform','pkg_name','device_num'])
    print(df)

    yconnect = create_engine('mysql+pymysql://db_app:Vee8lie3aiNee9sa@php-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com:3306/new_ssp?')
    pd.io.sql.to_sql(df, 'cloudmobi_model_setting', yconnect, schema='new_ssp', if_exists='append',index=False)

if __name__ == '__main__':

    clickhouse_client = clickhouse_util()
    client=clickhouse_client.return_client()
    date = str((datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y%m%d'))
    execute_sql(date,client)
