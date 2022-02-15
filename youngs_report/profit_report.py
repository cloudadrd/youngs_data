from clickhouse_driver import Client
from sqlalchemy import create_engine
import pandas as pd
from ck_util import clickhouse_util
import datetime
import pymysql
import re
import sys
from sqlalchemy import create_engine
import os
clickhouse_client = clickhouse_util()
client=clickhouse_client.return_client()

def execute_sql(day,day_format,client):
    sql_str="""
        select day,adtype,slot,country,user_id,channel,sum(revenue),sum(cost) from kunlun_report
        where day = '{day}' and (revenue != 0 or cost != 0) group by day,adtype,slot,country,user_id,channel
        """.format(day=day)
    print(sql_str)

    result = client.execute(sql_str)
    df = pd.DataFrame(list(result),columns=['date','adtype','slot','country','user_id','channel','revenue','cost'])
    df['user_id'] =  df['user_id'].astype('int')
    return df

def get_mysql():
    conn_mysql = pymysql.connect(
        host="php-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com",    # 数据库服务器主机地址
        user="db_app",  # 用户名
        password="Vee8lie3aiNee9sa", # 密码
        database="new_ssp", #数据库名称
        port=3306, # 端口号 可选 整型
        charset="utf8" # 编码  可选#
    )
    cur = conn_mysql.cursor()
    sql_kunlun = """
        select id,email ,username  from cloudmobi_user cu
        """
    cur.execute(sql_kunlun)
    #print(cur.fetchall())
    user_df = pd.DataFrame(list(cur.fetchall()),columns=['user_id','account_email','user_name'])
    #user_df['user_id'] = user_df['user_id'].astype('string')
    channel_type_sql = "select short_name ,channel_type  from cloudmobi_ad_network"
    cur.execute(channel_type_sql)
    channel_df =pd.DataFrame(list(cur.fetchall()),columns=['channel','channel_type'])
    merge_user_df = pd.merge(df, user_df, how = 'left',on=['user_id'])
    all_df = pd.merge(merge_user_df,channel_df, how = 'left' , on = ['channel'])
    all_df = all_df.loc[:,['date','adtype','slot','country','user_id','revenue','cost','channel_type','user_name','account_email']]
    all_df = all_df.groupby(['date','adtype','slot','country','user_id','channel_type','user_name','account_email'],axis=0,as_index=False).sum()
    conn = create_engine(
            'mysql+pymysql://db_app:Vee8lie3aiNee9sa@php-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com:3306/new_ssp?charset=utf8')

    pd.io.sql.to_sql(all_df, 'cloudmobi_profit_report', conn, schema='new_ssp', if_exists='append', index=False)



if __name__ == '__main__':

    clickhouse_client = clickhouse_util()
    client=clickhouse_client.return_client()
    ys_day = datetime.datetime.today() - datetime.timedelta(days=1)
    day = str(ys_day.strftime('%Y%m%d'))
    day_format = str(ys_day.strftime('%Y-%m-%d'))
    print(day_format)
    #time_slot = str((datetime.datetime.today() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H'))
    df=execute_sql(day,day_format,client)
    get_mysql()