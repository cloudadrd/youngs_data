import pandas as pd
import psycopg2
import os
from sqlalchemy import create_engine
import configparser
import datetime
import pymysql
import redis

from config import REALTIME_CONFIG, CRAWDB_CONFIG


def select_data(time_slot):
    con = psycopg2.connect(**REALTIME_CONFIG)
    sql = """
            select SUBSTRING(time_slot,0,9) as date,SUBSTRING(time_slot,9,10) as hour,channel,offer,slot,country,platform,pkg,sum(count) as conversion,0 as impression,0 as click from conversion where time_slot='{time_slot}' group by channel,offer,slot,country,platform,pkg,SUBSTRING(time_slot,0,9) ,SUBSTRING(time_slot,9,10)
        """.format(time_slot=time_slot)
    realtime_df = pd.read_sql(sql, con)

    return realtime_df


def select_af_data():
    con = pymysql.connect(**CRAWDB_CONFIG)
    sql = """
        select id,ssp_offer_id as offer,channel,slot,sum(payout) as payout from aff_offer group by id,ssp_offer_id,channel

    """
    af_df = pd.read_sql(sql, con)
    return af_df


def merge_df(time_slot):
    realtime_df = select_data(time_slot=time_slot)
    af_df = select_af_data()
    result_df = pd.merge(af_df, realtime_df, how='inner', on=['offer', 'slot', 'channel'])
    filter_df = ['date', 'hour','country','platform','pkg','id','impression','click','conversion','payout']
    result_df = result_df[filter_df]
    result_df['date']=result_df['date'].apply(lambda x:datetime.datetime.strptime(x,'%Y%m%d').strftime('%Y-%m-%d'))
    result_df['revenue']=result_df['conversion']*result_df['payout']
    del result_df['payout']
    result_df=result_df.rename(columns={'id':'offer'})
    return result_df

def sink_to_mysql(result_df):
    yconnect = create_engine('mysql+pymysql://db_app:Vee8lie3aiNee9sa@php-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com:3306/new_ssp?')
    print("*****")
    print(result_df)
    pd.io.sql.to_sql(result_df, 'cloudmobi_af_cap_report', yconnect, schema='new_ssp', if_exists='append',index=False)
def connect_redis(key, value):
    r = redis.Redis(host='freq-01.fhjqtp.0001.apse1.cache.amazonaws.com', port=6379, decode_responses=True)
    r.set(key, value)  # 设置 name 对应的值
    print(key, r.get(key))


if __name__ == '__main__':
    time_slot = str((datetime.datetime.today() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H'))
    result_df=merge_df(time_slot)
    sink_to_mysql(result_df)


