import pandas as pd
import psycopg2
import os
from sqlalchemy import create_engine
import configparser
import datetime
import pymysql
import redis

from config import REALTIME_CONFIG,CRAWDB_CONFIG


def select_data(time_slot):
    con = psycopg2.connect(**REALTIME_CONFIG)
    sql = """
     select channel,offer,slot,sum(count) as conv from conversion where time_slot>='{time_slot}' group by channel,offer,slot

        """.format(time_slot=time_slot)
    realtime_df = pd.read_sql(sql, con)

    return realtime_df


def select_af_data():
    con =pymysql.connect(**CRAWDB_CONFIG)
    sql ="""
        select id,ssp_offer_id as offer,channel,slot from aff_offer group by id,ssp_offer_id,channel

    """
    af_df = pd.read_sql(sql,con)
    return af_df
def merge_df(time_slot):
    realtime_df = select_data(time_slot=time_slot)
    af_df = select_af_data()
    result_df=pd.merge(af_df,realtime_df,how='left',on=['offer','slot','channel'])
    filter_df = ['id','conv']
    result_df=result_df[filter_df].dropna(axis=0)

    for i in zip(result_df['id'],result_df['conv']):
        redis_key=i[0]+"_fenix"
        redis_value=int(i[1])
        print(redis_key)
        connect_redis(redis_key,redis_value)

def connect_redis(key, value):
    r = redis.Redis(host='freq-01.fhjqtp.0001.apse1.cache.amazonaws.com', port=6379, decode_responses=True)
    r.set(key, value)  # 设置 name 对应的值
    print(key, r.get(key))

if __name__ == '__main__':
    time_slot = str((datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d%H'))
    merge_df(time_slot)
