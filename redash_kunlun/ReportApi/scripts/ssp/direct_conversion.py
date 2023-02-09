# -*- coding: utf-8 -*-

import pandas as pd
import psycopg2
import os
from sqlalchemy import create_engine
import configparser
from datetime import timezone,timedelta,datetime
import pymysql

import boto3

def get_data():
    realtime_config = {
        'host': 'cloudmobi-realtime.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com',
        'user': 'redash',
        'password': 'Redash2018',
        'database': 'monitor',
        'port': 5439
    }

    time_slot = str((datetime.today() - timedelta(hours=1)).strftime('%Y%m%d%H'))
    date = str((datetime.today() - timedelta(hours=1)).strftime('%Y%m%d'))

    print(date)
    con = psycopg2.connect(**realtime_config)
    sql="""
    select pb_channel,pb_channel_id,offer,to_char(update_time, 'YYYYMMDDHH24') as time_slot,clk_id from athena_realtime.direct_conversion where pdate='{a}' and  to_char(update_time, 'YYYYMMDDHH24')='{b}'
    """.format(a=date,b=time_slot)
    print(sql)
    direct_conversion_df = pd.read_sql(sql,con)
    print(direct_conversion_df)

    yconnect = create_engine(
        'postgresql+psycopg2://redash:Redash2018@cloudmobi-realtime.c3otanzuchnc.ap-southeast-1.redshift.amazonaws.com:5439/monitor?')
    pd.io.sql.to_sql(direct_conversion_df, 'pb_forward', yconnect, schema='public', if_exists='append', index=False)


def buy_flow_mask_partitions(time_slot):
        bucket_name = 'test-v4'
        client = boto3.client('athena', region_name='ap-southeast-1')
        config = {
            'OutputLocation': 's3://' + bucket_name + '/athena_output/',
            'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
        }
        sql = "alter table direct_conversion add  if not exists partition(pdate='%s') LOCATION 's3://cloud-flume/athena_log/direct-conversion/pdate=%s'" % (
        time_slot, time_slot)

        print(sql)
        context = {'Database': 'realtime'}

        client.start_query_execution(QueryString=sql,
                                     QueryExecutionContext=context,
                                     ResultConfiguration=config)

if __name__ == '__main__':
    t = datetime.now(timezone(timedelta(hours=18)))
    today = (t - timedelta(0)).strftime("%Y%m%d")
    get_data()
    buy_flow_mask_partitions(today)



