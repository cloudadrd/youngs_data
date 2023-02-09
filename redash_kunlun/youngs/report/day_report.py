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
    SELECT a.*,b.cost FROM
    (select substring(time_slot,1,8) day ,channel_type ,channel
    ,case when lower(platform) = 'ios' then 2 ELSE 1 END platform
    ,country,offer_pkg
    ,sum(CASE WHEN event_type != '1' THEN 1 ELSE 0 END ) conversion
    ,sum(CASE WHEN event_type != '1' THEN payout   ELSE 0 END) revenue
    FROM real_conversion WHERE time_slot >='{day}00' and time_slot <= '{day}23'
    group by day,channel_type ,channel ,platform ,country,offer_pkg) a
    LEFT JOIN
    (SELECT day,channel
    ,case when lower(platform) = 'ios' then 2 ELSE 1 END platform
    ,country,offer_pkg  ,sum(cost) cost
    from kunlun_report where day = '{day}'
    group by day,channel ,platform ,country,offer_pkg  ) b
    on a.channel = b.channel
    and a.platform  = b.platform
    and a.country  = b.country
    and a.offer_pkg  = b.offer_pkg
    and a.day = b.day
    """.format(day=day,day_format = day_format)

    result = client.execute(sql_str)
    df = pd.DataFrame(list(result),columns=['date','channel_type','channel','platform','country','pkg','conversion','revenue','cost'])
    print(df)
    return df

def write_to_mysql(t2, df):
    try:
        # 删除 前两个小时的数据

        # db.commit()
        # db.close()
        # cursor.close()
        conn = create_engine(
            'mysql+pymysql://db_app:Vee8lie3aiNee9sa@php-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com:3306/new_ssp?charset=utf8')

        pd.io.sql.to_sql(df, 'youngs_day_report', conn, schema='new_ssp', if_exists='append', index=False)
    except  Exception as e:
        print("invalid msg:", e.args, file=sys.stderr)






if __name__ == '__main__':

    clickhouse_client = clickhouse_util()
    client=clickhouse_client.return_client()
    ys_day = datetime.datetime.today() - datetime.timedelta(days=1)
    day = str(ys_day.strftime('%Y%m%d'))
    day_format = str(ys_day.strftime('%Y-%m-%d'))
    #time_slot = str((datetime.datetime.today() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H'))
    df=execute_sql(day,day_format,client)
    write_to_mysql(day,df)

