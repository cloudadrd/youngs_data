#!/usr/bin/python
# -*- coding: UTF-8 -*-
from http.server import BaseHTTPRequestHandler, HTTPServer
import logging
import datetime
from urllib import parse
import pymysql
import pandas as pd
import json
from ck_util import clickhouse_util

php_system_config = {
        'host': 'php-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com',
        'user': 'db_app',
        'password': 'Vee8lie3aiNee9sa',
        'database': 'new_ssp',
        'port': 3306
    }

con = pymysql.connect(**php_system_config)


def execute_sql(day,hour,day_format,client):
    sql_str = """
    select
        case when  click_tb.channel != '' then  click_tb.channel else conversion_tb.channel END  channel
            ,case when  click_tb.country != ''  then click_tb.country else conversion_tb.country end country
            ,case when  click_tb.offer_id != '' then click_tb.offer_id else conversion_tb.offer_id end  offer_id
            ,coalesce( round(click,2) ,0.00) click
            ,coalesce ( conversion , 0) conversion
            ,round( (case when click >0 then conversion/click  else 0 end) ,2) cvr
            from
            (
           select  offer_id,country,channel,sum(click) click from (
            select offer_id
            ,country,channel,round(sum(clk_count)/10000,2) click
            from real_buzz
            where time_slot >= '{day}00' and clk_count > 0
            and offer_id != '' and country != '' and channel != '' and offer_pkg not like '%lazada%'
            group by offer_id,country,channel
            UNION ALL
            select case when adtype = '39' then splitByString('_',offer_id)[2] else offer_id end offer_id
            ,country,channel,round(count(1)/10000,2) click
            from real_fenix_click
            where time_slot >= '{day}00' and click_through = '1'
            and country != '' and channel != '' and offer_pkg not like '%lazada%'
            group by (case when adtype = '39' then splitByString('_',offer_id)[2] else offer_id end) ,country,channel,adtype,offer_id
            UNION ALL
            select offer_id,country,channel,round(sum(clk_count)/10000,2) click
            from real_buzz_sync
            where time_slot >= '{day}00'
            and country != '' and channel != '' and offer_pkg not like '%lazada%'
            group by offer_id,country,channel
            UNION ALL
            select offer_id ,country,channel,round( sum(`count`)/1000,2) click
            from real_click  where day  = '{day_format}'
            and adtype in ('3','8','9')  group by offer_id,country,channel
            )
            group by offer_id,country,channel
            ) click_tb
            FULL JOIN
            (select offer_id,country,channel,count(1) conversion
            FROM real_conversion
            WHERE time_slot>= '{day}00' and  substr(toString(toYYYYMMDDhhmmss(utctime)),1,10) >= '{day}00'
            and (( (adtype < '19' or adtype = '39') and event_type = '2') or ( adtype >= '19' and adtype != '39' and event_type = '1' and offer_pkg != 'com.alibaba.aliexpresshd') or (offer_pkg ='com.alibaba.aliexpresshd' and event_type = '2')) and offer_pkg not like '%lazada%'
            and offer_id != '' and country != '' and channel != ''
            GROUP BY offer_id,country,channel) conversion_tb
            on click_tb.offer_id = conversion_tb.offer_id
            and click_tb.country = conversion_tb.country
            and click_tb.channel = conversion_tb.channel
        """.format(day=day,hour=hour,day_format=day_format)
    print(sql_str)

    result = client.execute(sql_str)

    df = pd.DataFrame(list(result), columns=['channel', 'country', 'offer_id', 'click','conversion','cvr'])
    print(df)
    return df

    # result_dict = {}
    # result = client.execute(sql_str)
    # for line in result:
    #     key = line[0] + "_" + line[1] + "_" + line[2] + "_" + line[3]
    #     value = line[5]
    #     result_dict[key] = value
    # return result_dict

def execute_php_sql():

    sql = """
            select channel,SUBSTRING_INDEX(adv_offer,'_',-1) as offer_id,country,offer_id as offer from new_offer
        """
    new_offer_df = pd.read_sql(sql, con)
    return new_offer_df





if __name__ == '__main__':
    from sys import argv
    clickhouse_client = clickhouse_util()
    client = clickhouse_client.return_client()
    start_date = datetime.datetime.now().strftime('%Y%m%d')
    updata_hour = datetime.datetime.now().strftime('%Y%m%d%H')
    day_format = datetime.datetime.now().strftime('%Y-%m-%d')
    hour = (datetime.datetime.now()- datetime.timedelta(hours=1)).strftime('%Y%m%d%H')
    hour2 = (datetime.datetime.now()- datetime.timedelta(hours=2)).strftime('%Y%m%d%H')
    date = datetime.datetime.now().strftime('%Y%m%d')
    df = execute_sql(start_date, hour,day_format,client)
    new_offer_df = execute_php_sql()
    print(df,new_offer_df)
    merge_data = pd.merge(df, new_offer_df, how="left", on=['channel', 'offer_id', 'country'])
    columns = ['offer','click','conversion','cvr']
    final_merge_data = merge_data[columns]
    final_merge_data = final_merge_data.loc[final_merge_data['offer'].notnull()]
    cur = con.cursor()
    print("*"*40)
    print(final_merge_data)
    del_sql = "delete from realtime_cvr"
    cur.execute(del_sql)
    con.commit()
    for index,row in final_merge_data.iterrows():
        print(".............")
        key = str(row[0])
        value = str(row[1]) + "_" + str(row[2]) + "_" + str(row[3])
        #udata_sql = "insert into realtime_cvr(`key`,`value`) values ('%s','%s') ON DUPLICATE KEY UPDATE `value`= '%s' " % (key,value,value)
        udata_sql = "insert into realtime_cvr(`key`,`value`,`update_time`) values ('%s','%s','%s') ON DUPLICATE KEY UPDATE `value`= '%s', `update_time` = '%s' " % (key,value,updata_hour,value,updata_hour)

        print(udata_sql)
        cur.execute(udata_sql)
        con.commit()
