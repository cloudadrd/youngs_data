import pymysql
import math
import pandas as pd
from http.server import BaseHTTPRequestHandler, HTTPServer
import datetime
import json
import logging
from ck_util import clickhouse_util
# 需求文档  https://janzlz0n1f.feishu.cn/docs/doccnlBQXPAuQkRdTxi2w336nTd
#
from clickhouse_driver import Client

php_system_config = {
    'host': '10.17.15.98',
    'user': 'db_app',
    'password': 'Vee8lie3aiNee9sa',
    'database': 'new_ssp',
    'port': 3306
}

con = pymysql.connect(**php_system_config)



    # 查询前五分钟转化数
def get_conv( time_slot, minutes,client):
    sql_str = """
        SELECT
        channel,
        offer_id,
        count( * ) AS conv
    FROM
        real_conversion
    WHERE
        time_slot >= '{time_slot}'
        AND toMinute ( utctime ) >= {minutes}
        AND event_type != '1'
        AND adtype NOT IN ( '19', '38' )
    GROUP BY
        channel,
        offer_id
    """.format(time_slot=time_slot,minutes=minutes)
    print(sql_str)
    result = client.execute(sql_str)
    conv_df = pd.DataFrame(list(result),
                            columns=['channel', 'offer_id', 'conv'])
    return conv_df

def execute_php_sql():
    # 10.17.15.98
    # php-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com

    sql = """

        SELECT
        channel,
        SUBSTRING_INDEX( adv_offer, '_',- 1 ) AS offer_id,
        target_cvr
    FROM
        new_offer
    where
        channel_type = 2
        AND status = 1
        AND adv_status = 1
        AND cvr_status = 1
        """
    new_offer_df = pd.read_sql(sql, con)
    return new_offer_df






if __name__ == '__main__':
    from sys import argv

    clickhouse_client = clickhouse_util()
    client = clickhouse_client.return_client()

    time_slot = (datetime.datetime.today()).strftime('%Y%m%d%H')
    minutes=(datetime.datetime.today() - datetime.timedelta(minutes=5)).strftime('%M')
    print(time_slot,minutes)

    df = get_conv(time_slot, minutes,client)
    new_offer_df = execute_php_sql()
    merge_data = pd.merge(new_offer_df, df, how="left", on=['channel', 'offer_id'])
    columns = ['channel', 'offer_id', 'conv', 'target_cvr']
    final_merge_data = merge_data[columns]
    final_merge_data.to_csv("./aaaa.csv")

    # 删除conv 为0的行
    #final_merge_data = final_merge_data.dropna()
    final_merge_data.dropna(subset=['target_cvr'],inplace=True)
    #final_merge_data = final_merge_data[final_merge_data['conv'] > 0]
    final_merge_data = final_merge_data[final_merge_data['target_cvr'] > 0]
    print(final_merge_data)

    # 计算出pacing_cvr df_data.apply(lambda x: x['总数'] / x['人数'], axis=1)
    final_merge_data['pacing_cvr'] = final_merge_data.apply(lambda x: x['conv'] / x['target_cvr'] * 10000 / 5,
                                                            axis=1)
    cur = con.cursor()                                                        
    print(final_merge_data)
    result_list = []
    for index, row in final_merge_data.iterrows():
        line_dict = {}
        channel_offer = str(row[0]) + "_" + str(row[1])
        pacing_cvr = row[4]
        if pacing_cvr=='nan' or pacing_cvr==None or math.isnan(pacing_cvr):
            pacing_cvr=0
        else:
            pacing_cvr=round(pacing_cvr)
        udata_sql = "insert into target_pacing(`key`,`value`) values ('%s','%s') ON DUPLICATE KEY UPDATE `value`= '%s' " % (channel_offer,pacing_cvr,pacing_cvr)
        print(udata_sql)
        cur.execute(udata_sql)
        con.commit()