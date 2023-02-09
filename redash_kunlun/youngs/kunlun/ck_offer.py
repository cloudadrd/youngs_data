from clickhouse_driver import Client
from sqlalchemy import create_engine
import pandas as pd
from ck_util import clickhouse_util
import datetime
import pymysql
import re
import os
clickhouse_client = clickhouse_util()
client=clickhouse_client.return_real_bi_client()



def get_mysql(day):
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
    select id,offer_id,adv_offer,create_type,channel,channel_type,adv_status,conversion_flow,status,title
 ,pkg,attribute_provider,pid,platform,country,revenue,adtype,adv_tracking_link,target_cvr,cvr_status,is_s2s,s2s_tracking_link,app_url
 ,site_type,site_value,site_black_value,description,traffic from new_ssp.new_offer
"""
    cur.execute(sql_kunlun)
    result=cur.fetchall()
    result_list = []
    print('.....')
    for line in result:
        print(len(line))
  #      print(line)
        traffic = line[27]
        try:
            traffic_list = eval(traffic)
            cap_daily=0
            for traffic in traffic_list:
                pub_status = traffic.get('pub_status')
                if pub_status == 1:
                    cap_daily=cap_daily+traffic.get('cap_daily')
        except Exception as e:
            print(e.args)
            cap_daily=0
        line_list=list(line)
        line_list.insert(6,cap_daily)
        result_list.append(line_list)

    df_kunlun = pd.DataFrame(result_list,columns=['id','offer_id','adv_offer','create_type','channel','channel_type','cap_daily','adv_status','conversion_flow','status','title'
    ,'pkg','attribute_provider','pid','platform','country','revenue','adtype','adv_tracking_link','target_cvr','cvr_status','is_s2s','s2s_tracking_link','app_url'
    ,'site_type','site_value','site_black_value','description','traffic']).drop(columns=['id'])
    df_kunlun.to_csv("./aaa.csv")
    return df_kunlun




def read_sql(sql):
    data, columns = client.execute(
	sql, columnar=True, with_column_types=True)
    df = pd.DataFrame({re.sub(r'\W', '_', col[0]): d for d, col in zip(data, columns)})

    return df
def get_type_dict(tb_name):
    sql = "select name, type from system.columns where table='{tb_name}'".format(tb_name = tb_name)
    df = read_sql(sql)
    df = df.set_index('name')
    type_dict = df.to_dict('dict')['type']

    return type_dict


if __name__ == '__main__':

    clickhouse_client = clickhouse_util()
    client=clickhouse_client.return_real_bi_client()
    ys_day = datetime.datetime.today() - datetime.timedelta(days=1)
    day = str(ys_day.strftime('%Y%m%d'))
    day_format = str(ys_day.strftime('%Y-%m-%d'))
    #time_slot = str((datetime.datetime.today() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H'))

    df_result = get_mysql(day)

    type_dict = get_type_dict("new_offer")
    columns = list(type_dict.keys())
    # 类型处理
    for i in range(len(columns)):
        col_name = columns[i]
        col_type = type_dict[col_name]
        print(col_name + "--" + col_type)
        if 'Date' in col_type:
            df_result[col_name] = pd.to_datetime(df_result[col_name])
        elif 'Int' in col_type:
            df_result[col_name] = df_result[col_name].astype('int')
        elif 'Float' in col_type:
            df_result[col_name] = df_result[col_name].astype('float')
        elif col_type == 'String':
            df_result[col_name] = df_result[col_name].astype('str').fillna(value = '')
    # df数据存入clickhouse
    cols = ','.join(columns)
    data = df_result.to_dict('records')
    client.execute("ALTER TABLE new_offer DELETE where offer_id is not null")
    client.execute("INSERT INTO new_offer ({cols}) VALUES".format(cols=cols), data, types_check=True)
