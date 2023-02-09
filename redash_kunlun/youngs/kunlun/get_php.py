from clickhouse_driver import Client
from sqlalchemy import create_engine
import pandas as pd
from ck_util import clickhouse_util
import datetime
import pymysql
import re
import os
clickhouse_client = clickhouse_util()
client=clickhouse_client.return_client()



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
    select a.day,c.slot_str as slot ,b.short_name as country ,a.re_real_revenue re_real_revenue
from 
((select day,placement_id,country_id,sum(re_real_revenue) re_real_revenue from cloudmobi_new_reporting where day = '%s'  group by day,placement_id,country_id) a
left JOIN 
(select id,short_name from cloudmobi_country  ) b
on a.country_id = b.id
left join
(select id,case when slot_str is null or slot_str = '' then id else slot_str end as slot_str from cloudmobi_app_placements  ) c
on a.placement_id = c.id)
order by re_real_revenue desc 
"""%day
    cur.execute(sql_kunlun)
    #print(cur.fetchall())
    df_kunlun = pd.DataFrame(list(cur.fetchall()),columns=['day','slot','country','re_real_revenue'])
   
    return df_kunlun




def read_sql(sql):
    data, columns = client.execute(
	sql, columnar=True, with_column_types=True)
    df = pd.DataFrame({re.sub(r'\W', '_', col[0]): d for d, col in zip(data, columns)})

    return df
def get_type_dict(tb_name):
    sql = "select name, type from system.columns where table='{tb_name}'".format(tb_name = tb_name)
    print(sql)
    df = read_sql(sql)
    df = df.set_index('name')
    type_dict = df.to_dict('dict')['type']

    return type_dict


if __name__ == '__main__':

    clickhouse_client = clickhouse_util()
    client=clickhouse_client.return_client()
    ys_day = datetime.datetime.today() - datetime.timedelta(days=1)
    day = str(ys_day.strftime('%Y%m%d'))
    day_format = str(ys_day.strftime('%Y-%m-%d'))
    #time_slot = str((datetime.datetime.today() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H'))

    df_result = get_mysql(day)

    print(df_result)
    type_dict = get_type_dict("php_new_reporting")
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
            print(col_name)
        elif col_type == 'String':
            df_result[col_name] = df_result[col_name].astype('str').fillna(value = '')
    # df数据存入clickhouse
    cols = ','.join(columns)
    data = df_result.to_dict('records')
    client.execute("INSERT INTO php_new_reporting ({cols}) VALUES".format(cols=cols), data, types_check=True)
