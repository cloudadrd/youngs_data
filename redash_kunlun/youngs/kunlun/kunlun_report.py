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

def execute_sql(day,day_format,client):
    sql_str="""
    
    select 
'{day}'
,base.slot
,base.country
,base.offer_id
,base.platform
,base.adtype
,base.offer_pkg
,base.channel
,case when rate > 0  then payout_all_offer * 0.2 * rate
    when  base.adtype not in ('39','38','19') and php.re_real_revenue > 0 then base.payout
    when base.adtype = '19' and offer_tb.offer_cost > 0 then base.payout *0.8
    else base.payout
    end  as last_revenue
,case when rate > 0 then base.cost
    when  base.adtype not in ('39','38','19') and php.re_real_revenue > 0 then base.payout *( php.re_real_revenue / payout_all_slot)
    else base.cost
    end  as last_cost
from (select base.*
,sum(base.payout) over (partition by base.offer_id) as payout_all_offer
,sum(base.payout) over (partition by base.slot,base.country ) as payout_all_slot
,offer_tb.offer_cost
,php.re_real_revenue
,b.conversion/ b.conversion_all rate
FROM
(
select slot,country,offer_id ,platform,adtype ,offer_pkg,channel,sum(payout) payout,sum(cost) cost
from
real_conversion  where time_slot  >= '{day}00'  and time_slot <= '{day}23'
group by slot,country,offer_id ,platform,adtype ,offer_pkg,channel
) base
left JOIN
(select channel,offer_id,country,sum(offer_cost) offer_cost from
(select slot,channel,offer_id,country,sum(cost) offer_cost ,sum(payout) pt
from real_conversion
where time_slot  >= '{day}00'  and time_slot <= '{day}23'
group by slot,channel,offer_id,country )
where pt = 0
group by channel,offer_id,country
) offer_tb
on base.channel = offer_tb.channel
and base.country = offer_tb.country
and base.offer_id = offer_tb.offer_id
left join
(select slot,country,re_real_revenue from php_new_reporting where day= '{day_format}') php
on base.slot = php.slot
and base.country = php.country
left JOIN
(select slot,country,offer_id,platform ,offer_pkg ,channel
,conversion
,sum(conversion) over (partition by country,offer_id,platform ,offer_pkg ,channel ) as conversion_all
FROM
( select slot,country,offer_id,platform ,offer_pkg ,channel
,count(1) conversion ,sum(payout ) pt
from
real_conversion  where time_slot  >= '{day}00'  and time_slot <= '{day}23' and adtype = '38'
group by slot,country,offer_id ,platform,offer_pkg,channel )
where pt = 0) b
on base.slot = b.slot and base.country = b.country and base.offer_id = b.offer_id and base.platform = b.platform and base.offer_pkg = b.offer_pkg and base.channel = b.channel )
where length(base.country) = 2
    """.format(day=day,day_format = day_format)
    print(sql_str)
    result = client.execute(sql_str)
    df = pd.DataFrame(list(result),columns=['day','slot','country','offer_id','platform','adtype','offer_pkg','channel','revenue','cost'])
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
    select a.*,
    CONCAT(b.user_id,''),c.kunlun_code
from
(select case when slot_str is null or slot_str = '' then id else slot_str end as slot_str,CONCAT(app_id,'') app_id from cloudmobi_app_placements ) a
left join
(select id, CONCAT(user_id,'') user_id from cloudmobi_app )b
on a.app_id = b.id
left join
(select id,kunlun_code from cloudmobi_user) c
on b.user_id = c.id
"""
    cur.execute(sql_kunlun)
    #print(cur.fetchall())
    df_kunlun = pd.DataFrame(list(cur.fetchall()),columns=['slot','app_id','user_id','kunlun_code'])
    print(df_kunlun)
    sql_adv = "select short_name,adv_id from cloudmobi_ad_network"
    cur.execute(sql_adv)
    df_adv =pd.DataFrame(list(cur.fetchall()),columns=['channel','adv_id'])
    return df_kunlun,df_adv


def merge_df(df,df_kunlun,df_adv):
    df1=pd.merge(df, df_kunlun, on='slot', how='left')
    df_all = pd.merge(df1, df_adv, on='channel', how='left')
    
    return df_all

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
    df=execute_sql(day,day_format,client)
    df_kunlun,df_adv = get_mysql()
    df_result = merge_df(df,df_kunlun,df_adv)

    type_dict = get_type_dict("kunlun_report")
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
    client.execute("INSERT INTO kunlun_report ({cols}) VALUES".format(cols=cols), data, types_check=True)
