from sqlalchemy import create_engine
import pandas as pd
from ck_util import clickhouse_util
import time
from NewOffer import NewOffer
import datetime
# 获取今天offer_id 截止到当前的事件数。

def select_today_event(client,offer_id_list,day):
    sql_str="""

            SELECT offer_id,count(*) as event_num from real_conversion where toYYYYMMDD(utctime)='%s'
            and event_type='2'
            and offer_id in %s
            group by offer_id
    """%(day,offer_id_list)
    result=client.execute(sql_str)
    print(result)
    return result
# 获取 运营设置的event_cap数。
def select_event_cap(client,day):
    newoffer=NewOffer()
    event_cap_data = newoffer.get_event_cap()
    offer_id_list=[]
    offer_id_dict={}
    for line in event_cap_data:
        offer_id_list.append(line[0])
        offer_id_dict[line[0]]=line[1]

    # 获取当前的事件数
    now_event=select_today_event(client,offer_id_list,day)
    over_offer_id=[]
    # 判断是否超过event_cap. 将超的offer_id添加到 over_offer_id list中。
    for line in now_event:
        offer_id=line[0]
        now_event=line[1]
        if now_event>=int(offer_id_dict[offer_id]):
            over_offer_id.append([offer_id,2])

    # list 转为 dataframe
    over_offer_id_df = pd.DataFrame(over_offer_id,columns=['offer_id','status'])

    print(over_offer_id_df)
    return over_offer_id_df

def execute_sql(time_slot, client, day, df1,event_cap_df):
    sql_str = """
    SELECT
       offer_id AS offer,
       af_siteid,
       COUNT(*) install
    FROM real_bi.real_conversion
    WHERE toYYYYMMDD(utctime)>='{day}'
    and formatDateTime(utctime,'%Y%m%d%H%M')>='{time_slot}'
    AND event_type='1'
    AND adtype in ('19','38','20','40')
    AND offer_pkg !='com.dts.freefireth'
    GROUP BY
         offer,
         af_siteid

    """.format(day=day, time_slot=time_slot)

    print(sql_str)
    result = client.execute(sql_str)
    # print(result)
    df = pd.DataFrame(list(result), columns=['offer_id', 'af_siteid', 'install'])
    df3 = pd.merge(df, df1, how='left', on=['offer_id', 'af_siteid'])
    df3.to_csv("./aaaa.csv")
    df4 = df3.loc[df3['black'].isnull()]
    print(df3.loc[df3['black'].notnull()])
    columns = ['offer_id', 'af_siteid', 'install']
    final_merge_data = df4[columns]
    # 关联出已超的offer_id 状态置为2 其余的状态为1

    final_df = pd.merge(final_merge_data,event_cap_df,how='left',on=['offer_id'])
    final_df = final_df.fillna(1)
    truncate_table()
    time.sleep(3)
    yconnect = create_engine(
        'mysql+pymysql://db_app:Vee8lie3aiNee9sa@php-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com:3306/new_ssp?')
    pd.io.sql.to_sql(final_df, 'cloudmobi_buzz_install', yconnect, schema='new_ssp', if_exists='append',
                     index=False)


php_system_config = {
    'host': 'php-system-bj.c3vdzrglae3k.ap-southeast-1.rds.amazonaws.com',
    'user': 'db_app',
    'password': 'Vee8lie3aiNee9sa',
    'database': 'new_ssp',
    'port': 3306
}
import pymysql


def truncate_table():
    con = pymysql.connect(**php_system_config)
    sql = "delete from cloudmobi_buzz_install"
    cur = con.cursor()
    cur.execute(sql)
    con.commit()
    con.close()


def black_list():
    con = pymysql.connect(**php_system_config)
    sql = "select adv_offer,site_black_value from new_offer where site_black_value != ''"
    cur = con.cursor()
    cur.execute(sql)
    return cur.fetchall()


if __name__ == '__main__':

    clickhouse_client = clickhouse_util()
    client = clickhouse_client.return_client()
    day = str((datetime.datetime.today() - datetime.timedelta(minutes=10)).strftime('%Y%m%d'))
    time_slot = str((datetime.datetime.today() - datetime.timedelta(minutes=10)).strftime('%Y%m%d%H%M'))
    print(time_slot)
    black_l = black_list()
    black = []
    for row in black_l:
        key = row[0]
        value = row[1]
        l = row[1].split(",")
        for i in l:
            black.append([key, i, 'black'])
    df1 = pd.DataFrame(black, columns=['offer_id', 'af_siteid', 'black'])


    event_cap_df = select_event_cap(client,day)

    execute_sql(time_slot, client, day, df1,event_cap_df)

