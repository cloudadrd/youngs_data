import pymysql
from config import report2_config
import pandas as pd
from sqlalchemy import create_engine
from ck_util import clickhouse_util
import sys
def select_af_data(timeslot):
    con = pymysql.connect(**report2_config)
    sql = """
        select agency,pid,campaign,impression,click,ctr,install,cvr,cost,ecpi,roi,pkg,platform from af_report where date='%s'
    """%timeslot
    af_df = pd.read_sql(sql, con)
    return af_df

def insert_data(timeslot): # uri = 'clickhouse://default:zSEA4XzCMKKV6rbA6fLy@10.0.29.66/real_bi'
    df = select_af_data(timeslot)
    result_list=df.to_dict(orient='records')
    clickhouse_client = clickhouse_util()
    client = clickhouse_client.return_client()
    for i in result_list:
        i['pid'] = i.get('pid').split('_')[0]
        platform=i['platform']
        if platform == 'android':
            i['platform']='Android'
        else:
            i['platform']='iOS'


    client.execute('INSERT INTO af_report (agency,pid,campaign,impression,click,ctr,install,cvr,cost,ecpi,roi,pkg,platform) VALUES',result_list)

if len(sys.argv) > 1:
    timeslot = sys.argv[1]
else:
    timeslot = str((datetime.today() - timedelta(hours=1)).strftime('%Y%m%d'))

insert_data(timeslot)