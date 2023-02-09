from clickhouse_driver import Client
from sqlalchemy import create_engine
import pandas as pd
from ck_util import clickhouse_util
import datetime
import numpy as np
import zipfile
import gzip
import shutil
import pymysql
import re
import os
import json
import requests
#clickhouse_client = clickhouse_util()
#client=clickhouse_client.return_client()

def execute_sql(day,client):
    sql_str="""
    select
    channel
    ,'OFLX_GA' offer_type
    ,adv_id
    ,kunlun_code
    ,'' traffic
    ,country
    ,concat(day,'000000') as create
    ,'JSXMLX_1'
    ,round(sum(revenue )*10000) revenue
    ,0 as t1
    ,0 as t2
    ,round(sum(cost)*10000) cost
    ,0 as t3
    ,0 as t4
    from(select * from  kunlun_report
    where day = '%s'
    and adv_id != ''
    and ((kunlun_code != '') or (kunlun_code = '' and cost = 0 )))
    group by  adv_id ,channel ,kunlun_code ,country,day
    """%day
    result = client.execute(sql_str)
    #print(result)
    df = pd.DataFrame(list(result),columns=['offer_id','offer_type','adv_id','kunlun_code','traffic','country','created','type','revenue','t1','t2','cost','t3','t4'])
    url = "https://kunlunapi.ymtech.info/kunlun/advertiserRecord/findList"
    #url = "https://kunlunapi.yeahmobi.com//kunlun/advertiserRecord/findList"
    payload = json.dumps({
    "businessCode": "SD"
    })
    headers = {
    'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.text)
    re = json.loads(str(response.text))["data"]
    list_col = []
    for i in re :
        list_col.append([i['code'],i['contractCode']])

    df_kunlun = pd.DataFrame(list_col,columns=['adv_id','contractCode'])
    df_result = pd.merge(df, df_kunlun, on='adv_id', how='left')
    print(df_result)
    return df_result

def kunlun_read_mysql_username(df):
    map_list = []
    #昆仑接口合同编号
    con_url="https://kunlunapi.ymtech.info/kunlun/channel/queryChannelInfoDetailByCodeOrName"
    headers={'content-Type':'application/x-www-form-urlencoded'}
    con_adv_url = "https://kunlunapi.ymtech.info/kunlun/customer/queryCustomerContractByCode"

    list_channelID = np.array(df).tolist()
    for a in range(0, len(list_channelID)):
        body_data="codeOrName=%s"%list_channelID[a][3]
        #print(body_data)
        adv_data = "code=%s"%list_channelID[a][14]
        #print(adv_data)
        bb=requests.post(url=con_adv_url,data=adv_data,headers=headers)
        bb=json.loads(bb.text)
        adv_currency =""
        try :
            adv_currency=bb.get('data').get('customerContractRes').get('currency')
        except Exception as e:
            print("客户为空")
        list_channelID[a].append(adv_currency)
        aa=requests.post(url=con_url,data=body_data,headers=headers)
        aa=json.loads(aa.text)
        SupID=""
        currency=""
        try:
            #获取渠道合同编号
            #print(aa)
            SupID=aa.get('data').get('contractList')[0].get('channelContractRes').get('code')
            #获取渠道币种
            currency=aa.get('data').get('contractList')[0].get('channelContractRes').get('currency')
        except Exception as e:
            print("合同编和为空")
        list_channelID[a].append(SupID)
        list_channelID[a].append(currency)
        #list_channelID[a].append(i['SupID'])
    for line in list_channelID:
        map_list.append(line)
    #print("*********")
    #print(map_list)
    df_result = pd.DataFrame(map_list, columns=['offer_id','offer_type','adv_id','kunlun_code','traffic','country','created','type','revenue','t1','t2','cost','t3','t4'
    ,'contractCode','adv_currency','supid','outcome_dollar'])
    list = ['offer_id','offer_type','adv_id','contractCode','kunlun_code','supid','traffic','country','created','type','adv_currency','revenue','t1','t2','outcome_dollar'
,'cost','t3','t4']
    finnal_df = df_result[list]
    return finnal_df

def zip_to_local(final_income_df, yesterday):
    try:
        os.mkdir("/opt/kunlun/result/" + yesterday)
    except:
        pass
    final_income_list = np.array(final_income_df).tolist()
    dat_name = "SD_" + yesterday + "000000" + "_000.dat"
    for line in final_income_list:
        field = '\u0001'.join(str(i).replace("\u0001", " ") for i in line)
        field.replace("\r\n", ' ')
        with open("./" + dat_name, 'a') as o:
            o.write(field + '\r\n')
    # zip = zipfile.ZipFile("./" + dat_name + ".gz", "w", zipfile.ZIP_DEFLATED)
    # zip.write("./" + dat_name, "./" + dat_name)
    with open("./" + dat_name, 'rb')as read, gzip.open("./" + dat_name + ".gz", 'wb')as write:
        shutil.copyfileobj(read, write)
    os.remove("./" + dat_name)

if __name__ == '__main__':

    clickhouse_client = clickhouse_util()
    client=clickhouse_client.return_client()
    ys_day = datetime.datetime.today() - datetime.timedelta(days=1)
    day = str(ys_day.strftime('%Y%m%d'))
    day_format = str(ys_day.strftime('%Y-%m-%d'))
    #time_slot = str((datetime.datetime.today() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H'))
    df = execute_sql(day,client)
    final_outcome_df = kunlun_read_mysql_username(df)
    final_outcome_df2 = final_outcome_df.dropna(axis=0, how='any')
    final_outcome_df2['cost']=np.where(final_outcome_df2['outcome_dollar']=='CNY',final_outcome_df2['cost']*6.5,final_outcome_df2['cost'])
    final_outcome_df2['revenue']=np.where(final_outcome_df2['adv_currency']=='CNY',final_outcome_df2['revenue']*6.5,final_outcome_df2['revenue'])
    final_outcome_df2['cost'] = final_outcome_df2['cost'].astype('long')
    final_outcome_df2['revenue'] = final_outcome_df2['revenue'].astype('long')
    

    final_outcome_df2.to_csv("./outcome.csv")
    print(final_outcome_df2)
    zip_to_local(final_outcome_df2, day)
