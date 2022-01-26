# athour qingda.liu
import sys
import datetime
from urllib.request import Request, urlopen
import pandas as pd
import requests
from sqlalchemy import create_engine

def get_data(date):
    url = "https://spark-ml-train-new.oss-us-east-1.aliyuncs.com/airflow/t_ym13_data/ym13_%s.csv"%date
    print(url)
    response = requests.get(url).text
    response_list = response.split("\n")
    col_list = ['date','channel_type','platform','channel','country','pkg','conversion','revenue','cost','click','install']
    value_list = []

    for line in response_list :
        #print(line)
        if line.__contains__("country") :
            #col_list = line.replace('"','').split(",")
            #print(col_list)
            continue
        row = line.replace('"','').split(",")
        if len(row) >= 12 :
            day = str(row[0])
            country = str(row[1])
            if str(row[2]) == 'Android' :
                platform = '1'
            elif str(row[2] == 'iOS'):
                platform = '2'
            else:
                continue
            channel_type = '6'
            pkg = str(row[3])
            channel = str(row[5])
            imp = str(row[6])
            click = str(row[7])
            conversion = str(row[8])
            revenue = str(row[9])
            cost = str(row[10])
            value_list.append([day,channel_type,platform,channel,country,pkg,conversion,revenue,cost,click,'0'])

    df = pd.DataFrame(value_list,columns = col_list)
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

    return 0

if __name__ == '__main__' :

    date = str((datetime.datetime.today() - datetime.timedelta(1)).strftime("%Y-%m-%d"))
    df = get_data(date)
    df['conversion'] = df['conversion'].astype('int')
    df['revenue'] = df['revenue'].astype('float')
    df['cost'] = df['cost'].astype('float')
    df['install'] = df['install'].astype('int')
    df['click'] = df['click'].astype('int')
    df = df.groupby(['date','channel_type','platform','channel','country','pkg'],axis=0,as_index=False).sum()
    print(df)
    df.to_csv('./out_yoho',sep=',',index=False,header=False)
    write_to_mysql(date,df)
