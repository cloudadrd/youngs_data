import time
from urllib.request import Request, urlopen
import datetime

with open('./click_url','r') as o:
    a=o.readlines()
hour = time.localtime().tm_hour
aa='sdf32340505'
yesterday_str = str((datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%m%d"))
today_str = str(datetime.datetime.now().strftime("%m%d"))

print(hour)
yesterday_str='0505'
for i in range(0,len(a)):
    if hour ==0 and i==0:
        url = a[i].replace(yesterday_str,today_str)
        print(url)
        request = Request(url)
        html = urlopen(request)
    if hour ==1 and i==1:
        url = a[i].replace(yesterday_str,today_str)
        request = Request(url)
        html = urlopen(request)
