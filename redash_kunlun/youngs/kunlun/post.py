import urllib3
import datetime
import requests
import sys




if len(sys.argv) > 1:
    yesterday = sys.argv[1]
else:
    yesterday = (datetime.datetime.today() - datetime.timedelta(1)).strftime("%Y%m%d")



#url = 'http://172.30.10.96:8093/kunlun/uploadData/detail'
#url = 'https://kunlunapi.yeahmobi.com/kunlun/uploadData/detail'
url = 'https://kunlunapi.ymtech.info/kunlun/uploadData/detail'

#url = 'http://172.30.176.16:8093/kunlun/uploadData/detail'
file1 = 'SD_'+yesterday+"000000"+'_000.dat.gz'

file2 = 'SD_'+yesterday+"000000"+'.check'


multiple_files = [
        ('file', (file1, open("./"+file1, 'rb'),)),
        ('file', (file2, open("./"+file2, 'rb')))]


data = {
  'date':yesterday+"000000", 'businessKey': 'SD'
 }

 # data传入请求参数dict,files传入待上传文件参数dict
print("......")
r = requests.post(url, data=data, files=multiple_files)
print("------")
import shutil
#shutil.move(file1,"/mnt/s3/kunlun/result/"+yesterday+"/")
#shutil.move(file2,"/mnt/s3/kunlun/result/"+yesterday+"/")
import os
#os.remove("./" + file1)
#os.remove("./"+file2)
print(r.json())
