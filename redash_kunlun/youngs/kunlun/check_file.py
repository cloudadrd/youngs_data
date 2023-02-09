import hashlib
import os
import datetime
import sys
from urllib import request
if len(sys.argv)>1:
    yesterday=sys.argv[1]
else:
    yesterday=(datetime.datetime.today()-datetime.timedelta(1)).strftime("%Y%m%d")

check_file_income='SD_'+yesterday+"000000"+'_000.dat.gz'

check_file_income_ = os.popen("md5sum "+check_file_income).read()
check_file_income_md5=check_file_income_.split(' ')[0]

#check_file_income_md5= hashlib.md5(check_file_income.encode(encoding='UTF-8')).hexdigest()
#check_file_outcome_md5= hashlib.md5(check_file_outcome.encode(encoding='UTF-8')).hexdigest()

check_file_name="./"+"SD_"+yesterday+"000000.check"
with open (check_file_name,'a') as o:
    o.write(check_file_income+','+check_file_income_md5+'\r\n')
o.close()
