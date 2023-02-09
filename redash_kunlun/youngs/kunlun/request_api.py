import sys
import pandas as pd
import requests
import numpy as np
import json
from urllib.request import Request, urlopen
import pandas as pd
import numpy as np

url = "https://kunlunapi.ymtech.info/kunlun/advertiserRecord/findList"
#url = "https://kunlunapi.yeahmobi.com//kunlun/advertiserRecord/findList"


payload = json.dumps({
  "businessCode": "SD"
})
headers = {
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

re = json.loads(response.text)

#print(re)
for i in re :
    print(i["code"])
