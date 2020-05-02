from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
import pandas as pd
import requests
import urllib.request
from bs4 import BeautifulSoup
from datetime import date, timedelta, datetime  

To_Date=date.today().strftime('%d-%m-%Y')
No_of_Days=10
FDate=date.today()
Fm_Date=FDate-timedelta(days=No_of_Days)
From_Date=Fm_Date.strftime('%d-%m-%Y')
Days=(FDate.day-Fm_Date.day)
now=datetime.now()
dt_string=now.strftime("%d/%m/%Y %H:%M")

Trading_Days = []
url="https://www1.nseindia.com/products/dynaContent/equities/indices/historicalindices.jsp?indexType=NIFTY%2050&fromDate="+str(From_Date)+"&toDate="+str(To_Date)
headers = {'User-Agent':'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Mobile Safari/537.36','Referer':'https://www1.nseindia.com/products/content/equities/indices/historical_index_data.htm'}
response = requests.get(url,headers=headers)
soup = BeautifulSoup(response.text,"html.parser")
s=soup.findAll('nobr')
for i in s:
    for m in i:
        Trading_Days.append(m)
        

links = []
for i in Trading_Days:
    url = 'https://www1.nseindia.com/content/historical/DERIVATIVES/'
    link=(url+(i[-4:len(i)])+'/'+(i[-8:len(i)-5].upper()+'/'+'fo'+i.upper().replace('-',"")+'bhav.csv.zip'))
    links.append(link)


url = links

all = pd.DataFrame()

count = 0
while count < len(url):
    with urlopen(url[count]) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zfile:
            zfile.extractall('D:\Automate the Borring Stuff\Bhavcopy')
            r=zfile.namelist()
            for i in r:
                data=pd.read_csv('D:\Automate the Borring Stuff\Bhavcopy\{}'.format(i))
                filter=pd.DataFrame(data).drop(['STRIKE_PR','OPTION_TYP','CONTRACTS','VAL_INLAKH','INSTRUMENT',
                'OPEN','HIGH','LOW','CLOSE','SETTLE_PR','Unnamed: 15'],axis=1).set_index('SYMBOL')
                filter['HighCOI%'] = (filter['CHG_IN_OI']/filter['OPEN_INT'])*100
                filter = filter[filter!=0].dropna()
                all=all.append(filter)
                count = count + 1
                print('Data receivied for {} Trading Day'.format(count))           
              
print(links)
all.to_csv('HighOI.csv',index = True)

                
