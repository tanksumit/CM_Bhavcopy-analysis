from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
import pandas as pd
import requests
import urllib.request
from bs4 import BeautifulSoup
from datetime import date, timedelta, datetime  
import xlwings as xw

Excel_File = "Graphs.xlsx"

wb = xw.Book(Excel_File)

NIFTYSHEET = wb.sheets("DATA_NF")

BANKNIFTYSHEET = wb.sheets("DATA_BN")


To_Date=date.today().strftime('%d-%m-%Y')
#Keep max 90 days, not more than that
No_of_Days=30
FDate=date.today()
Fm_Date=FDate-timedelta(days=No_of_Days)
From_Date=Fm_Date.strftime('%d-%m-%Y')
Days=(FDate.day-Fm_Date.day)
now=datetime.now()
dt_string=now.strftime("%d/%m/%Y %H:%M")

Trading_Days = []
url = "https://www1.nseindia.com/products/dynaContent/equities/indices/historicalindices.jsp?indexType=NIFTY%2050&fromDate="+str(From_Date)+"&toDate="+str(To_Date)
headers = {'User-Agent':'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Mobile Safari/537.36','Referer':'https://www1.nseindia.com/products/content/equities/indices/historical_index_data.htm'}
response = requests.get(url,headers=headers)
soup = BeautifulSoup(response.text,"html.parser")
s=soup.findAll('nobr')
for i in s:
      for m in i:
            Trading_Days.append(m)
        
print('Data will received for ',len(Trading_Days),"Days")

print(Trading_Days)

if len(Trading_Days) > 0:

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
                zfile.extractall('D:\Automate the Borring Stuff\Bhavcopy\Bhavcopy_Files')
                r=zfile.namelist()
                for i in r:
                    data=pd.read_csv('D:\Automate the Borring Stuff\Bhavcopy\Bhavcopy_Files\{}'.format(i))
                    filter=pd.DataFrame(data).drop(['STRIKE_PR','OPTION_TYP','CONTRACTS','VAL_INLAKH'],axis=1)
                    BN=(filter.loc[filter['INSTRUMENT']=='FUTIDX']).set_index('SYMBOL')
                    BANKNIFTY=BN.loc[['BANKNIFTY']]
                    BANKNIFTY['COI_OI']=BANKNIFTY['OPEN_INT'].sum()
                    BANKNIFTY['COI_CHOI']=BANKNIFTY['CHG_IN_OI'].sum()
                    NF=(filter.loc[filter['INSTRUMENT']=='FUTIDX']).set_index('SYMBOL')
                    NIFTY=NF.loc[['NIFTY']]
                    NIFTY['COI_OI']=NIFTY['OPEN_INT'].sum()
                    NIFTY['COI_CHOI']=NIFTY['CHG_IN_OI'].sum()
                    BNNF = pd.concat([BANKNIFTY,NIFTY])
                    BNNF['COUNT'] = range(6)
                    all=all.append(BNNF).drop(['INSTRUMENT',"EXPIRY_DT",'SETTLE_PR'],axis = 1)[['TIMESTAMP',
                    'OPEN','HIGH','LOW','CLOSE','OPEN_INT','CHG_IN_OI','COI_OI','COI_CHOI','COUNT']]
                    count = count + 1
                    print('Data receivied for {} Trading Day'.format(count))           
                
    all.to_csv('Cumulative.csv',index = True)

if len(Trading_Days) > 0:
    NIFTYDATA = pd.read_csv('Cumulative.csv')
    NIFTYDATA = NIFTYDATA.loc[NIFTYDATA['COUNT'] == 3]
    NIFTYSHEET.range('A1').options(header = True, index = False).value = NIFTYDATA
    BANKNIFTYDATA = pd.read_csv('Cumulative.csv')
    BANKNIFTYDATA = BANKNIFTYDATA.loc[BANKNIFTYDATA['COUNT'] == 0]
    BANKNIFTYSHEET.range('A1').options(header = True, index = False).value = BANKNIFTYDATA