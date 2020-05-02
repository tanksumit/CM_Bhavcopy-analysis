#Import required modules

from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
import pandas as pd
import requests
import urllib.request
from bs4 import BeautifulSoup
from datetime import date, timedelta, datetime  
import xlwings as xw
import json
import xlrd



#List of FNO Stocks. Its helpt to filter out this stocsk from Delievery % file and Cash Bavcopy
fno = ['RELIANCE','ZEEL','AMARAJABAT','COLPAL','IDEA','ASIANPAINT','NAUKRI','MUTHOOTFIN','JUSTDIAL','NESTLEIND','ACC','TATACONSUM','MFSL','EXIDEIND','SRF','MARUTI','MARICO','ESCORTS','HEROMOTOCO','APOLLOTYRE','SUNTV','HCLTECH','CUMMINSIND','TORNTPHARM','BRITANNIA','M&M','TCS','BAJAJ-AUTO','INFRATEL','BOSCHLTD','EICHERMOT','NIITTECH','PIDILITIND','PETRONET','ADANIPOWER','KOTAKBANK','HINDUNILVR','TECHM','NCC','INFY','GLENMARK','ASHOKLEY','ITC','HAVELLS','BERGEPAINT','AMBUJACEM','DRREDDY','NTPC','MOTHERSUMI','ADANIPORTS','RAMCOCEM','BEL','TATAPOWER','ADANIENT','AUROPHARMA','BHARTIARTL','TITAN','BIOCON','AXISBANK','GMRINFRA','PEL','SUNPHARMA','MCDOWELL-N','JINDALSTEL','INDUSINDBK','TORNTPOWER','UPL','CONCOR','SRTRANSFIN','GAIL','TATASTEEL','COALINDIA','ULTRACEMCO','BANDHANBNK','JUBLFOOD','DABUR','BATAINDIA','CHOLAFIN','LT','SBIN','BAJAJFINSV','WIPRO','MRF','JSWSTEEL','CIPLA','IDFCFIRSTB','HDFCBANK','TATACHEM','GRASIM','BAJFINANCE','HDFCLIFE','APOLLOHOSP','SHREECEM','IBULHSGFIN','LUPIN','CESC','HDFC','ICICIBANK','L&TFH','VOLTAS','DIVISLAB','PAGEIND','MANAPPURAM','HINDALCO','YESBANK','IOC','FEDERALBNK','TATAMOTORS','BHARATFORG','GODREJCP','POWERGRID','MINDTREE','NMDC','PNB','BPCL','SAIL','NATIONALUM','DLF','UBL','LICHSGFIN','PFC','BANKBARODA','BHEL','EQUITAS','RECLTD','M&MFIN','HINDPETRO','SIEMENS','TVSMOTOR','BALKRISIND','ICICIPRULI','GODREJPROP','MGL','IGL','CANBK','PVR','VEDL','CADILAHC','OIL','INDIGO','UJJIVAN',
'RBLBANK','ONGC','CENTURYTEX']

print('opening excel file for storing data')
#File used to store data
Excel_File = "BhavCopy_Data.xlsx"

#Sheet data wise 
wb = xw.Book(Excel_File)

CASHBC = wb.sheets("CASHBC")

fnoData = wb.sheets("FNO_Data")

FNOBC = wb.sheets("FNO_BHAVCOPY")

Delievery = wb.sheets("DELIEVERY")

FNO_STOCKOPS = wb.sheets("FNO_STOCKOPS")


#Find no. of Trading days
print('finding trading days out of last 40 days')
To_Date = date.today().strftime('%d-%m-%Y')
#by default keep it to 40
No_of_Days = 40
FDate = date.today()
Fm_Date = FDate-timedelta(days=No_of_Days)
From_Date = Fm_Date.strftime('%d-%m-%Y')
Days = (FDate.day-Fm_Date.day)
now = datetime.now()
dt_string = now.strftime("%d/%m/%Y %H:%M")
print(From_Date)
print(To_Date)

#https://www1.nseindia.com/products/content/equities/indices/historical_index_data.htm
#Through inspect get below link from above web address


#https://www1.nseindia.com/products/dynaContent/equities/indices/historicalindices.jsp?indexType=NIFTY%2050&fromDate=01-03-2020&toDate=01-05-2020


Trading_Days = []
url = "https://www1.nseindia.com/products/dynaContent/equities/indices/historicalindices.jsp?indexType=NIFTY%2050&fromDate="+str(From_Date)+"&toDate="+str(To_Date)
headers = {'User-Agent':'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Mobile Safari/537.36','Referer':'https://www1.nseindia.com/products/content/equities/indices/historical_index_data.htm'}
response = requests.get(url,headers=headers)
soup = BeautifulSoup(response.text,"html.parser")
s=soup.findAll('nobr')
for i in s:
      for m in i:
            Trading_Days.append(m)

print(Trading_Days)

# #Download and Analysis of Cash Bhav Copy used below link to download Cahs bhavcopy
# https://www1.nseindia.com/content/historical/EQUITIES/2020/APR/cm21APR2020bhav.csv.zip

#Creat day wise links by useing trading days from above Trading_Days list
print('Creating links to download Cash bhav copy')

links = []
for i in Trading_Days:
    url = 'https://www1.nseindia.com/content/historical/EQUITIES/'
    link = (url+(i[-4:len(i)])+'/'+(i[-8:len(i)-5].upper()+'/'+'cm'+i.upper().replace('-',"")+'bhav.csv.zip'))
    links.append(link)


#Download all Bhavcopy day wise 
print('Downloading cash bhav copy CSV')

url = links

all = pd.DataFrame()

count = 0
while count < len(url):
    with urlopen(url[count]) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zfile:
            zfile.extractall('D:\Automate the Borring Stuff\CM_Bhavcopy\Bhavcopy_Files')
            r=zfile.namelist()
            for i in r:
                data = pd.read_csv('D:\Automate the Borring Stuff\CM_Bhavcopy\Bhavcopy_Files\{}'.format(i))
                filter = pd.DataFrame(data).drop(['OPEN','CLOSE','ISIN'],axis=1)
                EQ = (filter.loc[filter['SERIES'] =='EQ'])
                i = EQ[EQ['SYMBOL'].isin(fno)].drop(['SERIES','Unnamed: 13'],axis = 1).set_index('SYMBOL')
                i['CHANGE'] = (i['LAST']-i['PREVCLOSE'])/i['PREVCLOSE']*100
                i['RANGE'] = (i['HIGH']-i['LOW'])
                i.drop(['LAST','PREVCLOSE','LOW','HIGH'], axis = 1, inplace = True)
                i['TOTTRDQTY (LACS)']=i['TOTTRDQTY']/100000
                i['TOTTRDVAL (CRORE)']=i['TOTTRDVAL']/10000000
                i=i[['TIMESTAMP','TOTTRDQTY (LACS)','TOTTRDVAL (CRORE)','TOTALTRADES','CHANGE','RANGE']]
                
                all = all.append(i)
                count = count + 1
                print('Data receivied for cash bhav copy {} Trading Day'.format(count))
                
#Processing of Cash Bhav copy files

print('Processing of Cash Bhav copy files')

lastr = len(all['TIMESTAMP'])
Day1 = all[lastr-144:lastr]
Day2 = all[lastr-288:lastr-144]
Day3 = all[lastr-432:lastr-288]
Day4 = all[lastr-576:lastr-432]
Day5 = all[lastr-720:lastr-576]
Day6 = all[lastr-864:lastr-720]
Day7 = all[lastr-1008:lastr-864]
Day8 = all[lastr-1152:lastr-1008]
Day9 = all[lastr-1296:lastr-1152]
Day10 = all[lastr-1440:lastr-1296]
Day11 = all[lastr-1584:lastr-1440]
Day12 = all[lastr-1728:lastr-1584]
Day13 = all[lastr-1872:lastr-1728]
Day14 = all[lastr-2016:lastr-1872]
Day15 = all[lastr-2160:lastr-2016]

TOTTRDQTY_DAY1 = ((Day1['TOTTRDQTY (LACS)']-Day2['TOTTRDQTY (LACS)'])/Day2['TOTTRDQTY (LACS)'])*100
TOTTRDQTY_DAY2 = ((Day2['TOTTRDQTY (LACS)']-Day3['TOTTRDQTY (LACS)'])/Day3['TOTTRDQTY (LACS)'])*100
TOTTRDQTY_DAY3 = ((Day3['TOTTRDQTY (LACS)']-Day4['TOTTRDQTY (LACS)'])/Day4['TOTTRDQTY (LACS)'])*100
TOTTRDQTY_DAY4 = ((Day4['TOTTRDQTY (LACS)']-Day5['TOTTRDQTY (LACS)'])/Day5['TOTTRDQTY (LACS)'])*100
TOTTRDQTY_DAY5 = ((Day5['TOTTRDQTY (LACS)']-Day6['TOTTRDQTY (LACS)'])/Day6['TOTTRDQTY (LACS)'])*100
TOTTRDQTY_DAY6 = ((Day6['TOTTRDQTY (LACS)']-Day7['TOTTRDQTY (LACS)'])/Day7['TOTTRDQTY (LACS)'])*100
TOTTRDQTY_DAY7 = ((Day7['TOTTRDQTY (LACS)']-Day8['TOTTRDQTY (LACS)'])/Day8['TOTTRDQTY (LACS)'])*100
TOTTRDQTY_DAY8 = ((Day8['TOTTRDQTY (LACS)']-Day9['TOTTRDQTY (LACS)'])/Day9['TOTTRDQTY (LACS)'])*100
TOTTRDQTY_DAY9 = ((Day9['TOTTRDQTY (LACS)']-Day10['TOTTRDQTY (LACS)'])/Day10['TOTTRDQTY (LACS)'])*100
TOTTRDQTY_DAY10 = ((Day10['TOTTRDQTY (LACS)']-Day11['TOTTRDQTY (LACS)'])/Day11['TOTTRDQTY (LACS)'])*100
TOTTRDQTY_DAY11 = ((Day11['TOTTRDQTY (LACS)']-Day12['TOTTRDQTY (LACS)'])/Day12['TOTTRDQTY (LACS)'])*100
TOTTRDQTY_DAY12 = ((Day12['TOTTRDQTY (LACS)']-Day13['TOTTRDQTY (LACS)'])/Day13['TOTTRDQTY (LACS)'])*100
TOTTRDQTY_DAY13 = ((Day13['TOTTRDQTY (LACS)']-Day14['TOTTRDQTY (LACS)'])/Day14['TOTTRDQTY (LACS)'])*100


TOTTRDVAL_Day1 = ((Day1['TOTTRDVAL (CRORE)']-Day2['TOTTRDVAL (CRORE)'])/Day2['TOTTRDVAL (CRORE)'])*100
TOTTRDVAL_Day2 = ((Day2['TOTTRDVAL (CRORE)']-Day3['TOTTRDVAL (CRORE)'])/Day3['TOTTRDVAL (CRORE)'])*100
TOTTRDVAL_Day3 = ((Day3['TOTTRDVAL (CRORE)']-Day4['TOTTRDVAL (CRORE)'])/Day4['TOTTRDVAL (CRORE)'])*100
TOTTRDVAL_Day4 = ((Day4['TOTTRDVAL (CRORE)']-Day5['TOTTRDVAL (CRORE)'])/Day5['TOTTRDVAL (CRORE)'])*100
TOTTRDVAL_Day5 = ((Day5['TOTTRDVAL (CRORE)']-Day6['TOTTRDVAL (CRORE)'])/Day6['TOTTRDVAL (CRORE)'])*100
TOTTRDVAL_Day6 = ((Day6['TOTTRDVAL (CRORE)']-Day7['TOTTRDVAL (CRORE)'])/Day7['TOTTRDVAL (CRORE)'])*100
TOTTRDVAL_Day7 = ((Day7['TOTTRDVAL (CRORE)']-Day8['TOTTRDVAL (CRORE)'])/Day8['TOTTRDVAL (CRORE)'])*100
TOTTRDVAL_Day8 = ((Day8['TOTTRDVAL (CRORE)']-Day9['TOTTRDVAL (CRORE)'])/Day9['TOTTRDVAL (CRORE)'])*100
TOTTRDVAL_Day9 = ((Day9['TOTTRDVAL (CRORE)']-Day10['TOTTRDVAL (CRORE)'])/Day10['TOTTRDVAL (CRORE)'])*100
TOTTRDVAL_Day10 = ((Day10['TOTTRDVAL (CRORE)']-Day11['TOTTRDVAL (CRORE)'])/Day11['TOTTRDVAL (CRORE)'])*100
TOTTRDVAL_Day11= ((Day11['TOTTRDVAL (CRORE)']-Day12['TOTTRDVAL (CRORE)'])/Day12['TOTTRDVAL (CRORE)'])*100
TOTTRDVAL_Day12 = ((Day12['TOTTRDVAL (CRORE)']-Day13['TOTTRDVAL (CRORE)'])/Day13['TOTTRDVAL (CRORE)'])*100
TOTTRDVAL_Day13 = ((Day13['TOTTRDVAL (CRORE)']-Day14['TOTTRDVAL (CRORE)'])/Day14['TOTTRDVAL (CRORE)'])*100

#Data storing to Excel
print('Cash Bhav copy process and now Data storing to excel')

CASHBC.range('A1').options(header = True, index = True).value = Day1 
CASHBC.range('H1').options(header = True, index = True).value = Day2
CASHBC.range('O1').options(header = True, index = True).value = Day3
CASHBC.range('V1').options(header = True, index = True).value = Day4
CASHBC.range('AC1').options(header = True, index = True).value = Day5
CASHBC.range('AJ1').options(header = True, index = True).value = Day6
CASHBC.range('AQ1').options(header = True, index = True).value = Day7 
CASHBC.range('AX1').options(header = True, index = True).value = Day8
CASHBC.range('BE1').options(header = True, index = True).value = Day9
CASHBC.range('BL1').options(header = True, index = True).value = Day10
CASHBC.range('BS1').options(header = True, index = True).value = Day11
CASHBC.range('BZ1').options(header = True, index = True).value = Day12
CASHBC.range('CG1').options(header = True, index = True).value = Day13
CASHBC.range('CN1').options(header = True, index = True).value = Day14
CASHBC.range('CU1').options(header = True, index = True).value = Day15

CASHBC.range('DE2').options(header = False, index = False).value = TOTTRDQTY_DAY1
CASHBC.range('DF2').options(header = False, index = False).value = TOTTRDQTY_DAY2
CASHBC.range('DG2').options(header = False, index = False).value = TOTTRDQTY_DAY3
CASHBC.range('DH2').options(header = False, index = False).value = TOTTRDQTY_DAY4
CASHBC.range('DI2').options(header = False, index = False).value = TOTTRDQTY_DAY5
CASHBC.range('DJ2').options(header = False, index = False).value = TOTTRDQTY_DAY6
CASHBC.range('DK2').options(header = False, index = False).value = TOTTRDQTY_DAY7
CASHBC.range('DL2').options(header = False, index = False).value = TOTTRDQTY_DAY8
CASHBC.range('DM2').options(header = False, index = False).value = TOTTRDQTY_DAY9
CASHBC.range('DN2').options(header = False, index = False).value = TOTTRDQTY_DAY10
CASHBC.range('DO2').options(header = False, index = False).value = TOTTRDQTY_DAY11
CASHBC.range('DP2').options(header = False, index = False).value = TOTTRDQTY_DAY12
CASHBC.range('DQ2').options(header = False, index = False).value = TOTTRDQTY_DAY13

CASHBC.range('DU2').options(header = False, index = False).value = TOTTRDVAL_Day1
CASHBC.range('DV2').options(header = False, index = False).value = TOTTRDVAL_Day2
CASHBC.range('DW2').options(header = False, index = False).value = TOTTRDVAL_Day3
CASHBC.range('DX2').options(header = False, index = False).value = TOTTRDVAL_Day4
CASHBC.range('DY2').options(header = False, index = False).value = TOTTRDVAL_Day5
CASHBC.range('DZ2').options(header = False, index = False).value = TOTTRDVAL_Day6
CASHBC.range('EA2').options(header = False, index = False).value = TOTTRDVAL_Day7
CASHBC.range('EB2').options(header = False, index = False).value = TOTTRDVAL_Day8
CASHBC.range('EC2').options(header = False, index = False).value = TOTTRDVAL_Day9
CASHBC.range('ED2').options(header = False, index = False).value = TOTTRDVAL_Day10
CASHBC.range('EE2').options(header = False, index = False).value = TOTTRDVAL_Day11
CASHBC.range('EF2').options(header = False, index = False).value = TOTTRDVAL_Day12
CASHBC.range('EG2').options(header = False, index = False).value = TOTTRDVAL_Day13
print('Cash Bhav copy Data stored to excel successfully')


#Get todays Price for FNO stocks and its list + 52 week high low
print('Now getting data for FNO stocks and its list + 52 week high low')
fno_url = "https://www1.nseindia.com/live_market/dynaContent/live_watch/stock_watch/foSecStockWatch.json"
                    
headers = {'User-Agent':'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Mobile Safari/537.36','Referer':'https://www1.nseindia.com/products/content/equities/indices/historical_index_data.htm'}

response = requests.get(fno_url,headers=headers).json()
data = pd.DataFrame(response['data']).drop(['ptsC','trdVolM','ntP', 'mVal', 'wkhicm_adj', 'wklocm_adj','xDt', 'cAct', 'trdVol','yPC', 'mPC'], axis = 1)
data = data.sort_values('symbol')[['symbol','open','high','low','ltP','per','wkhi','wklo']]
fnoData.range('A4').options(header = False, index = False).value = data
print('Data stroing for FNO OHLC with 52 WH High Low')


#Download and Analysis of FNO Bhav Copy
#Here generating FNO bhav copy links
print('Generating FNO bhav copy links')

links = []
for i in Trading_Days:
    url = 'https://www1.nseindia.com/content/historical/DERIVATIVES/'
    link = (url+(i[-4:len(i)])+'/'+(i[-8:len(i)-5].upper()+'/'+'fo'+i.upper().replace('-',"")+'bhav.csv.zip'))
    links.append(link)
print('Generated FNO bhav copy links')


#Now downloading FNO Bhav copy csv for each tradings day

print('Now downloading FNO Bhav copy csv for each tradings day') 
all = pd.DataFrame()
StockOption = pd.DataFrame()

count = 0
while count < len(links):
    with urlopen(links[count]) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zfile:
            zfile.extractall('D:\Automate the Borring Stuff\CM_Bhavcopy\Bhavcopy_Files')
            r=zfile.namelist()
            for i in r:
                data = pd.read_csv('D:\Automate the Borring Stuff\CM_Bhavcopy\Bhavcopy_Files\{}'.format(i))
                filter = pd.DataFrame(data)
                data = (filter.loc[filter['INSTRUMENT']=='FUTSTK'])
                data = data.drop(['SETTLE_PR','OPEN','HIGH','LOW','CLOSE','CONTRACTS','VAL_INLAKH','INSTRUMENT','OPTION_TYP','STRIKE_PR','Unnamed: 15'],axis = 1)
                data_1 = data.groupby(['SYMBOL']).sum().groupby('SYMBOL').sum()
                data_1['TIMESTAMP'] = i[-17:-8]
                data_1 = data_1[['TIMESTAMP','OPEN_INT','CHG_IN_OI']]
                all=all.append(data_1)
                
                stkopn = (filter.loc[filter['INSTRUMENT']=='OPTSTK']) 
                stkopnCE = (filter.loc[filter['OPTION_TYP']=='CE'])
                stkopnPE = (filter.loc[filter['OPTION_TYP']=='PE'])
                stkopnCE = stkopnCE.drop(['SETTLE_PR','OPEN','HIGH','LOW','CLOSE','CONTRACTS','VAL_INLAKH','INSTRUMENT','Unnamed: 15'],axis = 1)
                STKOPNCE = stkopnCE.groupby(['SYMBOL']).sum().groupby('SYMBOL').sum()
                STKOPNCE['TIMESTAMP'] = i[-17:-8]
                STKOPNCE=STKOPNCE[['TIMESTAMP','OPEN_INT','CHG_IN_OI']]

                stkopnPE = stkopnPE.drop(['SETTLE_PR','OPEN','HIGH','LOW','CLOSE','CONTRACTS','VAL_INLAKH','INSTRUMENT','Unnamed: 15'],axis = 1)
                STKOPNPE = stkopnPE.groupby(['SYMBOL']).sum().groupby('SYMBOL').sum()
                STKOPNPE['TIMESTAMP'] = i[-17:-8]
                STKOPNPE=STKOPNPE[['TIMESTAMP','OPEN_INT','CHG_IN_OI']]
                
                StockOptionCEPE = STKOPNCE.join(STKOPNPE, on = 'SYMBOL',how = 'left', lsuffix='_CE', rsuffix='_PE')
                StockOption=StockOption.append(StockOptionCEPE)
                count = count + 1
                print('Data receivied for fno bhav copy {} Trading Day'.format(count))           


#FNO Bhav copy data segregation
print('FNO Bhav copy data segregation strated')
lastr = len(all['TIMESTAMP'])
Day1 = all[lastr-144:lastr]
Day2 = all[lastr-288:lastr-144]
Day3 = all[lastr-432:lastr-288]
Day4 = all[lastr-576:lastr-432]
Day5 = all[lastr-720:lastr-576]
Day6 = all[lastr-864:lastr-720]
Day7 = all[lastr-1008:lastr-864]
Day8 = all[lastr-1152:lastr-1008]
Day9 = all[lastr-1296:lastr-1152]
Day10 = all[lastr-1440:lastr-1296]
Day11 = all[lastr-1584:lastr-1440]
Day12 = all[lastr-1728:lastr-1584]
Day13 = all[lastr-1872:lastr-1728]
Day14 = all[lastr-2016:lastr-1872]
Day15 = all[lastr-2160:lastr-2016]

lastr = len(StockOption['TIMESTAMP_CE'])
Day1StockOption = StockOption[lastr-147:lastr]
Day2StockOption = StockOption[lastr-294:lastr-147]
Day3StockOption = StockOption[lastr-441:lastr-294]
Day4StockOption = StockOption[lastr-588:lastr-441]
Day5StockOption = StockOption[lastr-735:lastr-588]
Day6StockOption = StockOption[lastr-882:lastr-735]
Day7StockOption = StockOption[lastr-1029:lastr-882]
Day8StockOption = StockOption[lastr-1176:lastr-1029]
Day9StockOption = StockOption[lastr-1323:lastr-1176]
Day10StockOption = StockOption[lastr-1470:lastr-1323]
Day11StockOption = StockOption[lastr-1617:lastr-1470]
Day12StockOption = StockOption[lastr-1764:lastr-1617]
Day13StockOption = StockOption[lastr-1911:lastr-1764]
Day14StockOption = StockOption[lastr-2058:lastr-1911]
Day15StockOption = StockOption[lastr-2205:lastr-2058]

#Data storing of FNO Bhav copy
print('Data storing of FNO Bhav copy in excel started')
FNOBC.range('A1').options(header = True, index = True).value = Day1 
FNOBC.range('E1').options(header = True, index = True).value = Day2
FNOBC.range('I1').options(header = True, index = True).value = Day3
FNOBC.range('M1').options(header = True, index = True).value = Day4
FNOBC.range('Q1').options(header = True, index = True).value = Day5
FNOBC.range('U1').options(header = True, index = True).value = Day6
FNOBC.range('Y1').options(header = True, index = True).value = Day7 
FNOBC.range('AC1').options(header = True, index = True).value = Day8
FNOBC.range('AG1').options(header = True, index = True).value = Day9
FNOBC.range('AK1').options(header = True, index = True).value = Day10
FNOBC.range('AO1').options(header = True, index = True).value = Day11
FNOBC.range('AS1').options(header = True, index = True).value = Day12
FNOBC.range('AW1').options(header = True, index = True).value = Day13
FNOBC.range('BA1').options(header = True, index = True).value = Day14
FNOBC.range('BE1').options(header = True, index = True).value = Day15


FNO_STOCKOPS.range('A1').options(header = True, index = True).value = Day1StockOption 
FNO_STOCKOPS.range('H1').options(header = True, index = True).value = Day2StockOption
FNO_STOCKOPS.range('O1').options(header = True, index = True).value = Day3StockOption
FNO_STOCKOPS.range('V1').options(header = True, index = True).value = Day4StockOption
FNO_STOCKOPS.range('AC1').options(header = True, index = True).value = Day5StockOption
FNO_STOCKOPS.range('AJ1').options(header = True, index = True).value = Day6StockOption
FNO_STOCKOPS.range('AQ1').options(header = True, index = True).value = Day7StockOption
FNO_STOCKOPS.range('AX1').options(header = True, index = True).value = Day8StockOption
FNO_STOCKOPS.range('BE1').options(header = True, index = True).value = Day9StockOption
FNO_STOCKOPS.range('BL1').options(header = True, index = True).value = Day10StockOption
FNO_STOCKOPS.range('BS1').options(header = True, index = True).value = Day11StockOption
FNO_STOCKOPS.range('BZ1').options(header = True, index = True).value = Day12StockOption
FNO_STOCKOPS.range('CG1').options(header = True, index = True).value = Day13StockOption
FNO_STOCKOPS.range('CN1').options(header = True, index = True).value = Day14StockOption
FNO_STOCKOPS.range('CU1').options(header = True, index = True).value = Day15StockOption

print('Data storing of FNO Bhav copy successfull')

# #https:/www1.nseindia.com/archives/equities/mto/MTO_17042020.DAT

#Data download for Delievery %

print('Creating links for Delievery% for each trading days')
delievryurls = []

for i in Trading_Days:
    delievryurl = 'https://www1.nseindia.com/archives/equities/mto/MTO_'
    if i[3:6].upper() == "JAN":
        link=delievryurl+i[0:2]+'01'+i[-4:len(i)]+'.DAT'
        delievryurls.append(link)
    elif i[3:6].upper() == "FEB":
        link=delievryurl+i[0:2]+'02'+i[-4:len(i)]+'.DAT'
        delievryurls.append(link)
    elif i[3:6].upper() == "MAR":
        link=delievryurl+i[0:2]+'03'+i[-4:len(i)]+'.DAT'
        delievryurls.append(link)
    elif i[3:6].upper() == "APR":
        link=delievryurl+i[0:2]+'04'+i[-4:len(i)]+'.DAT'
        delievryurls.append(link)
    elif i[3:6].upper() == "MAY":
        link=delievryurl+i[0:2]+'05'+i[-4:len(i)]+'.DAT'
        delievryurls.append(link)
    elif i[3:6].upper() == "JUN":
        link=delievryurl+i[0:2]+'06'+i[-4:len(i)]+'.DAT'
        delievryurls.append(link)
    elif i[3:6].upper() == "JUL":
        link=delievryurl+i[0:2]+'07'+i[-4:len(i)]+'.DAT'
        delievryurls.append(link)
    elif i[3:6].upper() == "AUG":
        link=delievryurl+i[0:2]+'08'+i[-4:len(i)]+'.DAT'
        delievryurls.append(link)
    elif i[3:6].upper() == "SEP":
        link=delievryurl+i[0:2]+'09'+i[-4:len(i)]+'.DAT'
        delievryurls.append(link)
    elif i[3:6].upper() == "OCT":
        link=delievryurl+i[0:2]+'10'+i[-4:len(i)]+'.DAT'
        delievryurls.append(link)
    elif i[3:6].upper() == "NOV":
        link=delievryurl+i[0:2]+'11'+i[-4:len(i)]+'.DAT'
        delievryurls.append(link)
    elif i[3:6].upper() == "DEC":
        link=delievryurl+i[0:2]+'12'+i[-4:len(i)]+'.DAT'
        delievryurls.append(link)
    
print('succesfully created links for Delievery% for each trading days')

#Data filtering 
print('Data filtering of Delievery % based on FNO list')

all = pd.DataFrame()

for i in delievryurls:
    data = pd.read_csv(i,skiprows = 4, names = ['A','B','SYMBOL','D','TOTALTRQ','DELIEVERQ','DELIEVERY%'])
    data.drop(['A','B'],axis = 1,inplace = True)
    data=(data.loc[data['D']=='EQ'])
    data['DATE'] = i[-12:-4]
    i=data[data['SYMBOL'].isin(fno)]
    data = i.set_index('SYMBOL')
    data.drop(['D'],axis = 1,inplace = True)
    data=data[['DATE','TOTALTRQ','DELIEVERQ','DELIEVERY%']]
    all=all.append(data)

lastr = len(all['TOTALTRQ'])
Day1 = all[lastr-144:lastr]
Day2 = all[lastr-288:lastr-144]
Day3 = all[lastr-432:lastr-288]
Day4 = all[lastr-576:lastr-432]
Day5 = all[lastr-720:lastr-576]
Day6 = all[lastr-864:lastr-720]
Day7 = all[lastr-1008:lastr-864]
Day8 = all[lastr-1152:lastr-1008]
Day9 = all[lastr-1296:lastr-1152]
Day10 = all[lastr-1440:lastr-1296]
Day11 = all[lastr-1584:lastr-1440]
Day12 = all[lastr-1728:lastr-1584]
Day13 = all[lastr-1872:lastr-1728]
Day14 = all[lastr-2016:lastr-1872]
Day15 = all[lastr-2160:lastr-2016]

print('Data filtering of Delievery % based on FNO list done')

print('Data storig of Delievery % in excel strated')

Delievery.range('A1').options(header = True, index = True).value = Day1 
Delievery.range('F1').options(header = True, index = True).value = Day2
Delievery.range('K1').options(header = True, index = True).value = Day3
Delievery.range('P1').options(header = True, index = True).value = Day4
Delievery.range('U1').options(header = True, index = True).value = Day5
Delievery.range('Z1').options(header = True, index = True).value = Day6
Delievery.range('AE1').options(header = True, index = True).value = Day7 
Delievery.range('AJ1').options(header = True, index = True).value = Day8
Delievery.range('AO1').options(header = True, index = True).value = Day9
Delievery.range('AT1').options(header = True, index = True).value = Day10
Delievery.range('AY1').options(header = True, index = True).value = Day11
Delievery.range('BD1').options(header = True, index = True).value = Day12
Delievery.range('BI1').options(header = True, index = True).value = Day13
Delievery.range('BN1').options(header = True, index = True).value = Day14
Delievery.range('BS1').options(header = True, index = True).value = Day15

print('Data storig of Delievery % in excel done')



# # #Creat Day wise file for complete analysis
# print('Creating of csv data file for google sheet')

# finaldata = pd.read_excel('BhavCopy_Data.xlsx',sheet_name = "FNO_Data", skiprows=1)

# finaldata.to_csv('BhavcopyAnalysis_{0}.csv'.format(datetime.now().strftime('%d%m%y')))

print('Successfully complted programe')