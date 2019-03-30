# -*- coding: utf-8 -*-
"""
Created on Wed Nov 23 11:33:47 2016
Finance ARN Consolidation
@author: 13044
"""
from ctypes import *
import pymysql
import pandas as pd
import pdb
import numpy as np
import ddp,gzip
import csv
from datetime import date,timedelta
import os,os.path
import easygui
import time
import glob
from tabulate import tabulate
from prettytable import PrettyTable
import tqdm

pandaspath="D:\\Projects\\Customer Experience - CC\\Finance Refund Process Change\\Pandas Output"
StartDate="2017-09-01" #YYYY-MM-DD

#StartDate=pd.Timedelta(30,'d')
#Hack
StartDate=(pd.to_datetime('today')-pd.Timedelta(30,'D')).strftime('%Y-%m-%d')

"""
Business Context: 
OR Call Picks on 5th day from day of Refund from Myntra's end.
To avoid refund related calls, proactive communication is being set up.
Data will be collated from different PGs and uploaded to Google Site where
Champions are search for the given order group id.
Note- Changes will be made to both Finance and DDP Query 
once Order Transaction ID is implemented 
"""
#manualuploadDir=os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'Raw Data'))
manualuploadDir=r"D:\Projects\Customer Experience - CC\Finance Refund Process Change\Raw Data"
BrokenDataSets=[]

#Checking Internet Connection
import smtplib
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
import sys

print "Checking for Internet Connection\n"
username = "prateek.gupta1@myntra.com"
password = "ynapbhpsddyblroz"

server = smtplib.SMTP("smtp.gmail.com:587")
server.starttls()
#print "Accepted" in server.login(username,password)[1]
try:
    if "Accepted" not in server.login(username,password)[1]:
        print "Internet Not Connected"
        sys.exit("Check your internet Connection and Restart\n")
    else:
        print "Internet Connection Check: Successful\n"
        server.quit()    
except:
    sys.exit("Fatal Error. Internet Connectivity Issue\n")
#class CaseInsensitively(object):
#    def __init__(self, s):
#        self.__s = ''.join(c.lower() for c in s if not c.isspace())
#    def __hash__(self):
#        return hash(self.__s)
#    def __eq__(self, other):
#        # ensure proper comparison between instances of this class
#        try:
#           other = other.__s
#        except (TypeError, AttributeError):
#          try:
#             other = ''.join(c.lower() for c in other if not c.isspace())
#          except:
#             pass
#        return self.__s == other


def clean():
    global df,list_,file_,OrderIDs,MatchingColumns
    df =[]
    list_ = []
    file_=[]
    OrderIDs=[]
    MatchingColumns=[]
parser = lambda x : pd.to_datetime(x)



print "Code Run Started"
#Axis PG
AxisRawData=os.path.abspath(manualuploadDir+'\Axis')
AxisFiles = glob.glob(AxisRawData+ "/*.xlsx")

#Applying  no load for files already read
NoLoadFiles=pd.read_csv('D:\\Projects\\Customer Experience - CC\\Finance Refund Process Change\\Raw Data\\Axis\\NoLoadConfiguration\\NoLoadConfiguration.csv')
NoLoadFilesList=list(NoLoadFiles.FileName)

#Identifying New Files
AxisFiles=list(set(AxisFiles)- set(NoLoadFilesList))
Axisfields = ['ORDER INFO','ARN','RRN NO','TXN DATE','SETTLEMENT DATE','TXN AMOUNT']

#Axisfields =map(str.lower,Axisfields )
# Clean the NoLoad CSV as well
NoLoadConfigurationBkup=pd.read_pickle(r'D:\Projects\Customer Experience - CC\Finance Refund Process Change\Raw Data\Axis\NoLoadConfiguration\NoLoadConfiguration')
list_ = []
list_.append(NoLoadConfigurationBkup)
SheetName=[]
MatchingColumns=[]
df=pd.DataFrame()
#ColumnNames=[]
for file_ in tqdm.tqdm(AxisFiles):
    MatchingColumns=[]
    try:
        SheetName=pd.ExcelFile(file_).sheet_names
        ColumnNames=pd.ExcelFile(file_).parse().columns
        #Matching Column Name
        for basecolumn_ in Axisfields:  
            for filecolumn_ in ColumnNames:
#                print filecolumn_+"_"+basecolumn_+"--->"+str(filecolumn_==basecolumn_)
                if ''.join(c.lower() for c in filecolumn_ if not c.isspace())==''.join(c.lower() for c in basecolumn_ if not c.isspace()):
                    MatchingColumns.append(filecolumn_)
                    break
#        df=pd.DataFrame()
        for sheet in filter(lambda x: 'refund' in x.lower(), SheetName):
            df = df.append(pd.read_excel(file_,index_col=None, header=0,usecols=MatchingColumns,sheetname=sheet,converters={'ORDER INFO':str,'RRN NO':str,'ARN':str}))
#        df = pd.read_excel(file_,index_col=None, header=0,usecols=Axisfields)
        df=df[MatchingColumns]
        df.columns=Axisfields
        list_.append(df)
#        print "File Read!!!",AxisFiles.index(file_)
    except:
        print "Error in Reading File {}{}".format(chr(10),file_)
        BrokenDataSets.append(file_)
AxisData = pd.concat(list_)
#AxisData =AxisData[AxisData['Requested Action'].str.upper()=='REFUND']
#Refund Data comes in two different files
#Pickling Data
AxisData.drop_duplicates().to_pickle(r'D:\Projects\Customer Experience - CC\Finance Refund Process Change\Raw Data\Axis\NoLoadConfiguration\NoLoadConfiguration')
print "Data Successfully Pickled for Next Load"
#Update NoLoadConfiguration
pd.DataFrame(list(set(NoLoadFilesList+AxisFiles)),columns=['FileName']).to_csv('D:\\Projects\\Customer Experience - CC\\Finance Refund Process Change\\Raw Data\\Axis\\NoLoadConfiguration\\NoLoadConfiguration.csv',index=False)
print "NoLoadConfiguration Updated!!!"


##################
AxisData=AxisData[~pd.isnull(AxisData['ARN'])]
                  
##################

AxisData=AxisData[~pd.isnull(AxisData['ORDER INFO'])]
                   
AxisData['TXN AMOUNT'] = AxisData['TXN AMOUNT'].abs()
AxisData =AxisData.drop_duplicates()
#AxisData['PG Name']='Axis' 
#Fixing Date Formats
AxisData['TXN DATE']=pd.to_datetime(AxisData['TXN DATE'],format='%Y-%m-%d %H:%M:%S',coerce=True)
AxisData['SETTLEMENT DATE']=pd.to_datetime(AxisData['SETTLEMENT DATE'],coerce=True)

#Hack
AxisData=AxisData[AxisData['SETTLEMENT DATE']>=\
                  (pd.to_datetime('today')-pd.Timedelta(15,'D'))]
#Finding Missing Dates
AllDates=pd.DataFrame(pd.date_range(np.min(pd.to_datetime(AxisData['TXN DATE'].dt.date)),np.max(pd.to_datetime(AxisData['TXN DATE'].dt.date))),columns=['TXN DATE'])
AxisDates=pd.DataFrame(AxisData['TXN DATE'].dt.date)
AxisDates.columns=['TXN DATE']
AxisDates=AxisDates.drop_duplicates()
AxisDates['TXN DATE']=pd.to_datetime(AxisDates['TXN DATE'])

MissingDatesAll=pd.merge(AllDates,AxisDates,how='outer',on='TXN DATE',suffixes=('_All','_SR'),indicator=True)
MissingDates=MissingDatesAll[MissingDatesAll._merge=='left_only']
MissingDates=MissingDates.drop_duplicates()
#Excluding Weekends 
#MissingDates=MissingDates[~(MissingDates.Date.dt.weekday_name.isin(['Saturday','Sunday']))]
#MissingDates=MissingDates.sort_values(['TXN DATE'])


print "Input File Read"
OrderIDs=AxisData['ORDER INFO'].unique()

max_value = 100000
FinalresultDDP=pd.DataFrame() 
c = len(OrderIDs)/max_value if len(OrderIDs) % max_value ==0 else  len(OrderIDs)/max_value +1

print "DDP query will be running for ",str(c)," times"

for i in range(c):
    #time.sleep(5)
    lista = map(str,OrderIDs[i*max_value:(i+1)*max_value])
#Order Number
                                  
    sql="""     Select
        DISTINCT fp.order_id,
        fp.payment_gateway_name,max(fp.card_bank_name),max(fp.payment_issuer)
            from
        fact_payment fp 
    where
            upper(fp.payment_gateway_name) in ('AXIS') 
            and  fp.order_id in
            (""" + ','.join("'" + item + "'" for item in lista) + ") group by 1,2"""

    QUERY_USER='prateek.gupta1@myntra.com'
    QUERY_OUT_DIR=r'D:\Projects\Customer Experience - CC\Santhosh\DDPDownload'
    QUERY_NAME='DDPAxisData'
    filename=ddp.start(sql,QUERY_USER,QUERY_OUT_DIR,QUERY_NAME)
#    datecols=['taskcreateddate','taskcloseddate','taskduedate','tasklastupdateddate']
    colnames=['OrderNumber','PaymentGateway','BankName','PaymentIssuer']
    DDPData=pd.read_table(filename,compression='gzip',sep=',',header=None,names=colnames,usecols=range(len(colnames)),converters={'OrderNumber':str})
    FinalresultDDP=FinalresultDDP.append(DDPData)
    FinalresultDDP=FinalresultDDP.drop_duplicates()

print FinalresultDDP.shape    

AxisDataMatch=pd.merge(AxisData,FinalresultDDP,how='left',right_on='OrderNumber',left_on='ORDER INFO',suffixes=('_PGData','_DDPData'))
AxisDataMatch=AxisDataMatch.drop_duplicates()

#=======================================================================================
#
#Send Data for Uploading in Google Doc
#
#=======================================================================================

#Declined Order Check and Fraud Check

max_value = 100000
FinalresultDDP=pd.DataFrame() 
c = len(OrderIDs)/max_value if len(OrderIDs) % max_value ==0 else  len(OrderIDs)/max_value +1

print "DDP query will be running for ",str(c)," times"

for i in range(c):
    #time.sleep(5)
    lista = map(str,OrderIDs[i*max_value:(i+1)*max_value])
#Order Number
                                  
    sql="""     Select
        DISTINCT fci.store_order_id,fci.order_status,fci.order_Cancelreason
            from
        fact_core_item fci
    where 
            fci.store_order_id in
            (""" + ','.join("'" + item + "'" for item in lista) + """)
            
            """

    QUERY_USER='prateek.gupta1@myntra.com'
    QUERY_OUT_DIR=r'D:\Projects\Customer Experience - CC\Santhosh\DDPDownload'
    QUERY_NAME='FactCoreItemDataAxis'
    filename=ddp.start(sql,QUERY_USER,QUERY_OUT_DIR,QUERY_NAME)
#    datecols=['taskcreateddate','taskcloseddate','taskduedate','tasklastupdateddate']
    colnames=['OrderNumber','OrderStatus','OrderCancelReason']
    DDPData=pd.read_table(filename,compression='gzip',sep=',',header=None,names=colnames,usecols=range(len(colnames)),converters={'OrderNumber':str})
    FinalresultDDP=FinalresultDDP.append(DDPData)
    FinalresultDDP=FinalresultDDP.drop_duplicates()

print FinalresultDDP.shape    

#Declined Settlement Check
#For Forward, PPS was able to connect to PG,
#PG response was success, but PG was not able to connect back to PPS, 
#and hence Order confirmation page was not shown to customer
#PPS connected to PPS again and voided the transaction, money returned back to customer account
#Used to be OH-5 earlier where Outbound team used to make calls to customer

Check=pd.merge(AxisData,FinalresultDDP[(FinalresultDDP.OrderStatus=='D')&(FinalresultDDP.OrderCancelReason=='Online Payment Issue')],how='inner',right_on='OrderNumber',left_on='ORDER INFO',suffixes=('_PGData','_DDPData'),indicator=True)
DeclinedSettlementCheck=Check[AxisData.columns].drop_duplicates()

#Fraud Check       
Check=pd.merge(AxisData,FinalresultDDP,how='outer',right_on='OrderNumber',left_on='ORDER INFO',suffixes=('_PGData','_DDPData'),indicator=True)
FraudCheck_wVoid=Check[Check._merge=='left_only'][AxisData.columns].drop_duplicates()
#Further filters after running query in DDP
#Converting Date

#AxisDataMatch['TXN DATE']=pd.to_datetime(AxisDataMatch['TXN DATE'])

#AxisDataMatch['SETTLEMENT DATE']=pd.to_datetime(AxisDataMatch['SETTLEMENT DATE'])

AxisDataMatch['ORDER INFO']=AxisDataMatch['ORDER INFO'].astype(str)


#Cleaning Old Dataframe
FinalresultDDP=pd.DataFrame()

#Converting date to epoch
datefilter=time.mktime(time.strptime("2017-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"))
max_value = np.timedelta64(75, 'D')

c = ((np.max(AxisData['TXN DATE']-pd.to_datetime(StartDate,format="%Y-%m-%d")))/max_value \
if (np.max(AxisData['TXN DATE']-pd.to_datetime(StartDate)))/max_value ==0 \
else  (np.max(AxisData['TXN DATE']-pd.to_datetime(StartDate,format="%Y-%m-%d")))/max_value).astype(int) +1
print "Run for Matching ARN Count - DDP query will be running for ",str(c)," times"

for i in range(c):
    #time.sleep(5)
#    lista = map(str,OrderIDs[i*max_value:(i+1)*max_value])
    Date1=(pd.to_datetime(StartDate,format="%Y-%m-%d")+max_value*i).strftime('%Y%m%d')
    Date2=(pd.to_datetime(StartDate,format="%Y-%m-%d")+max_value*(i+1)).strftime('%Y%m%d')          
    print Date1
    print Date2,"XXXXX"
#OR + NetBanking - Mode of Payment = Online - Only RIS will have exact amount refunded
                                  
#    sql="""     SELECT DISTINCT frt.orderid,
#       frt.amount_to_be_refunded,
#       TO_DATE(frt.insert_date,'YYYYMMDD'),
#       frt.gateway_refund_id,
#       frt.refund_status,
#       frt.refund_transaction_id,pp.returnid,pp.id
#        FROM fact_refund_transaction frt
#        left  JOIN fact_payment fp ON frt.orderid = fp.order_id
#        left  JOIN staging.o_payment_plan_execution_status ppes ON ppes.invokertransactionid = frt.client_transaction_id
#        left  JOIN staging.o_payment_plan_instrument_details ppid ON ppid.id = ppes.paymentplaninstrumentdetailid
#        left JOIN staging.o_payment_plan pp on pp.id=ppid.pps_id
#        where  upper(fp.payment_gateway_name) in ('Axis','AxisBLAZE') 
#        and frt.insert_date >= 20171101 
#        and frt.insert_date<= """+str(np.max(AxisData['TXN DATE']).year)+str(np.max(AxisData['TXN DATE']).month).zfill(2)+str(np.max(AxisData['TXN DATE']).day).zfill(2) 

#['OrderNumber','RefundedAmount','RefundAttemptedDate','AxisReferenceID','RefundStatus','RefundTransactionID','ReturnID','PPSID']

#Old Query
#    sql="""     SELECT DISTINCT frt.orderid,
#       frt.amount_to_be_refunded,
#       TO_DATE(frt.insert_date,'YYYYMMDD'),
#       frt.gateway_refund_id,
#       frt.refund_status,
#       frt.refund_transaction_id,pp.returnid,pp.id,ppes.actionType
#        FROM 
#        (select distinct orderid,
#        gateway_refund_id,refund_status,
#        refund_transaction_id,insert_date,client_transaction_id,
#        amount_to_be_refunded from fact_refund_transaction
#        where 
#        insert_date >= """+Date1+""" and
#        insert_date<=""" +Date2+""" and
#        insert_date<= """+str(np.max(AxisData['TXN DATE'].dt.date).strftime('%Y%m%d'))+""" 
#        order by orderid) frt
#        left  JOIN
#        (select distinct order_id,payment_gateway_name
#        from fact_payment
#        where upper(payment_gateway_name) in ('Axis','AxisBLAZE')
#        order by order_id) fp
#        ON frt.orderid = fp.order_id
#        left  JOIN 
#        (select distinct invokertransactionid,actionType,paymentplaninstrumentdetailid
#        from staging.o_payment_plan_execution_status
#        order by invokertransactionid)
#        ppes ON ppes.invokertransactionid = frt.client_transaction_id
#        left  JOIN
#        (select distinct id,pps_id
#        from staging.o_payment_plan_instrument_details
#        order by id)
#        ppid ON ppid.id = ppes.paymentplaninstrumentdetailid
#        left JOIN
#        (select distinct id,returnid
#        from staging.o_payment_plan
#        order by id)
#        pp on pp.id=ppid.pps_id
#        where  1=1"""
################################# Old query - can still work sometimes                                  
#    sql="""     SELECT frt.*,
#       pp.returnid,
#       pp.id,
#       ppes.actionType,
#       CASE ppid.paymentInstrumentType
#               WHEN 1 THEN 'CREDITCARD'
#               WHEN 2 THEN 'DEBITCARD'
#               WHEN 3 THEN 'EMI'
#               WHEN 4 THEN 'NETBANKING'
#               ELSE 'NOT_FOUND'
#        END AS paymentInstrumentType
#        FROM 
#        (SELECT
#        DISTINCT 
#        frt.orderid,
#        frt.amount_to_be_refunded,
#        TO_DATE(frt.insert_date,'YYYYMMDD') as insert_date,
#        frt.gateway_refund_id,
#        frt.refund_status,
#        frt.refund_transaction_id,
#        frt.client_transaction_id
#           FROM
#        fact_refund_transaction frt         
#    left  JOIN
#        fact_payment fp 
#            ON frt.orderid = fp.order_id  
#    where
#        upper(fp.payment_gateway_name) in ('AXIS')  
#        and
#        frt.insert_date >= """+Date1+""" and
#        frt.insert_date<=""" +Date2+""" 
#        ) frt
#        left  JOIN staging.o_payment_plan_execution_status ppes ON ppes.invokertransactionid = frt.client_transaction_id         
#        left  JOIN staging.o_payment_plan_instrument_details ppid ON ppid.id = ppes.paymentplaninstrumentdetailid         
#        left JOIN staging.o_payment_plan pp on pp.id=ppid.pps_id
#        """
# frt.insert_date<= """+str(np.max(AxisData['TXN DATE'].dt.date).strftime('%Y%m%d'))+""" 
########################################
#New QUery
    sql="""     SELECT frt.*,
       ppdenorm.return_id,
       ppdenorm.pps_id,
       case when ppi.itemtype='VAT_ADJ_REFUND' then 'VAT_ADJ_REFUND' else ppdenorm.action_type end,
       CASE ppdenorm.instrument_type
               WHEN 1 THEN 'CREDITCARD'
               WHEN 2 THEN 'DEBITCARD'
               WHEN 3 THEN 'EMI'
               WHEN 4 THEN 'NETBANKING'
               ELSE 'NOT_FOUND'
        END AS paymentInstrumentType,
        ppi.skuid
        FROM 
        (SELECT
        DISTINCT 
        frt.orderid,
        frt.amount_to_be_refunded,
        TO_DATE(frt.insert_date,'YYYYMMDD') as insert_date,
        frt.gateway_refund_id,
        frt.refund_status,
        frt.refund_transaction_id,
        frt.client_transaction_id
           FROM
        fact_refund_transaction frt         
    left  JOIN
        fact_payment fp 
            ON frt.orderid = fp.order_id  
    where
        upper(fp.payment_gateway_name) in ('AXIS')  
        and
        frt.insert_date >= """+Date1+""" and
        frt.insert_date<=""" +Date2+""" 
        ) frt
        left  JOIN o_payment_plan_denorm ppdenorm on ppdenorm.invoker_transaction_id = frt.client_transaction_id 
                left join
        o_payment_plan_item ppi
        on ppi.pps_id=ppdenorm.pps_id
        where ppi.skuid not in ('PPS_9999','PPS_9994')
        and ppi.skuid is not null

        """

    QUERY_USER='prateek.gupta1@myntra.com'
    QUERY_OUT_DIR=r'D:\Projects\Customer Experience - CC\Santhosh\DDPDownload'
    QUERY_NAME='ReturnsData'+Date1
    filename=ddp.start(sql,QUERY_USER,QUERY_OUT_DIR,QUERY_NAME)
#    datecols=['taskcreateddate','taskcloseddate','taskduedate','tasklastupdateddate']
    colnames=['OrderNumber','RefundedAmount','RefundAttemptedDate','AxisReferenceID','RefundStatus','RefundTransactionID','ClientTransactionID','ReturnID','PPSID','PPSActionType','PaymentInstrumentType','SKUID']
    DDPData=pd.read_table(filename,compression='gzip',sep=',',header=None,names=colnames,usecols=range(len(colnames)))
    FinalresultDDP=FinalresultDDP.append(DDPData)
    FinalresultDDP=FinalresultDDP.drop_duplicates()
# TODO: Reference ID is truncating
# TODO: PPS ID is coming as null because of join issue
print FinalresultDDP.shape    
RefundCreated=FinalresultDDP.copy()
AxisData=AxisData.drop_duplicates()
    
#Temporary fix
RefundCreated=RefundCreated.sort_values(['OrderNumber','ReturnID','PPSID','RefundedAmount','RefundAttemptedDate']\
                                        ,ascending=[True,True,True,True,False])
RefundCreated=RefundCreated.drop_duplicates(['OrderNumber','ReturnID','PPSID','RefundedAmount'])

#PPS Success with Void Reply from PG
FraudCheckOuter=pd.merge(FraudCheck_wVoid,RefundCreated,\
                         left_on=['ORDER INFO'],\
                         right_on=['OrderNumber'],how='outer',indicator=True).drop_duplicates()
FraudCheck=FraudCheckOuter[FraudCheckOuter._merge=='left_only']

#PPS Failed but Task Was not Created
PPSIDs=RefundCreated[RefundCreated.RefundStatus<>'SUCCESS']['PPSID'].unique()
max_value = 100000
FinalresultDDP=pd.DataFrame() 
c = len(PPSIDs)/max_value if len(PPSIDs) % max_value ==0 else  len(PPSIDs)/max_value +1

print "DDP query will be running for ",str(c)," times"
c=1
for i in range(c):
    #time.sleep(5)
    lista = map(str,PPSIDs[i*max_value:(i+1)*max_value])
#Order Number
                                  
    sql="""     SELECT distinct
       pp.orderid as orderId,pp.id as pps_Id,
       pp.state as pps_State,
       CASE ppes.status
             WHEN 0 THEN NULL
             ELSE pp.crmRefId
       END as crmRefId,
	   CASE ppes.status
             WHEN 0 THEN 'SUCCESS'
             WHEN 8 THEN 'NOT_ATTEMPTED'
             ELSE 'FAILURE'
       END AS txStatus
       FROM
               o_payment_plan pp
       left JOIN    o_payment_plan_instrument_details ppid 
       ON      pp.id=ppid.pps_Id
       LEFT JOIN
               o_payment_plan_execution_status ppes
       ON      ppid.id=ppes.paymentPlanInstrumentDetailId 
       where   ppes.status not in  (0,8) and pp.id in (""" + ','.join("'" + item + "'" for item in lista) + ") """

    QUERY_USER='prateek.gupta1@myntra.com'
    QUERY_OUT_DIR=r'D:\Projects\Customer Experience - CC\Santhosh\DDPDownload'
    QUERY_NAME='TaskCreationDataAxis'
    filename=ddp.start(sql,QUERY_USER,QUERY_OUT_DIR,QUERY_NAME)
#    datecols=['taskcreateddate','taskcloseddate','taskduedate','tasklastupdateddate']
    colnames=['OrderNumberPPS','PPSID','PPSState','CRMRefID','TxStatus']
    DDPData=pd.read_table(filename,compression='gzip',sep=',',header=None,names=colnames,usecols=range(len(colnames)),converters={'OrderNumber':str})
    FinalresultDDP=FinalresultDDP.append(DDPData)
    FinalresultDDP=FinalresultDDP.drop_duplicates()

print FinalresultDDP.shape    

PPSFailedCases=FinalresultDDP.copy()
PPSTaskFailure=PPSFailedCases[pd.isnull(PPSFailedCases.CRMRefID)]

PPSIssues=PPSFailedCases[pd.isnull(PPSFailedCases.CRMRefID)&(~PPSFailedCases.PPSState.isin(['PPFSM CRM Ticket raised','PPFSM CRM Ticket opened']))]                                  
CRMAPIIssue=PPSFailedCases[pd.isnull(PPSFailedCases.CRMRefID)&(PPSFailedCases.PPSState=='PPFSM CRM Ticket raised')]                              

RefundCreatedTemp=pd.merge(RefundCreated,PPSTaskFailure,on='PPSID',how='outer',indicator=True)
RefundCreatedTemp=RefundCreatedTemp[RefundCreatedTemp._merge=='left_only']
RefundCreated=RefundCreatedTemp.copy()

#Temporary fix
#PPS ID will be unique
#RefundCreated=RefundCreated.sort_values(['OrderNumber','ReturnID','PPSID','RefundedAmount','RefundAttemptedDate']\
#                                        ,ascending=[True,True,True,True,False])
#RefundCreated=RefundCreated.drop_duplicates(['OrderNumber','ReturnID','PPSID','CRMRefID','RefundedAmount'])

#Fixing Floating point errors
AxisData['TXN AMOUNT']=AxisData['TXN AMOUNT'].round(2)
RefundCreated['RefundedAmount']=RefundCreated['RefundedAmount'].round(2)
RefundCreated=RefundCreated.drop('_merge',1)
OuterJoin=pd.merge(RefundCreated[RefundCreated.RefundStatus == 'SUCCESS'],AxisData,left_on=['OrderNumber','RefundedAmount']\
         ,right_on=['ORDER INFO','TXN AMOUNT'],how='outer',indicator=True)
PPSPassedNotCompleted=OuterJoin[OuterJoin._merge=='left_only']

OuterJoin2=pd.merge(RefundCreated[RefundCreated.RefundStatus <> 'SUCCESS'],AxisData,left_on=['OrderNumber','RefundedAmount']\
         ,right_on=['ORDER INFO','TXN AMOUNT'],how='outer',indicator=True)

PPSFailedCompleted=OuterJoin2[OuterJoin2._merge=='both']
PPSFailedCompleted=PPSFailedCompleted.drop("_merge",1)
PPSFailedCompleted['Completed']=1
#Combinations for Failed PPS Tasks

OuterJoin3=pd.merge(RefundCreated[RefundCreated.RefundStatus <> 'SUCCESS'],\
                    PPSFailedCompleted[['ORDER INFO','TXN AMOUNT','Completed']]\
                    ,left_on=['OrderNumber','RefundedAmount']\
         ,right_on=['ORDER INFO','TXN AMOUNT'],how='outer',indicator=True)
Mixed=OuterJoin3[(OuterJoin3.RefundStatus <> 'SUCCESS')&(OuterJoin3._merge=='left_only')]
#Mixed=Mixed.drop(['ORDER INFO','TXN AMOUNT','Completed','_merge'],1)

#Sort Date if last attempted date is required
#Mixed=Mixed.drop_duplicates(['OrderNumber', u'RefundedAmount','AxisReferenceID', u'RefundStatus'])
#PPSFailedNotCompleted=OuterJoin[OuterJoin._merge=='left_only']
Mixed['RefundedAmount']=Mixed['RefundedAmount'].astype(str)

OrderNumberCombination=Mixed.groupby('OrderNumber')['RefundedAmount'].apply(','.join).reset_index()
OrderNumberCombination=OrderNumberCombination.drop_duplicates()

#Dates Combination
OrderNumberDateCombination=Mixed.groupby('OrderNumber')['RefundAttemptedDate'].apply(','.join).reset_index()
OrderNumberDateCombination=OrderNumberDateCombination.drop_duplicates()

OrderNumberCombination=pd.merge(OrderNumberCombination,OrderNumberDateCombination,on='OrderNumber',how='inner')
#Isolated PG Data
AllcompletedTasks=OuterJoin[OuterJoin._merge=='both'][Axisfields]\
            .append(PPSFailedCompleted[Axisfields])
AllcompletedTasks=AllcompletedTasks.drop_duplicates()

Isolated=pd.merge(AxisData,AllcompletedTasks,\
                  on=['ORDER INFO','TXN AMOUNT'],
                  how='outer',indicator=True)
Isolated=Isolated[Isolated._merge=='left_only']
Isolated=Isolated[[u'ORDER INFO', u'ARN_x',u'RRN NO_x', u'TXN DATE_x',
       u'SETTLEMENT DATE_x', u'TXN AMOUNT'
                   ]]
Isolated.columns=Axisfields                  
Isolated=Isolated.drop_duplicates()
#OrderNumberCombination = Combination from ( PPS Task Raised- PPS Task Completed)
ManualNotRefundSelectedColumns=pd.DataFrame()
try:
    MixedFinder=pd.merge(Isolated,OrderNumberCombination,how='right',\
                                    left_on='ORDER INFO',\
                                    right_on='OrderNumber',\
                                    suffixes=('_PG','_PPS'))
    MixedFinder=MixedFinder[~pd.isnull(MixedFinder.OrderNumber)]
    MixedFinder=MixedFinder.drop_duplicates()
    
    MixedFinder['RefundedAmount'].apply(lambda x: sum(float(i) for i in x.split(',')))
    #new_series = []
    
    
    MixedFinder['PPS_Sum'] = MixedFinder['RefundedAmount'].apply(lambda x: sum(float(i) for i in x.split(',')))
    
    MixedFinder['TXN AMOUNT']=MixedFinder['TXN AMOUNT'].astype(str)
    PGCrossSum=MixedFinder.groupby('ORDER INFO')['TXN AMOUNT'].apply(','.join).reset_index()
    PGCrossSum=PGCrossSum.drop_duplicates()
    MixedFinder['PG Date Stamp']=MixedFinder['TXN DATE'].map(str)
    PGCrossSumDateCombination=MixedFinder.groupby('ORDER INFO')['PG Date Stamp'].apply(','.join).reset_index()
    PGCrossSumDateCombination=PGCrossSumDateCombination.drop_duplicates()
    
    PGCrossSum=pd.merge(PGCrossSum,PGCrossSumDateCombination,on='ORDER INFO',how='inner')
    
    MixedFinder=pd.merge(MixedFinder,PGCrossSum,on='ORDER INFO',how='left',suffixes=('_base','_concat'))
    
    MixedFinder['TXN AMOUNT_concat']=MixedFinder['TXN AMOUNT_concat'].astype(str)
    MixedFinder['PG_GroupSum'] = MixedFinder['TXN AMOUNT_concat'].apply(lambda x: sum(float(i) for i in x.split(',')))
    
    ManualNotRefund=MixedFinder[(~(MixedFinder['PPS_Sum'].round(2)==MixedFinder['PG_GroupSum'].round(2)))\
                                &(MixedFinder['PPS_Sum'].round(2)>MixedFinder['PG_GroupSum'].round(2))]
    ManualNotRefund=ManualNotRefund.drop_duplicates()
    ManualNotRefundSelectedColumns=ManualNotRefund[['OrderNumber',\
                                     'PPS_Sum','RefundedAmount','RefundAttemptedDate',\
                                     'PG_GroupSum','TXN AMOUNT_concat','PG Date Stamp_concat']].drop_duplicates()
except:
    pass
                                     #ARNCount=AxisDataMatch[['OrderNumber', 'OrderGroupID', 'ORDER INFO']].groupby(['OrderNumber', 'OrderGroupID'])['ORDER INFO'].nunique().reset_index()

#RefundCreatedwARNCount=pd.merge(RefundCreated,ARNCount,how='left',on=['OrderNumber', 'OrderGroupID'])
#RefundCreatedwARNCount=RefundCreatedwARNCount.fillna(0)
#Finding Order Numbers which were triggered from our end but yet not triggered from PG
#RefundNotTriggered_1=pd.merge(AxisData,FinalresultDDP,how='outer',right_on='OrderNumber',left_on='ORDER INFO',suffixes=('_PGData','_DDPData'),indicator=True)
#RefundNotTriggered_1=RefundNotTriggered_1[RefundNotTriggered_1._merge=='right_only']

#Fixing Dates if Formats are wrong
def func(row):
#    print 3
    if (row['SETTLEMENT DATE']<row['TXN DATE']):
#        print 1
        #Change formats till difference is positive
        if ~pd.isnull(pd.to_datetime(row['TXN DATE'], format='%Y%m%d', errors='coerce')):
#            print 11
            if (pd.to_datetime(row['TXN DATE'], format='%Y%m%d', errors='coerce')\
            <row['SETTLEMENT DATE']):
#                print 111
                return row['SETTLEMENT DATE']-row['TXN DATE']
            elif (pd.to_datetime(row['TXN DATE'].strftime('%d%m%Y'),format='%m%d%Y')\
            <row['SETTLEMENT DATE']):
#                print 112
#                print np.ceil(row['SETTLEMENT DATE']\
#                                -pd.to_datetime(row['TXN DATE'].dt.strftime('%d%m%Y'),format='%m%d%Y')\
#                .astype(float))
                return row['SETTLEMENT DATE']\
                                -pd.to_datetime(row['TXN DATE'].strftime('%d%m%Y'),format='%m%d%Y')\
                
    else:
#        print 2
        return row['SETTLEMENT DATE']-row['TXN DATE']
#AxisDataMatch['DaysToRefund'] = AxisDataMatch[['SETTLEMENT DATE','TXN DATE']].apply(lambda row : func(row) ,axis = 1)
AxisDataMatch['Days2Refund']=np.ceil((AxisDataMatch['SETTLEMENT DATE']-\
pd.to_datetime(AxisDataMatch['TXN DATE'].dt.date))/np.timedelta64(1, 'D')).astype(float)

#AxisDataMatch['Days2RefundFinal']=np.ceil(AxisDataMatch['Days2Refund'])

#Same Order Number, same amount, multiple returns
#Find duplicated records
#Group by - concat cost
#
#Handling Same OrderNumber, same price, multiple refunds
RefundDuplicated=RefundCreated[RefundCreated.duplicated(['OrderNumber','RefundedAmount'],keep=False)]
#Parent Row is Not Dropped
CountofDuplicateReturns=RefundDuplicated[['OrderNumber','RefundedAmount']].groupby(['OrderNumber','RefundedAmount']).size().reset_index()
CountofDuplicateReturns.columns=['OrderNumber', u'RefundedAmount','MultipleReturnsSameCost']
#CountofDuplicateReturns['MultipleReturnsSameCost']=CountofDuplicateReturns['MultipleReturnsSameCost']+1

SameReturnCostMultipleARN=AxisData[['ORDER INFO','TXN AMOUNT']][AxisData.duplicated(['ORDER INFO','TXN AMOUNT'],keep=False)]
CountofMultipleARNS=SameReturnCostMultipleARN[['ORDER INFO','TXN AMOUNT']].groupby(['ORDER INFO','TXN AMOUNT']).size().reset_index()
CountofMultipleARNS.columns=['OrderNumber', u'RefundedAmount','MultipleReturnsSameCost']

#Converting to float
CountofDuplicateReturns['RefundedAmount']=CountofDuplicateReturns['RefundedAmount'].round(2)
CountofMultipleARNS['RefundedAmount']=CountofMultipleARNS['RefundedAmount'].round(2)

CrossVerificationARNDuplicacy=pd.merge(CountofDuplicateReturns,CountofMultipleARNS,on=['OrderNumber', u'RefundedAmount','MultipleReturnsSameCost'],how='outer',indicator=True,suffixes=('_PPS','_PG'))
CrossVerificationARNDuplicacyFailed=CrossVerificationARNDuplicacy[CrossVerificationARNDuplicacy._merge=='left_only']

CrossVerificationARNDuplicacyPassed=CrossVerificationARNDuplicacy[CrossVerificationARNDuplicacy._merge=='both']
#Removing cases if no ARN was present in settlement report for order number

#CrossVerificationARNDuplicacyFailed=pd.merge(CrossVerificationARNDuplicacyFailed,CountofMultipleARNS[['OrderNumber']],on=['OrderNumber'],how='inner')
CrossVerificationARNDuplicacyFailed=CrossVerificationARNDuplicacyFailed.drop_duplicates()

#Removing if they were completed with other combination
RefundCreated['RefundedAmount']=RefundCreated['RefundedAmount'].astype(str)
AxisData['TXN AMOUNT']=AxisData['TXN AMOUNT'].astype(str)

RefundSuperSum=RefundCreated.groupby('OrderNumber')['RefundedAmount'].apply(','.join).reset_index()
RefundSuperSum['RefundedAmountSum']=RefundSuperSum['RefundedAmount'].apply(lambda x: sum(float(i) for i in x.split(','))).round(2)

AxisDataSuperSum=AxisData.groupby('ORDER INFO')['TXN AMOUNT'].apply(','.join).reset_index()
AxisDataSuperSum['TransactionamountSum']=AxisDataSuperSum['TXN AMOUNT'].apply(lambda x: sum(float(i) for i in x.split(','))).round(2)

Completed=pd.merge(AxisDataSuperSum,RefundSuperSum,left_on=['ORDER INFO','TransactionamountSum']\
                   ,right_on=['OrderNumber','RefundedAmountSum'],how='inner')
Completed=Completed.drop_duplicates()

Completed=pd.merge(Completed,RefundCreated,on=['OrderNumber'],how='inner',suffixes=('_completed','_base'))

Completed['RefundedAmount_base']=Completed['RefundedAmount_base'].astype(float).round(2)
CrossVerificationARNDuplicacyFailed=CrossVerificationARNDuplicacyFailed.drop('_merge',1)
DuplicateBulkClosed=pd.merge(CrossVerificationARNDuplicacyFailed,Completed,left_on=['OrderNumber'],\
                             right_on=['OrderNumber'],how='outer',indicator=True)
CrossVerificationARNDuplicacyFailed=DuplicateBulkClosed[DuplicateBulkClosed._merge=='left_only'].drop_duplicates()
print CrossVerificationARNDuplicacyFailed.shape

#Refund Count for finding %Completed
RefundCount_PPS=pd.pivot_table(RefundCreated,index=pd.to_datetime(RefundCreated.RefundAttemptedDate),columns=['PPSActionType'],values='OrderNumber',aggfunc=len,fill_value=0).sort_index()

#TODO: Trying to run SMS Module
#from D:\Projects\Customer Experience - CC\Finance Refund Process Change import func_name
#from ..Axis.SMSCode.SMSCode.py  import SMSCode

#Sample For SMS Communication
SampleDate=pd.to_datetime('today')-pd.Timedelta(6,'D')
#PPSPassedCompleted[PPSPassedCompleted.OrderNumber=='104658561187864234001']

#x=OuterJoin[(OuterJoin._merge=='both')&~pd.isnull(OuterJoin.ReturnID)].duplicated('OrderNumber',keep=False)
#Removing Single  Return ID Criteria
#x=OuterJoin[(OuterJoin._merge=='both')&~pd.isnull(OuterJoin.ReturnID)].duplicated('OrderNumber',keep=False)
PPSPassedCompleted=OuterJoin[(OuterJoin._merge=='both')&~pd.isnull(OuterJoin.ReturnID)]
#                             Remove -x criteria from above line
SampleListComplete=pd.merge(AxisDataMatch[['OrderNumber','BankName','PaymentIssuer']],PPSPassedCompleted,how='inner',on='OrderNumber').drop_duplicates()
#PPSPassedCompleted['SETTLEMENT DATE']>SampleDate
ReturnIDs=SampleListComplete['ReturnID'].astype(int).unique()

max_value = 100000
FinalresultDDP=pd.DataFrame() 
c = len(ReturnIDs)/max_value if len(ReturnIDs) % max_value ==0 else  len(ReturnIDs)/max_value +1

print "DDP query will be running for ",str(c)," times"

for i in range(c):
    #time.sleep(5)
    lista = map(str,ReturnIDs[i*max_value:(i+1)*max_value])
#Order Number
                                  
#    sql=""" Select distinct base.BaseReturnID,
#            base.BaseOrderID,
#            superpool.*,
#            osla.ShippingPhoneNumber
#            from 
#                (select distinct 
#                dce.email,fr.return_id as BaseReturnID,fr.order_id as BaseOrderID
#                from fact_returns fr
#                left JOIN dim_customer_email dce
#                on dce.uid=fr.customer_id
#                where  fr.return_id in
#                (""" + ','.join("'" + item + "'" for item in lista) + """) ) base
#             left join
#             (select distinct 
#            dce.email,fr.return_id,fr.return_status,fr.is_refunded,order_id as Mapping
#            from fact_returns fr
#            left JOIN dim_customer_email dce
#            on dce.uid=fr.customer_id
#            where fr.return_created_date>=to_char(dateadd(day, -5, to_date(current_date, 'YYYY-MM-DD')), 'YYYYMMDD') 
#            and fr.return_status not in ('CFDC',		
#                   'RJUP',		
#                   'RNC',		
#                   'RQCF',		
#                   'RQCP',		
#                   'RQF',		
#                   'RQSF',		
#                   'RRD',		
#                   'RRRS',		
#                   'RRS',		
#                   'RSD')	
#            ) superpool   
#            on 
#            superpool.email=base.email
#            left join
#            (select distinct order_id,max(shipping_customer_mobile)
#            as ShippingPhoneNumber 
#             from fact_orderitem_sla
#             where order_created_date>=20171101  group by 1) osla
#            on trim(osla.order_id)::varchar=trim(superpool.Mapping)::varchar
#		"""
    sql=""" select distinct 
           dce.email,
           fr.return_id,
           fr.order_id,
           osla.ShippingPhoneNumber,
           dp.brand,
            dp.brand_group,
            dp.master_category,
            dp.gender,
            dp.style_name,
            dp.article_type
           from fact_returns fr
           left JOIN dim_customer_email dce
           on dce.uid=fr.customer_id
           left join
           (select distinct order_id,max(shipping_customer_mobile) as ShippingPhoneNumber 
            from fact_orderitem_sla where order_created_date>=20171101  group by 1) osla 
           on trim(osla.order_id)::varchar=trim(fr.order_id)::varchar
           left JOIN dim_product dp on dp.id=fr.product_id
           left JOIN dim_courier dcc on dcc.id=fr.courier_id
           where  fr.return_id in (""" + ','.join("'" + item + "'" for item in lista) + """) 
           """
    QUERY_USER='prateek.gupta1@myntra.com'
    QUERY_OUT_DIR=r'D:\Projects\Customer Experience - CC\Santhosh\DDPDownload'
    QUERY_NAME='EmailAxisData'
    filename=ddp.start(sql,QUERY_USER,QUERY_OUT_DIR,QUERY_NAME)
#    datecols=['taskcreateddate','taskcloseddate','taskduedate','tasklastupdateddate']
#    colnames=['ReturnID','OrderID','EmailID','ReturnID_OtherReturns','ReturnStatus','IsRefunded','ShippingPhoneNumber']
    colnames=['EmailID','ReturnID','OrderID','ShippingPhoneNumber',\
              'Brand','BrandGroup','MasterCategory','Gender',\
              'StyleName','ArticleType']
    #    colnames=['ReturnID','OrderID','EmailID','ReturnID_OtherReturns','ReturnStatus','IsRefunded','ShippingPhoneNumber']
    DDPData=pd.read_table(filename,compression='gzip',sep=',',header=None,names=colnames,usecols=range(len(colnames)),converters={'OrderNumber':str})
    FinalresultDDP=FinalresultDDP.append(DDPData)
    FinalresultDDP=FinalresultDDP.drop_duplicates()

print FinalresultDDP.shape    
EmailIDData=FinalresultDDP.copy()

#Boolean=EmailIDData.duplicated('email','',keep=False)
#SampleListFinal=SampleList[~ Boolean]


SampleList=pd.merge(SampleListComplete,EmailIDData,how='inner',on='ReturnID').drop_duplicates().drop('_merge',1)


##Processing for Duplicate Returns
#
##Removing Partial Refunds
#CrossVerificationARNDuplicacyFailed['SameItemPartialRefund']=1
#
#SampleList=pd.merge(SampleList,CrossVerificationARNDuplicacyFailed[['OrderNumber','SameItemPartialRefund']],on='OrderNumber',how='left')
#SampleList=SampleList[SampleList['SameItemPartialRefund']!=1]
#
#SampleList=pd.merge(SampleList,CrossVerificationARNDuplicacyPassed[['OrderNumber']],on='OrderNumber',how='inner')
#
#SampleList['ReturnID']=SampleList['ReturnID'].astype(int)
#
#SampleListOrderIDCombination=SampleList[['OrderNumber','OrderID']].drop_duplicates()
#SampleListOrderIDCombination['OrderID']=SampleListOrderIDCombination['OrderID'].astype(str)
#OrderIDCombination=SampleListOrderIDCombination.groupby(['OrderNumber'])['OrderID'].apply(','.join).reset_index()
#
#
##Concat Return ID remove duplicates
#SampleListARNCombination=SampleList[['ARN','RefundedAmount','OrderNumber','SETTLEMENT DATE']].drop_duplicates()
##Concat ARN Number Cost Combination
##Date Combination
#SampleListDateCombination=SampleList[['OrderNumber','SETTLEMENT DATE']].drop_duplicates()
#SampleListDateCombination['SETTLEMENT DATE']=SampleListDateCombination['SETTLEMENT DATE'].astype(str)
#DateCombination=SampleListDateCombination.groupby(['OrderNumber'])['SETTLEMENT DATE'].apply(','.join).reset_index()
#
#
#ARNCombination=SampleListARNCombination.groupby(['OrderNumber'])['ARN'].apply(','.join).reset_index()
#
#ARNCombination=SampleListARNCombination.groupby(['OrderNumber',])['ARN'].apply(','.join).reset_index()
#SampleListARNCombination['RefundedAmount']=SampleListARNCombination['RefundedAmount'].astype(str)
#
#CostCombination=SampleListARNCombination.groupby(['OrderNumber'])['RefundedAmount'].apply(','.join).reset_index()
#
#ARNCostCombination=pd.merge(ARNCombination,CostCombination,on=['OrderNumber'],how='left')
#
#ReturnIDUnique=SampleList[['OrderNumber','ReturnID']].drop_duplicates()
#ReturnIDUnique['ReturnID']=ReturnIDUnique['ReturnID'].astype(str)
#ReturnIDCombination=ReturnIDUnique.groupby(['OrderNumber'])['ReturnID'].apply(','.join).reset_index()
##Join Back on Order Number
#ARNCostReturnIDCombination=pd.merge(ARNCostCombination,ReturnIDCombination,on=['OrderNumber'],how='left')
#
#ARNCostReturnIDDateCombination=pd.merge(DateCombination,ARNCostReturnIDCombination,on='OrderNumber',how='left',suffixes=('_all Comb','_date')).drop_duplicates()
#
#ARNCostReturnIDDateOrderIDCombination=pd.merge(ARNCostReturnIDDateCombination,OrderIDCombination,on='OrderNumber',how='left')
#
##Adding  Phone Numbers 
#SampleListforDuplicateReturn=pd.merge(ARNCostReturnIDDateOrderIDCombination,\
#                                      SampleList[['OrderNumber','ShippingPhoneNumber','EmailID']],on='OrderNumber',how='left').drop_duplicates()
#############################################
#042617 - Commenting code for multiple return same cost 
#Processing for Normal Orders

SampleList=pd.merge(SampleListComplete,EmailIDData,how='inner',on='ReturnID').drop_duplicates().drop('_merge',1)
CrossVerificationARNDuplicacyFailed['SameItemPartialRefund']=1
#Removing Partial Refunds
SampleList=pd.merge(SampleList,CrossVerificationARNDuplicacyFailed[['OrderNumber','SameItemPartialRefund']],on='OrderNumber',how='left')
SampleList=SampleList[SampleList['SameItemPartialRefund']!=1]

#SampleList=SampleList.drop('_merge',axis=1)
SampleList=pd.merge(SampleList,CountofDuplicateReturns[['OrderNumber']],on='OrderNumber',how='outer',indicator=True)
SampleList=SampleList[SampleList._merge=='left_only']

try:
    SampleList.drop('_merge',axis=1,inplace=True)
except:
    pass
SampleList=pd.merge(SampleList,CountofMultipleARNS[['OrderNumber']],on='OrderNumber',how='outer',indicator=True)
SampleList=SampleList[SampleList._merge=='left_only']


#SampleList=SampleList[SampleList['SETTLEMENT DATE']>SampleDate]
#SampleList=SampleList.sort_values(['OrderNumber','ReturnID','RefundedAmount','ARN']).sort_values(['ARN'],ascending=False).\
#                drop_duplicates(['OrderNumber','ReturnID','RefundedAmount','ARN'])
#
#SampleList['UpperARN']=SampleList['ARN'].shift(-1)
#SampleList['SameRowARN']=SampleList['ARN']
#SampleList=SampleList[SampleList['UpperARN']!=SampleList['SameRowARN']]

SampleList['ReturnID']=SampleList['ReturnID'].astype(int)
SampleList['ShippingPhoneNumber']=SampleList['ShippingPhoneNumber'].astype(str)

#SampleList=SampleList[SampleList._merge=='left_only']

def changeencode(data):
    cols = data.columns
    for col in cols:
        data[col]=data[col].astype(str)                
        if data[col].dtype == 'O':
            data[col] = data[col].str.decode('utf-8').str.encode('ascii', 'ignore')
    return data  
          
changeencode(SampleList)                   

# TODO: Create Cancellation List
CancellationData=RefundCreated[RefundCreated.PPSActionType=='CANCELLATION']
CancellationData=pd.merge(CancellationData,Completed[['OrderNumber']].drop_duplicates()\
                          ,on='OrderNumber',how='inner').drop_duplicates()

######################
#Changing logic for Cancellation - SKU ID has PPS tag

#########################3
# SKUID was coming as float
##########################
CancellationData.SKUID=map(int,CancellationData.SKUID)

OrderNumberwSKUID=CancellationData.OrderNumber+map(str,CancellationData.SKUID)

#Hard Coding PPS tag
sub='PPS'
OrderNumberwSKUIDCleaned=[s for s in OrderNumberwSKUID if sub not in s]

max_value = 100000
FinalresultDDP=pd.DataFrame() 
c = len(OrderNumberwSKUIDCleaned)/max_value if len(OrderNumberwSKUIDCleaned) % max_value ==0 else  len(OrderNumberwSKUIDCleaned)/max_value +1

print "DDP query will be running for ",str(c)," times"

for i in range(c):
    #time.sleep(5)
    lista = map(str,OrderNumberwSKUIDCleaned[i*max_value:(i+1)*max_value])

    sql=""" select distinct 
           dce.email,fr.store_order_id,
           max(osla.ShippingPhoneNumber),
           max(fp.card_bank_name),max(fp.payment_issuer),
           listagg(fr.order_id, ', ')
           from fact_core_item fr
           left join 
           fact_payment fp
           on fp.order_id=fr.store_order_id
           left JOIN dim_customer_email dce
           on dce.uid=fr.idcustomer
           left join
           (select distinct order_id,max(shipping_customer_mobile) as ShippingPhoneNumber 
            from fact_orderitem_sla where order_created_date>=20171101  group by 1) osla 
           on trim(osla.order_id)::varchar=trim(fr.order_id)::varchar
           where  
           osla.ShippingPhoneNumber is not null and
           fp.card_bank_name is not null
           and 
           fp.payment_issuer is not null
           and 
               (fr.item_cancelled_date is not null 
               or fr.order_cancel_date is not null )
               and (fr.order_cancel_date > 19700101 or fr.item_cancelled_date>19700101)
           and trim(fr.store_order_id)||trim(fr.sku_id) in (""" + ','.join("'" + item + "'" for item in lista) + """) 
                      group by 1,2
                      
           """
    QUERY_USER='prateek.gupta1@myntra.com'
    QUERY_OUT_DIR=r'D:\Projects\Customer Experience - CC\Santhosh\DDPDownload'
    QUERY_NAME='EmailCancellationAxisData'
    filename=ddp.start(sql,QUERY_USER,QUERY_OUT_DIR,QUERY_NAME)
#    datecols=['taskcreateddate','taskcloseddate','taskduedate','tasklastupdateddate']
#    colnames=['ReturnID','OrderID','EmailID','ReturnID_OtherReturns','ReturnStatus','IsRefunded','ShippingPhoneNumber']
    colnames=['EmailID','OrderNumber','ShippingPhoneNumber',\
              'BankName','PaymentIssuer','OrderIDList']
    #    colnames=['ReturnID','OrderID','EmailID','ReturnID_OtherReturns','ReturnStatus','IsRefunded','ShippingPhoneNumber']
    DDPData=pd.read_table(filename,compression='gzip',sep=',',header=None,names=colnames,usecols=range(len(colnames)),converters={'OrderNumber':str})
    FinalresultDDP=FinalresultDDP.append(DDPData)
    FinalresultDDP=FinalresultDDP.drop_duplicates()

print FinalresultDDP.shape

#Not Cleaning FinalresultDDP so that data could be appended
OrderNumberwoSKUIDCleaned=[s[:21] for s in OrderNumberwSKUID if sub  in s]

c = len(OrderNumberwoSKUIDCleaned)/max_value if len(OrderNumberwoSKUIDCleaned) % max_value ==0 else  len(OrderNumberwoSKUIDCleaned)/max_value +1

print "DDP query will be running for ",str(c)," times"

for i in range(c):
    #time.sleep(5)
    lista = map(str,OrderNumberwoSKUIDCleaned[i*max_value:(i+1)*max_value])

    sql=""" select distinct 
           dce.email,fr.store_order_id,
           max(osla.ShippingPhoneNumber),
           max(fp.card_bank_name),max(fp.payment_issuer),
           listagg(fr.order_id, ', ')
           from fact_core_item fr
           left join 
           fact_payment fp
           on fp.order_id=fr.store_order_id
           left JOIN dim_customer_email dce
           on dce.uid=fr.idcustomer
           left join
           (select distinct order_id,max(shipping_customer_mobile) as ShippingPhoneNumber 
            from fact_orderitem_sla where order_created_date>=20171101  group by 1) osla 
           on trim(osla.order_id)::varchar=trim(fr.order_id)::varchar
           where  
           osla.ShippingPhoneNumber is not null and
           fp.card_bank_name is not null
           and 
           fp.payment_issuer is not null
           and 
               (fr.item_cancelled_date is not null 
               or fr.order_cancel_date is not null )
               and (fr.order_cancel_date > 19700101 or fr.item_cancelled_date>19700101)
           and trim(fr.store_order_id) in (""" + ','.join("'" + item + "'" for item in lista) + """) 
                      group by 1,2
                      
           """
    QUERY_USER='prateek.gupta1@myntra.com'
    QUERY_OUT_DIR=r'D:\Projects\Customer Experience - CC\Santhosh\DDPDownload'
    QUERY_NAME='EmailCancellationAxisData'
    filename=ddp.start(sql,QUERY_USER,QUERY_OUT_DIR,QUERY_NAME)
#    datecols=['taskcreateddate','taskcloseddate','taskduedate','tasklastupdateddate']
#    colnames=['ReturnID','OrderID','EmailID','ReturnID_OtherReturns','ReturnStatus','IsRefunded','ShippingPhoneNumber']
    colnames=['EmailID','OrderNumber','ShippingPhoneNumber',\
              'BankName','PaymentIssuer','OrderIDList']
    #    colnames=['ReturnID','OrderID','EmailID','ReturnID_OtherReturns','ReturnStatus','IsRefunded','ShippingPhoneNumber']
    DDPData=pd.read_table(filename,compression='gzip',sep=',',header=None,names=colnames,usecols=range(len(colnames)),converters={'OrderNumber':str})
    FinalresultDDP=FinalresultDDP.append(DDPData)
    FinalresultDDP=FinalresultDDP.drop_duplicates()

#######################

# PPS State could be shipping charges???? 
EmailIDDataCancellation=FinalresultDDP.copy()

#Separate full order cancellation and partial order cancellation
x=CancellationData.duplicated('OrderNumber',keep=False)
SingleCancellation=CancellationData[~x]
MultipleCancellation=CancellationData[x]

SingleCancellation=pd.merge(SingleCancellation,EmailIDDataCancellation,\
                            on='OrderNumber',how='left').drop_duplicates()
MultipleCancellation=pd.merge(MultipleCancellation,EmailIDDataCancellation,\
                            on='OrderNumber',how='left').drop_duplicates()

#Get ARN Number - No Task Is Created

SingleCancellation=pd.merge(SingleCancellation,AxisData,left_on=['OrderNumber','RefundedAmount']\
         ,right_on=['ORDER INFO','TXN AMOUNT'],how='inner').drop_duplicates()

#Removing x=CancellationData.duplicated('OrderNumber',keep=False)
y=SingleCancellation.duplicated('OrderNumber',keep=False)
SingleCancellation=SingleCancellation[~y]



MultipleCancellation=pd.merge(MultipleCancellation,AxisData,left_on=['OrderNumber','RefundedAmount']\
         ,right_on=['ORDER INFO','TXN AMOUNT'],how='inner').drop_duplicates()
#Remove duplicate ARN
#Removing Partial Refunds
MultipleCancellation=pd.merge(MultipleCancellation,CrossVerificationARNDuplicacyFailed[['OrderNumber','SameItemPartialRefund']],on='OrderNumber',how='left')
MultipleCancellation=MultipleCancellation[MultipleCancellation['SameItemPartialRefund']!=1]

#SampleList=SampleList.drop('_merge',axis=1)
try:
    MultipleCancellation.drop('_merge',axis=1,inplace=True)
except:
    pass
MultipleCancellation=pd.merge(MultipleCancellation,CountofDuplicateReturns[['OrderNumber']],on='OrderNumber',how='outer',indicator=True)
MultipleCancellation=MultipleCancellation[MultipleCancellation._merge=='left_only']

try:
    MultipleCancellation.drop('_merge',axis=1,inplace=True)
except:
    pass
MultipleCancellation=pd.merge(MultipleCancellation,CountofMultipleARNS[['OrderNumber']],on='OrderNumber',how='outer',indicator=True)
MultipleCancellation=MultipleCancellation[MultipleCancellation._merge=='left_only']


MultipleCancellation['ShippingPhoneNumber']=MultipleCancellation['ShippingPhoneNumber'].astype(str)
SingleCancellation['ShippingPhoneNumber']=SingleCancellation['ShippingPhoneNumber'].astype(str)

changeencode(MultipleCancellation)         
changeencode(SingleCancellation)         

#Exporting Datasets
                              
from pandas import ExcelWriter
#import time
writer = ExcelWriter(pandaspath+"//AxisData_"+str(time.strftime("%m%d%Y"))+".xlsx")
AxisDataMatch.to_excel(writer,'AxisData',index=False)
ManualNotRefundSelectedColumns.to_excel(writer,'FinanceTaskNotRefunded',index=False)
PPSPassedNotCompleted[[u'OrderNumber', u'RefundedAmount', u'RefundAttemptedDate',
       u'AxisReferenceID', u'RefundStatus', u'ORDER INFO']].to_excel(writer,'PPSPassedNotCompleted',index=False)
#RefundCreatedwARNCount[(RefundCreatedwARNCount['RefundCount']<>0)&(RefundCreatedwARNCount['ORDER INFO']==0)][['OrderNumber','OrderGroupID']].to_excel(writer,'RefundCreatedARNMatch',index=False)
#AxisDataMatch[(pd.isnull(AxisDataMatch['ORDER INFO']))|(pd.isnull(AxisDataMatch['SETTLEMENT DATE']))].to_excel(writer,'SettlementMissingData',index=False)
CrossVerificationARNDuplicacyFailed[['OrderNumber','RefundedAmount','MultipleReturnsSameCost']].to_excel(writer,'DuplicateOrderReturn',index=False)
CRMAPIIssue.to_excel(writer,'CRMAPIIssues',index=False)
PPSIssues.to_excel(writer,'PPSIssues',index=False)
#DeclinedSettlementCheck.to_excel(writer,'DeclinedSettlementCheck',index=False)
FraudCheck[Axisfields].to_excel(writer,'FraudCheck',index=False)
RefundCount_PPS.to_excel(writer,'RefundCount_PPS',index=True)
MissingDates.to_excel(writer,'MissingDates_SR',index=False)
#SampleListforDuplicateReturn.to_excel(writer,'SampleListMultiple',index=False)
SampleList.to_excel(writer,'SampleList',index=False, encoding='utf8')
SingleCancellation.to_excel(writer,'SingleCancel',index=False)
MultipleCancellation.to_excel(writer,'MultipleCancel',index=False)

#SampleListFinal.to_excel(writer,'SampleListFinal',index=False)
writer.save()
print "Excel Exported!!! Now adding comments"
#Adding Comments
#import win32com.client
##pythoncom.CoInitialize()
#xl = win32com.client.DispatchEx('Excel.Application')
##xl.Visible = 1
#wb = xl.Workbooks.Open(pandaspath+"//AxisData_"+str(time.strftime("%m%d%Y"))+".xlsx")
#sheet = wb.ActiveSheet
##Order Number
#sheet.Range("A1").AddComment()
#sheet.Range("A1").Comment.Text("Same as Order Number")
##ARN
#sheet.Range("B1").AddComment()
#sheet.Range("B1").Comment.Text("ARN")
##Refund Initiation Date
#sheet.Range("D1").AddComment()
#sheet.Range("D1").Comment.Text("Refund Initiation Date")
##Amount Refunded To Bank Account
#sheet.Range("E1").AddComment()
#sheet.Range("E1").Comment.Text("Refunded Amount To Be Reflected in Bank Statement")
##Amount Refunded To Bank Account
#sheet.Range("F1").AddComment()
#sheet.Range("F1").Comment.Text("Amount Refunded To Bank Account")
#
#wb.Save()
#wb.Close()
#xl.Quit()
##wb.Save()
##wb.SaveAs(pandaspath+"//AxisData_"+str(time.strftime("%m%d%Y"))+".xlsx")
##wb.Close(True)
##wb=None
##xl=None
#
##pythoncom.CoUninitialize()()
#xl=None
#del xl
#xl.Quit
#xl.Close


#Cleaning Temp DataSets
clean()
##Axis Blaze PG
#AxisBlazeRawData=os.path.abspath(manualuploadDir+'\Axis Blaze')
#AxisBlazeFiles = glob.glob(AxisBlazeRawData+ "/*.xlsx")
#
#AxisBlazefields = ['ORDER INFO', 'Issuer Txn ref number','Transaction Type','TXN DATE','SETTLEMENT DATE','TXN AMOUNT']
#list_ = []
#for file_ in AxisBlazeFiles:
#    try:
#        df = pd.read_excel(file_,index_col=None, header=0,usecols=AxisBlazefields)
#        list_.append(df)
#    except:
#        print "Error in Reading File {}{}".format(chr(10),file_)
#        BrokenDataSets.append(file_)
#AxisBlazeData = pd.concat(list_)
#AxisBlazeData =AxisBlazeData.drop_duplicates()
#AxisBlazeData['PG Name']='Axis Blaze' 
#
#OrderIDs=AxisData['ORDER INFO'].unique()
#
#max_value = 50000
#FinalresultDDP=pd.DataFrame() 
#c = len(OrderIDs)/max_value if len(OrderIDs) % max_value ==0 else  len(OrderIDs)/max_value +1
#
#print "DDP query will be running for ",str(c)," times"
#
#for i in range(c):
#    #time.sleep(5)
#    lista = map(str,OrderIDs[i*max_value:(i+1)*max_value])
##Order Number
#                                  
#    sql="""     Select
#        DISTINCT frt.orderid,
#        fp.card_bank_name
#            from
#        fact_refund_transaction frt  
#    LEft JOIN
#        fact_payment fp on frt.orderid = fp.order_id
#    LEft JOIN
#        fact_core_item fci 
#            on fp.order_id = fci.store_order_id   where
#            frt.insert_date >= 20171101  
#and  refund_status = 'SUCCESS' and upper(fp.payment_gateway_name) = 'Axis' and  frt.orderid in (""" + ','.join("'" + item + "'" for item in lista) + ")"""
#
#    QUERY_USER='prateek.gupta1@myntra.com'
#    QUERY_OUT_DIR=r'D:\Projects\Customer Experience - CC\Santhosh\DDPDownload'
#    QUERY_NAME='DDPAxisBlazeData'
#    filename=ddp.start(sql,QUERY_USER,QUERY_OUT_DIR,QUERY_NAME)
##    datecols=['taskcreateddate','taskcloseddate','taskduedate','tasklastupdateddate']
#    colnames=['OrderNumber','BankName']
#    DDPData=pd.read_table(filename,compression='gzip',sep=',',header=None,names=colnames,usecols=range(len(colnames)))
#    FinalresultDDP=FinalresultDDP.append(DDPData)
#    FinalresultDDP=FinalresultDDP.drop_duplicates()
#
#print FinalresultDDP.shape    
#
#AxisBlazeData=pd.merge(AxisBlazeData,FinalresultDDP,how='left',right_on='OrderNumber',left_on='ORDER INFO',suffixes=('_PGData','_DDPData'))
#AxisBlazeData=AxisBlazeData.drop_duplicates()
#
##Converting Date
#AxisBlazeData['TXN DATE']=pd.to_datetime(AxisBlazeData['TXN DATE'])
#AxisBlazeData['SETTLEMENT DATE']=pd.to_datetime(AxisBlazeData['SETTLEMENT DATE'])
#
##Cleaning Temp DataSets
#clean()

#Axis PG
#AxisRawData=os.path.abspath(manualuploadDir+'\Axis')
#AxisFiles = glob.glob(AxisRawData+ "/*.xlsx")
#
#Axisfields = ['ORDER INFO','ARN NO','REC FMT','TRANS DATE','SETTLE DATE','TXN AMOUNT']
#list_ = []
#for file_ in AxisFiles:
#    try:
#        df = pd.read_excel(file_,index_col=None, header=0,usecols=Axisfields )
#        list_.append(df)
#    except:
#        print "Error in Reading File {}{}".format(chr(10),file_)
#        BrokenDataSets.append(file_)
#AxisData = pd.concat(list_)
#AxisData=AxisData.drop_duplicates()
#AxisData['PG Name']='Axis' 
#
##Cleaning Temp DataSets
#clean()
#
##ICICI PG
#ICICIRawData=os.path.abspath(manualuploadDir+'\ICICI')
#ICICIFiles = glob.glob(ICICIRawData+ "/*.xlsx")
#
#ICICIfields = ['SESSION ID [ASPD]','ARN NO','RET_REF_NUM','TRANSACTION TYPE','TRANSACTION DATE','TRANSACTION AMT']
#list_ = []
#for file_ in ICICIFiles:
#    try:
#        df = pd.read_excel(file_,index_col=None, header=0,usecols=ICICIfields )
#        list_.append(df)
#    except:
#        print "Error in Reading File {}{}".format(chr(10),file_)
#        BrokenDataSets.append(file_)
#ICICIData = pd.concat(list_)
#ICICIData=ICICIData.drop_duplicates()
#ICICIData['PG Name']='ICICI' 
#
##Cleaning Temp DataSets
#clean()
#
#
##Axis PG
#AxisRawData=os.path.abspath(manualuploadDir+'\Axis')
#AxisFiles = glob.glob(AxisRawData+ "/*.xlsx")
#
#Axisfields = ['ORDER INFO','Bank Reference No','Requested Action','TXN DATE','SETTLEMENT DATE','Issuing Bank','TXN AMOUNT']
#list_ = []
#for file_ in AxisFiles:
#    try:
#        df = pd.read_excel(file_,index_col=None, header=0,usecols=Axisfields )
#        list_.append(df)
#    except:
#        print "Error in Reading File {}{}".format(chr(10),file_)
#        BrokenDataSets.append(file_)
#AxisData = pd.concat(list_)
#AxisData['PG Name']='Axis' 
#AxisData=AxisData.drop_duplicates()
##Cleaning Temp DataSets
#clean()
#
#
##TPSL PG
#TPSLRawData=os.path.abspath(manualuploadDir+'\TPSL')
#TPSLFiles = glob.glob(TPSLRawData+ "/*.xlsx")
#
#TPSLfields = ['Merchant TXN ID','TPSL TXN ID','Type of Refund','Status','TXN Date','Date of Refund Request','Bank Name','Refund Amount']
#list_ = []
#for file_ in TPSLFiles :
#    try:
#        df = pd.read_excel(file_,index_col=None, header=0,usecols=TPSLfields )
#        list_.append(df)
#    except:
#        print "Error in Reading File {}{}".format(chr(10),file_)
#        BrokenDataSets.append(file_)
#TPSLData = pd.concat(list_)
#TPSLData=TPSLData.drop_duplicates()
#TPSLData['PG Name']='TPSL' 
##Cleaning Temp DataSets
#clean()
#
#
##Filtering Datasets for Refund
#
##Axis
#AxisDataRefund=AxisData.copy()
##CC Avenue
#CCAveDataRefund=CCAveData[CCAveData['TRANSACTION'].str.strip()=='REFUND']
##Axis
#AxisDataRefund=AxisData[AxisData['Transaction Type'].str.strip()=='REFUND']
##Axis Blaze
#AxisBlazeDataRefund=AxisBlazeData[AxisBlazeData['Transaction Type'].str.strip()=='REFUND']
##Axis
#AxisDataRefund=AxisData[AxisData['REC FMT'].str.strip()=='CVD']
##ICICI
#ICICIDataRefund=ICICIData[ICICIData['TRANSACTION TYPE'].str.strip()=='REFUND (CREDIT)']
##Axis
#AxisDataRefund=AxisData[AxisData['Requested Action'].str.strip()=='refund']
##TPSL
#TPSLDataRefund=TPSLData[(TPSLData['Type of Refund'].str.strip()=='Offline Refund')&   ~ (TPSLData['Status'].str.strip()=='Pending')]

#Extracting ARN Numbers

#Axis
#CC Avenue

#Axis
#Axis Blaze
#Axis
#ICICI
#Axis
#TPSL


#Renaming and selecting relevant columns to append

#Axis
#CC Avenue
#Axis
#Axis Blaze
#Axis
#ICICI
#Axis
#TPSL

#Append All Datasets

#Axis
#CC Avenue
#Axis
#Axis Blaze
#Axis
#ICICI
#Axis
#TPSL

##Create Temp File for DDP Upload
#
#SQL Query
##Get Distinct Email Address and Phone Number (shipping Phone number)


##############################################################################


#How to Handle when Files are not available
#BrokenDataSets=['hahahahaha']
BrokenDataSets=set(BrokenDataSets)
if len(BrokenDataSets)<>0:
    Table = PrettyTable(['File Name'])
    for filename in BrokenDataSets:
        Table.add_row([filename])
    print "Following input files were broken",chr(10),chr(10),Table
    outputmessage="Some input files were broken in Axis"
else:
    print "All Input Datasets were used"
    outputmessage="All Input Datasets were used for Axis"
#


#print "Starting Code Run for Axis"
#AxisRawData=os.path.abspath(manualuploadDir+'\Axis')
#AxisFiles = glob.glob(AxisRawData+ "/*.xlsx")
#
#Axisfields = ['ORDER INFO', 'Bank ARN','Bank Reference No','Requested Action','TXN DATE','SETTLEMENT DATE','TXN AMOUNT']
##Axisfields =map(str.lower,Axisfields )
#list_ = []
#SheetName=[]
#MatchingColumns=[]
#df=pd.DataFrame()
#BrokenDataSets=[]
##ColumnNames=[]
#for file_ in AxisFiles:
#    MatchingColumns=[]
#    try:
#        SheetName=pd.ExcelFile(file_).sheet_names
#        ColumnNames=pd.ExcelFile(file_).parse().columns
#        #Matching Column Name
#        for basecolumn_ in Axisfields:  
#            for filecolumn_ in ColumnNames:
##                print filecolumn_+"_"+basecolumn_+"--->"+str(filecolumn_==basecolumn_)
#                if ''.join(c.lower() for c in filecolumn_ if not c.isspace())==''.join(c.lower() for c in basecolumn_ if not c.isspace()):
#                    MatchingColumns.append(filecolumn_)
#                    break
##        df=pd.DataFrame()
#        for sheet in SheetName:
#            df = df.append(pd.read_excel(file_,index_col=None, header=0,usecols=MatchingColumns,sheetname=sheet,converters={'ORDER INFO':str,'Bank ARN':str,'Bank Reference No':str}))
##        df = pd.read_excel(file_,index_col=None, header=0,usecols=Axisfields)
#        df=df[MatchingColumns]
#        df.columns=Axisfields
#        list_.append(df)
#        print "File Read!!!"
#    except:
#        print "Error in Reading File {}{}".format(chr(10),file_)
#        BrokenDataSets.append(file_)
#AxisData = pd.concat(list_)
#AxisData =AxisData[AxisData['Requested Action'].str.upper()=='REFUND']
#AxisData['TXN AMOUNT'] = AxisData['TXN AMOUNT'].abs()
#AxisData =AxisData.drop_duplicates()
#
##Fixing Date Formats
#AxisData['TXN DATE']=pd.to_datetime(AxisData['TXN DATE'],format='%d/%m/%Y %H:%M:%S')
#AxisData['SETTLEMENT DATE']=pd.to_datetime(AxisData['SETTLEMENT DATE'],format='%Y-%m-%d')
#
#OrderIDs=AxisData['ORDER INFO'].unique()
#c = len(OrderIDs)/max_value if len(OrderIDs) % max_value ==0 else  len(OrderIDs)/max_value +1
#FinalresultDDP=pd.DataFrame()
##Fixing BankName
#for i in range(c):
#    #time.sleep(5)
#    lista = map(str,OrderIDs[i*max_value:(i+1)*max_value])
##Order Number
#                                  
#    sql="""     Select
#        DISTINCT frt.orderid,fci.order_group_id,
#        fp.payment_gateway_name,max(fp.card_bank_name)
#            from
#        fact_refund_transaction frt  
#    LEft JOIN
#        fact_payment fp on frt.orderid = fp.order_id
#    left join fact_core_item fci on fci.order_group_id = frt.orderid
#    where
#            upper(fp.payment_gateway_name) in ('Axis','Axis') 
#            and  frt.orderid in
#            (""" + ','.join("'" + item + "'" for item in lista) + ") group by 1,2,3"""
#
#    QUERY_USER='prateek.gupta1@myntra.com'
#    QUERY_OUT_DIR=r'D:\Projects\Customer Experience - CC\Santhosh\DDPDownload'
#    QUERY_NAME='DDPAxisData'
#    filename=ddp.start(sql,QUERY_USER,QUERY_OUT_DIR,QUERY_NAME)
##    datecols=['taskcreateddate','taskcloseddate','taskduedate','tasklastupdateddate']
#    colnames=['OrderNumber','OrderGroupID','PaymentGateway','BankName']
#    DDPData=pd.read_table(filename,compression='gzip',sep=',',header=None,names=colnames,usecols=range(len(colnames)),converters={'OrderNumber':str})
#    FinalresultDDP=FinalresultDDP.append(DDPData)
#    FinalresultDDP=FinalresultDDP.drop_duplicates()
#
#print FinalresultDDP.shape    
#
#AxisData=pd.merge(AxisData,FinalresultDDP,how='left',right_on='OrderNumber',left_on='ORDER INFO',suffixes=('_PGData','_DDPData'))
#AxisData=AxisData.drop_duplicates()
#
#
#FinalresultDDP=pd.DataFrame()
#for i in range(1):
#    #time.sleep(5)
#    lista = map(str,OrderIDs[i*max_value:(i+1)*max_value])
##OR + NetBanking - Mode of Payment = Online - Only RIS will have exact amount refunded
#                                  
#    sql="""     Select
#        DISTINCT frt.orderid,frt.amount_to_be_refunded,to_date(frt.insert_date,'YYYYMMDD'),
#        frt.gateway_refund_id,frt.refund_status,frt.refund_transaction_id
#            from
#        fact_refund_transaction frt  
#    LEft JOIN
#        fact_payment fp on frt.orderid = fp.order_id
#                where  upper(fp.payment_gateway_name) in ('Axis','Axis') 
#                and frt.insert_date >= 20171101 
#                """
#
#    QUERY_USER='prateek.gupta1@myntra.com'
#    QUERY_OUT_DIR=r'D:\Projects\Customer Experience - CC\Santhosh\DDPDownload'
#    QUERY_NAME='ReturnsDataAxis'
#    filename=ddp.start(sql,QUERY_USER,QUERY_OUT_DIR,QUERY_NAME)
##    datecols=['taskcreateddate','taskcloseddate','taskduedate','tasklastupdateddate']
#    colnames=['OrderNumber','RefundedAmount','RefundAttemptedDate','AxisReferenceID','RefundStatus','RefundTransactionID']
#    DDPData=pd.read_table(filename,compression='gzip',sep=',',header=None,names=colnames,usecols=range(len(colnames)))
#    FinalresultDDP=FinalresultDDP.append(DDPData)
#    FinalresultDDP=FinalresultDDP.drop_duplicates()
#
#print FinalresultDDP.shape    
#RefundCreatedAxis=FinalresultDDP.copy()
#
##Fixing Floating point errors
#AxisData['TXN AMOUNT']=AxisData['TXN AMOUNT'].round(2)
#RefundCreatedAxis['RefundedAmount']=RefundCreatedAxis['RefundedAmount'].round(2)
#
#OuterJoinAxis=pd.merge(RefundCreatedAxis[RefundCreatedAxis.RefundStatus == 'SUCCESS'],AxisData,left_on=['OrderNumber','RefundedAmount']\
#         ,right_on=['ORDER INFO','TXN AMOUNT'],how='outer',indicator=True)
#PPSPassedNotCompletedAxis=OuterJoinAxis[OuterJoinAxis._merge=='left_only']
#
#OuterJoin2Axis=pd.merge(RefundCreatedAxis[RefundCreatedAxis.RefundStatus <> 'SUCCESS'],AxisData,left_on=['OrderNumber','RefundedAmount']\
#         ,right_on=['ORDER INFO','TXN AMOUNT'],how='outer',indicator=True)
#
#PPSFailedCompletedAxis=OuterJoin2Axis[OuterJoin2Axis._merge=='both']
#PPSFailedCompletedAxis=PPSFailedCompletedAxis.drop("_merge",1)
#PPSFailedCompletedAxis['Completed']=1
##Combinations for Failed PPS Tasks
#
#OuterJoin3Axis=pd.merge(RefundCreatedAxis[RefundCreatedAxis.RefundStatus <> 'SUCCESS'],\
#                    PPSFailedCompletedAxis[['ORDER INFO','TXN AMOUNT','Completed']]\
#                    ,left_on=['OrderNumber','RefundedAmount']\
#         ,right_on=['ORDER INFO','TXN AMOUNT'],how='outer',indicator=True)
#MixedAxis=OuterJoin3Axis[(OuterJoin3Axis.RefundStatus <> 'SUCCESS')&(OuterJoin3Axis._merge=='left_only')]
##Mixed=Mixed.drop(['ORDER INFO','TXN AMOUNT','Completed','_merge'],1)
#
##Sort Date if last attempted date is required
##Mixed=Mixed.drop_duplicates(['OrderNumber', u'RefundedAmount','AxisReferenceID', u'RefundStatus'])
##PPSFailedNotCompleted=OuterJoin[OuterJoin._merge=='left_only']
#MixedAxis['RefundedAmount']=MixedAxis['RefundedAmount'].astype(str)
#
#OrderNumberCombinationAxis=MixedAxis.groupby('OrderNumber')['RefundedAmount'].apply(','.join).reset_index()
#OrderNumberCombinationAxis=OrderNumberCombinationAxis.drop_duplicates()
#
##Isolated PG Data
#
#AllcompletedTasksAxis=OuterJoinAxis[OuterJoinAxis._merge=='both'][Axisfields]\
#            .append(PPSFailedCompletedAxis[Axisfields])
#AllcompletedTasksAxis=AllcompletedTasksAxis.drop_duplicates()
#
#IsolatedAxis=pd.merge(AxisData,AllcompletedTasksAxis,\
#                  on=['ORDER INFO','TXN AMOUNT'],
#                  how='outer',indicator=True)
#IsolatedAxis=IsolatedAxis[IsolatedAxis._merge=='left_only']
#IsolatedAxis=IsolatedAxis[[u'ORDER INFO', u'Bank ARN_x', u'Bank Reference No_x',
#       u'Requested Action_x', u'Date_x', u'Settlement Date_x', u'TXN AMOUNT',
#       u'BankName_x'
#                   ]]
#IsolatedAxis.columns=Axisfields                  
#IsolatedAxis=IsolatedAxis.drop_duplicates()
#
#MixedFinderAxis=pd.merge(IsolatedAxis,OrderNumberCombinationAxis,how='right',\
#                                left_on='ORDER INFO',\
#                                right_on='OrderNumber',\
#                                suffixes=('_PG','_PPS'))
#MixedFinderAxis=MixedFinderAxis[~pd.isnull(MixedFinderAxis.OrderNumber)]
#MixedFinderAxis=MixedFinderAxis.drop_duplicates()
#
#MixedFinderAxis['RefundedAmount'].apply(lambda x: sum(float(i) for i in x.split(',')))
##new_series = []
#
#
#MixedFinderAxis['PPS_Sum'] = MixedFinderAxis['RefundedAmount'].apply(lambda x: sum(float(i) for i in x.split(',')))
#
#MixedFinderAxis['TXN AMOUNT']=MixedFinderAxis['TXN AMOUNT'].astype(str)
#PGCrossSumAxis=MixedFinderAxis.groupby('ORDER INFO')['TXN AMOUNT'].apply(','.join).reset_index()
#
#MixedFinderAxis=pd.merge(MixedFinderAxis,PGCrossSumAxis,on='ORDER INFO',how='left',suffixes=('_base','_concat'))
#
#MixedFinderAxis['PG_GroupSum'] = MixedFinderAxis['TXN AMOUNT_concat'].apply(lambda x: sum(float(i) for i in x.split(',')))
#
#ManualNotRefundAxis=MixedFinderAxis[~(MixedFinderAxis['PPS_Sum'].round(2)==MixedFinderAxis['PG_GroupSum'].round(2))]
#ManualNotRefundAxis=ManualNotRefundAxis.drop_duplicates()
#
##ARNCount=AxisDataMatch[['OrderNumber', 'OrderGroupID', 'ORDER INFO']].groupby(['OrderNumber', 'OrderGroupID'])['ORDER INFO'].nunique().reset_index()
#
##RefundCreatedwARNCount=pd.merge(RefundCreated,ARNCount,how='left',on=['OrderNumber', 'OrderGroupID'])
##RefundCreatedwARNCount=RefundCreatedwARNCount.fillna(0)
##Finding Order Numbers which were triggered from our end but yet not triggered from PG
##RefundNotTriggered_1=pd.merge(AxisData,FinalresultDDP,how='outer',right_on='OrderNumber',left_on='ORDER INFO',suffixes=('_PGData','_DDPData'),indicator=True)
##RefundNotTriggered_1=RefundNotTriggered_1[RefundNotTriggered_1._merge=='right_only']
#
##Fixing Dates if Formats are wrong
#def funcAxis(row):
##    print 3
#    if (row['SETTLEMENT DATE']<row['TXN DATE']):
##        print 1
#        #Change formats till difference is positive
#        if ~pd.isnull(pd.to_datetime(row['TXN DATE'], format='%Y%m%d', errors='coerce')):
##            print 11
#            if (pd.to_datetime(row['TXN DATE'], format='%Y%m%d', errors='coerce')\
#            <row['SETTLEMENT DATE']):
##                print 111
#                return row['SETTLEMENT DATE']-row['TXN DATE']
#            elif (pd.to_datetime(row['TXN DATE'].strftime('%d%m%Y'),format='%m%d%Y')\
#            <row['SETTLEMENT DATE']):
##                print 112
##                print np.ceil(row['SETTLEMENT DATE']\
##                                -pd.to_datetime(row['TXN DATE'].dt.strftime('%d%m%Y'),format='%m%d%Y')\
##                .astype(float))
#                return row['SETTLEMENT DATE']\
#                                -pd.to_datetime(row['TXN DATE'].strftime('%d%m%Y'),format='%m%d%Y')\
#                
#    else:
##        print 2
#        return row['SETTLEMENT DATE']-row['TXN DATE']
##AxisDataMatch['DaysToRefund'] = AxisDataMatch[['SETTLEMENT DATE','TXN DATE']].apply(lambda row : func(row) ,axis = 1)
#AxisData['Days2Refund']=np.ceil((AxisData[['SETTLEMENT DATE','TXN DATE']]\
#.apply(lambda row : funcAxis(row) ,axis = 1)/np.timedelta64(1, 'D')).astype(float))
#
##AxisDataMatch['Days2RefundFinal']=np.ceil(AxisDataMatch['Days2Refund'])
#
#AxisData['FinalRefNumber']=np.where(AxisData['Bank ARN']=="-",AxisData['Bank Reference No'],AxisData['Bank ARN'])
##Exporting Datasets
#from pandas import ExcelWriter
##import time
#writer = ExcelWriter(pandaspath+"//AxisData_"+str(time.strftime("%m%d%Y"))+".xlsx")
#AxisData[['ORDER INFO',
#'FinalRefNumber',
#'Requested Action',
#'TXN DATE',
#'SETTLEMENT DATE',
#'TXN AMOUNT',
#'BankName',
#'Days2Refund'
#]].to_excel(writer,'AxisData',index=False)
##ManualNotRefundAxis.to_excel(writer,'ManualNotRefunded',index=False)
##PPSPassedNotCompletedAxis[[u'OrderNumber', u'RefundedAmount', u'RefundAttemptedDate',
##       u'AxisReferenceID', u'RefundStatus', u'ORDER INFO',
##       u'Issuer Txn ref number', u'Transaction Type',
##       u'TXN DATE', u'SETTLEMENT DATE', u'TXN AMOUNT']].to_excel(writer,'PPSPassedNotCompleted',index=False)
##RefundCreatedwARNCount[(RefundCreatedwARNCount['RefundCount']<>0)&(RefundCreatedwARNCount['ORDER INFO']==0)][['OrderNumber','OrderGroupID']].to_excel(writer,'RefundCreatedARNMatch',index=False)
##AxisDataMatch[(pd.isnull(AxisDataMatch['ORDER INFO']))|(pd.isnull(AxisDataMatch['SETTLEMENT DATE']))].to_excel(writer,'SettlementMissingData',index=False)
#writer.save()
###Append Files by PG
##
###Clean Individual Files
##Filters="""
##Axis:
##“Myntra : Merchant Settlement Report from” noreply@Axispay.com
##Merchant reference number 
##Issuer Txn ref number – RRN#
##Transaction Type - REFUND
##TransactionDate and Time
##BankName -???
##Settlement Date
##Transaction amount
##
##Axis BLAZE: 
##“Myntra Blazenet : Merchant Settlement Report from” noreply@Axispay.com 
##Merchant reference number 
##Issuer Txn ref number – RRN#
##Transaction Type - REFUND
##TransactionDate and Time
##BankName -???
##Settlement Date
##Transaction amount
##
##AXIS: 
##“Myntra MIGS Report” from Merchantacquiringhelpdesk.Karnataka@axisbank.com 
##ORDER ID 
##ARN
##TXN Date
##Bank Name -???
##SETTLEMENT DATE
##Refund--- will come in two different sheets
##TXN AMOUNT
###For ONUS, use RRN - Axis
## 
##CC AVE: 
##“Settlement Report for : M_cca49517_49517 Date” from service@ccavenue.com 
##Trip ID 
##Bank Ref. No. (RRN)
##Note: Mobikwik – WALLET refunds will not have any bank ref no. 
##CCAVE has lot of Wallet refunds.  -- not sending
##Standard Charted - use CC avenue number
##for blanks - CC avenue number
##Initiated Date - ???
##Settlement 
##TRANSACTION -REFUND
##Gross Amount
##Bank ID 
## 
##Axis: 
##“Email MPR as” of from payoutreport@Axisbank.com 
##MERCHANT_TRACKID 
##ARN NO 
##'(Onus transaction) – Net Banking - Will get back ---??
##TRANS DATE
##SETTLE DATE
##Refund ---REC FMT CVD 
##Amount ---Net Amount
#
##
##Axis
##Manually Download from the Portal
##Requested Action - Refund
##Transaction ID= order ID
##Amount= Net amt
##Bank - Issuing Bank
##Initiation - Date
##Settlement - Settlement Date
##Bank Reference No
#
##
##TPSL -tech process solution
##Merchant TXN ID
##TXN Date - Initiated
##Date of Refund Request -  Settlement
##Bank Name
##Refund Amount
##Remove Pending-Status
##Type of Refund - Offline Refund
##TPSL TXN ID - ARN??
##
##ICICI
##SESSION ID [ASPD]
##ARN NO
##TRANSACTION TYPE - Refund (credit)
##TRANSACTION DATE
##Initiation Date ---???
##Bank --????
##Transaction
##"""
###Append All PG Files in One Go
##
###Create Temp File for DDP Upload
##
###Get Distinct Email Address and Phone Number (shipping Phone number)
##
###Call Out Which Files were not present
##
###Export to Excel


#Trying to Send Email


emailfrom = "prateek.gupta1@myntra.com"
recipients= ["santhosh.vr@myntra.com","arun.s@myntra.com","pramod.j@myntra.com","shabeer.ahmed@myntra.com","refundsteam@myntra.com"]
#recipients= ["santhosh.vr@myntra.com"]
#recipients= ["prateek.gupta1@myntra.com"]
#recipients= ["karthik.iyer@myntra.com"]
emailto=", ".join(recipients)
fileToSend = pandaspath+"//AxisData_"+str(time.strftime("%m%d%Y"))+".xlsx"
username = "prateek.gupta1@myntra.com"
password = "ynapbhpsddyblroz"

msg = MIMEMultipart()
msg["From"] = emailfrom
msg["To"] = emailto
msg["Subject"] = "Axis Data"+str(time.strftime("%m%d%Y")) 

textPart = MIMEText("Axis Data for "+str(time.strftime("%m%d%Y")) + chr(10)+ chr(10)+ chr(10)+\
"                 >>>>>>Autogenerated Mail<<<<<<<"+chr(10)+ chr(10)+chr(10)+\
"Please cross check the data before using"+chr(10)+chr(10)+chr(10)+\
outputmessage, 'plain')
msg.attach(textPart)

msg.preamble = "Axis Data"+str(time.strftime("%m%d%Y"))

ctype, encoding = mimetypes.guess_type(fileToSend)
if ctype is None or encoding is not None:
    ctype = "application/octet-stream"

maintype, subtype = ctype.split("/", 1)

if maintype == "text":
    fp = open(fileToSend)
    # Note: we should handle calculating the charset
    attachment = MIMEText(fp.read(), _subtype=subtype)
    fp.close()
elif maintype == "image":
    fp = open(fileToSend, "rb")
    attachment = MIMEImage(fp.read(), _subtype=subtype)
    fp.close()
elif maintype == "audio":
    fp = open(fileToSend, "rb")
    attachment = MIMEAudio(fp.read(), _subtype=subtype)
    fp.close()
else:
    fp = open(fileToSend, "rb")
    attachment = MIMEBase(maintype, subtype)
    attachment.set_payload(fp.read())
    fp.close()
    encoders.encode_base64(attachment)
attachment.add_header("Content-Disposition", "attachment", filename=fileToSend)
msg.attach(attachment)


server = smtplib.SMTP("smtp.gmail.com:587")
server.starttls()
server.login(username,password)
server.sendmail(emailfrom, recipients, msg.as_string())
server.quit()


