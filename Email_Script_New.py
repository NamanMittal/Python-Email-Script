import pandas as pd
import pytz
from exchangelib import Account, Credentials ,DELEGATE,HTMLBody
#C:\ProgramData\Anaconda3\Lib\site-packages
import re,datetime
#requires cached_property, dnspython,requests_ntlm,isodate,tzlocal,psycopg2

##
f = open("C:/Users/nmittal/Documents/Python/PowerBI/cred.txt","r")
f = f.read()


credentials = Credentials(
    username= f.split('\n')[0],
    password = f.split('\n')[1]
)
account = Account(
    primary_smtp_address='***', 
    credentials=credentials, 
    autodiscover=True, 
    access_type=DELEGATE
)

################### Max Date from Database ######################################
from sqlalchemy import create_engine
engine = create_engine('postgresql://', echo=False)
conn = engine.connect()
curr = conn.connection.cursor()
LastMaxTime = pd.to_datetime(pd.read_sql("""select max("DateTime") as maxtime from uno.fact_cs_email_tracker_py""",conn).maxtime)[0]
#################################################################################
#Count = account.inbox.total_count
ist = pytz.timezone('Asia/Kolkata')

Mail = pd.DataFrame([],columns = ["Folder","Message ID","DateTime","Subject","Body","Sender Name","Sender Mail","Receiver","Sensitivity","cc Recipients","cc Recipients Name","cc Recipients Mail","Read","Has Attachments","Importance"])
for item in account.inbox.all().only('message_id', 'datetime_received', 'subject','sender','text_body','sender','sensitivity','cc_recipients','is_read','has_attachments','importance','received_by').order_by('-datetime_received')[:2]:
         #if item.datetime_received.replace(tzinfo=None) >= LastMaxTime:
               Mail = Mail.append([
                    {
                    "Message ID": item.message_id,
                    "DateTime" : item.datetime_received.astimezone(ist).strftime("%d/%m/%y %H:%M:%S"),
                    "Subject" : item.subject,
                    "Sender Name" : item.sender.name,
                    "Sender Mail" : item.sender.email_address,
                    "Body" : item.text_body,
                    "Sensitivity" : item.sensitivity,
                    "cc Recipients" : item.cc_recipients,
                    "Read" : item.is_read,
                    "Has Attachments" : item.has_attachments,
                    "Importance" : item.importance,
                    "Receiver" : item.received_by
                    }
            ],ignore_index=True)

Mail["Folder"] = "Inbox"
cols = ['Folder', 'Sensitivity', 'Read', 'Has Attachments','Importance']
Mail[cols] = Mail[cols].astype('category')
Mail['cc Recipients'].fillna(value='', inplace=True)
Mail['Receiver'].fillna(value='', inplace=True)

Mail["cc Recipients Name"] = ''
Mail["cc Recipients Mail"] = '' 
Mail["Receiver Name"] = ''
Mail["Receiver Mail"] = '' 

for i in range(0,Mail.shape[0]):
    if Mail["cc Recipients"][i:i+1][i] != '':
        s = ''
        t = ''
        for j in range(0,len(Mail["cc Recipients"][i:i+1][i])):
            s = ','.join([Mail["cc Recipients"][i:i+1][i][j].name])
            t = ','.join([Mail["cc Recipients"][i:i+1][i][j].email_address])            
        Mail["cc Recipients Name"].iloc[i] = s
        Mail["cc Recipients Mail"].iloc[i] = t

for i in range(0,Mail.shape[0]):
    if Mail["Receiver"][i:i+1][i] != '':
        s = ''
        t = ''
        for j in range(0,len(Mail["Receiver"][i:i+1])):
            s = ','.join([Mail["Receiver"][i:i+1][i].name])
            t = ','.join([Mail["Receiver"][i:i+1][i].email_address])            
        Mail["Receiver Name"].iloc[i] = s
        Mail["Receiver Mail"].iloc[i] = t        

#################################################################################
############################### No Use Now  ####################################
del Mail["cc Recipients"]    
del Mail["Receiver"]
################################################################################
##Mail.to_excel("C://Users/ta0056/Desktop/OutMail_"+"0_100.xlsx")
################################################################################
#Count = account.outbox.total_count
#M2 = datetime.datetime(2019,9,20,5,0,0)

Mail_Sent = pd.DataFrame([],columns = ["Folder","Message ID","DateTime","Subject","Body","Sender","Receiver","Receiver Name","Receiver Mail","Sensitivity","cc Recipients","cc Recipients Name","cc Recipients Mail","Read","Has Attachments","Importance"])
for item in account.sent.all().only('message_id', 'datetime_sent', 'subject','to_recipients','text_body','sender','sensitivity','cc_recipients','is_read','has_attachments','importance').order_by('-datetime_sent')[:]:
    if item.datetime_sent(tzinfo=None) >= LastMaxTime:
        Mail_Sent = Mail_Sent.append([
                {  
                    "Message ID": item.message_id,
                    "DateTime" : item.datetime_sent.astimezone(ist).strftime("%d/%m/%y %H:%M:%S"),
                    "Subject" : item.subject,
                    "Receiver" : item.to_recipients,
                    "Body" : item.text_body,
                    "Sender" : item.sender,
                    "Sensitivity" : item.sensitivity,
                    "cc Recipients" : item.cc_recipients,
                    "Read" : item.is_read,
                    "Has Attachments" : item.has_attachments,
                    "Importance" : item.importance
                    }
            ],ignore_index=True)

#Mail_Sent.info()
Mail_Sent["Folder"] = "Sent"
Mail_Sent["Receiver Name"] = ''
Mail_Sent["Receiver Mail"] = ''
Mail_Sent.fillna(value='', inplace=True)
Mail_Sent["Folder"] = Mail_Sent["Folder"].astype('category')
Mail_Sent["Sensitivity"] = Mail_Sent["Sensitivity"].astype('category')
Mail_Sent["Read"] = Mail_Sent["Read"].astype('category')
Mail_Sent["Has Attachments"] = Mail_Sent["Has Attachments"].astype('category')
Mail_Sent["Importance"] = Mail_Sent["Importance"].astype('category')
Mail_Sent['cc Recipients'].fillna(value='', inplace=True)

Mail_Sent["cc Recipients Name"] = ''
Mail_Sent["cc Recipients Mail"] = ''
Mail_Sent["Sender Name"] = ''
Mail_Sent["Sender Mail"] = '' 

#cc Reciepient
for i in range(0,Mail_Sent.shape[0]):
    if Mail_Sent["cc Recipients"][i:i+1][i] != '':
        s = ''
        t = ''
        for j in range(0,len(Mail_Sent["cc Recipients"][i:i+1][i])):
            s = ','.join([Mail_Sent["cc Recipients"][i:i+1][i][j].name])
            t = ','.join([Mail_Sent["cc Recipients"][i:i+1][i][j].email_address])            
        Mail_Sent["cc Recipients Name"].iloc[i] = s
        Mail_Sent["cc Recipients Mail"].iloc[i] = t
#Receiver
for i in range(0,Mail_Sent.shape[0]):
    if Mail_Sent["Receiver"][i:i+1][i] != '':
        s = ''
        t = ''
        for j in range(0,len(Mail_Sent["Receiver"][i:i+1])):
            s = ','.join([Mail_Sent["Receiver"][i:i+1][i][j].name])
            t = ','.join([Mail_Sent["Receiver"][i:i+1][i][j].email_address])         
        Mail_Sent["Receiver Name"].iloc[i] = s
        Mail_Sent["Receiver Mail"].iloc[i] = t
#Sender
for i in range(0,Mail_Sent.shape[0]):
    if Mail_Sent["Sender"][i:i+1][i] != '':
        s = ''
        t = ''
        for j in range(0,len(Mail_Sent["Sender"][i:i+1])):
            s = ','.join([Mail_Sent["Sender"][i:i+1][i].name])
            t = ','.join([Mail_Sent["Sender"][i:i+1][i].email_address])          
        Mail_Sent["Sender Name"].iloc[i] = s
        Mail_Sent["Sender Mail"].iloc[i] = t


del Mail_Sent["cc Recipients"]    
del Mail_Sent["Receiver"]  
del Mail_Sent["Sender"]      

All_Mails = Mail.append(Mail_Sent,ignore_index = True,sort=False)
del i,j,s,t,cols
##########################################################################
#Searching LAN number IN Subject
All_Mails["Subject_LAN"] = ""
LAN_Subject = pd.DataFrame([set(re.findall('PH\w{13}',i)) for i in All_Mails.Subject])
LAN_Subject.fillna(value='', inplace=True)
#LAN_Subject["Combine"] = LAN_Subject.apply(lambda x : ",".join(x) if x[1] != "" else "" if x[1]=="" and x[0] ==""  else x[0] ,axis = 1)
LAN_Subject["Combine"] = LAN_Subject[LAN_Subject.columns].astype(str).apply(','.join, axis=1)
LAN_Subject.fillna(value='', inplace=True)
LAN_Subject["Combine"] = LAN_Subject["Combine"].apply(lambda x : x[:-1] if x[-1:]=="," else x)
All_Mails["Subject_LAN"] = LAN_Subject["Combine"]
##########################################################################
#Searching LAN number IN Body
All_Mails["Body_LAN"] = ""
LAN_Body = pd.DataFrame([set(re.findall('PH\w{13}',i)) for i in All_Mails.Body])
LAN_Body.fillna(value='', inplace=True)
LAN_Body["Combine"] = LAN_Body[LAN_Body.columns].astype(str).apply(','.join, axis=1)
LAN_Body.fillna(value='', inplace=True)
LAN_Body["Combine"] = LAN_Body["Combine"].apply(lambda x : x[:-1] if x[-1:]=="," else x)
All_Mails["Body_LAN"] = LAN_Body["Combine"]

##Combine both LAN Number groups
DJ = pd.DataFrame([])
#DJ["One"] = [i.split(",") if i!= "" else "" for i in LAN_Subject["Combine"]]
DJ["One"] = LAN_Subject["Combine"].str.split(",")
DJ["Two"] = LAN_Body["Combine"].str.split(",")
DJ["Three"] = DJ["One"] + DJ["Two"]
DJ["Three"] = [set(i) for i in DJ["Three"]]

All_Mails["LAN"] = DJ["Three"]
del All_Mails["Subject_LAN"],All_Mails["Body_LAN"]
###########################################################################
All_Mails.to_excel("F:/UNO/Python/QRC_EMAIL_TRACKER/QRC_EMAIL_TRACKER_"+datetime.datetime.now().strftime('%d%m%y_%Hhr%Mm'+".xlsx"))
#All_Mails.to_sql(name='fact_cs_email_tracker_py',con=engine,schema='uno',if_exists='append')

######################
##Send Email


import sys
sys.path.append('C:\ProgramData\Anaconda3\Lib\site-packages')


from exchangelib import Configuration, Account, DELEGATE #ServiceAccount, 
from exchangelib import Message, Mailbox, FileAttachment,ItemAttachment

from config import cfg  # load your credentials

subject = 'Testing via Exchangelib in Python'
--body = 'This is an autogenerated mail.Please do not respond.'
body = HTMLBody('<html><body><b>This is an autogenerated mail.</b>Please do not respond.</body></html>')
recipients = ['***']

def send_email(account, subject, body, recipients, attachments):
    to_recipients = []
    for recipient in recipients:
        to_recipients.append(Mailbox(email_address=recipient))
    # Create message
    m = Message(account=account,
                folder=account.sent,
                subject=subject,
                body=body,
                to_recipients=to_recipients)

    # attach files
    #for attachment_name, attachment_content in attachments or []:
    file = FileAttachment(name='Code', content=f)
    m.attach(file)
    m.send_and_save()


config = Configuration(server=cfg['server'], credentials=credentials)

# Read attachment
attachments = []
f = open("C:/Users/nmittal/Documents/Python/PowerBI/Twitter_ Search.py","rb+")
f = f.read()    
attachments.append(f)

# Send email
send_email(account,subject,body, recipients,f)
