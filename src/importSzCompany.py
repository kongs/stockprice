import csv
from datetime import datetime

from helpers.dbconnect import getdb

file=r'../data/szse-company-list.csv'

file=r'../data/list.csv'

insert_sql='''INSERT INTO companies(
            "ExchangeID", "Code", "SName_local", "SName_eng", "Name_local", 
            "Name_eng", "RegAddr", "AshareCode", "AshareShort", "AshareDate", 
            "BshareCode", "BshareShort", "BshareDate", "Country", "Region", 
            "Province", "City", "Sector", "Website")
    VALUES ($1, $2, $3, $4, $5, $6, 
            $7, $8, $9, $10, $11, 
            $12, $13, $14, $15, $16, 
            $17, $18, $19);
'''
db=getdb()
InsertCompany=db.prepare(insert_sql)

reader=csv.reader(open(file, 'r'))
next(reader)

with db.xact():
    for row in reader:
        if len(row)==0:continue
        if len(row[0])==0: continue
        data=list()
        data.append(2)                           #ExchangeID
        data.append(row[0].zfill(6))             #Company code
        data.append(row[1].replace(' ', ''))     #short
        data.append(None)                        #short_eng
        data.append(row[2])                      #full
        data.append(row[3])                      #eng
        data.append(row[4])                      #reg
        data.append(row[5].zfill(6)                         #AshareCode
                    if len(row[5])>0 else None)
        data.append(row[6].replace(' ', '')                 #Ashareshort
                    if len(row[6])>0 else None)            
        data.append(datetime.strptime(row[7], '%Y/%m/%d')   #AshareDate
                    if len(row[7])>0 else None)            
        data.append(row[10].zfill(6)                        #BshareCode
                    if len(row[10])>0 else None)           
        data.append(row[11].replace(' ', '')                #Bshareshort
                    if len(row[11])>0 else None)           
        data.append(datetime.strptime(row[12], '%Y/%m/%d')  #BshareDate
                    if len(row[12])>0 else None)
        data.append('China')
        data.append(row[15])                     #region
        data.append(row[16])                     #province
        data.append(row[17])                     #city
        data.append(row[18])                     #sector
        data.append(row[19])                     #website
        InsertCompany(*data)
        #print(data)
        print('Inserted: ' + data[2])

print('finished')
    