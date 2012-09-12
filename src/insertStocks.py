#import csv
#from helpers.dbconnect import getdb
#
#file=r'../data/list.csv'
#
#csvReader=csv.reader(open(file,'r'))
#
#db=getdb()
#InsertStock=db.prepare('INSERT INTO stocks(code, name_local, suffix) VALUES ($1, $2, $3)')
#
#with db.xact():
#    for row in csvReader:
#        InsertStock(row[0], row[1], 'SS')
#
#
import csv
from helpers.dbconnect import getdb

file=r'../data/list.csv'

insert_sql='''INSERT INTO companies(
            "ExchangeID", "Code", "SName_local", "AshareCode")
    VALUES ($1, $2, $3, $4);
'''
db=getdb()
#InsertCompany=db.prepare(insert_sql)

reader=csv.reader(open(file, 'r'))
print(reader.line_num)
next(reader)
print(reader.line_num)
with db.xact():
    for row in reader:
        continue
        if len(row)==0:continue
        if len(row[0])==0: continue
        data=list()
        data.append(1)                           #ExchangeID
        data.append(row[0].zfill(6))             #Company code
        data.append(row[1].replace(' ', ''))     #short
        data.append(row[0].zfill(6))             #Ashare code
        #InsertCompany(*data)
        print('Inserted: ' + data[1])
print(reader.line_num)
print('finished')
    