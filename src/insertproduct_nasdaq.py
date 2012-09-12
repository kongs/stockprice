import csv
from helpers import dbconnect

file=r'../data/clstnasdaq.csv'

db=dbconnect.getdb()
reader=csv.reader(open(file, 'r'))
next(reader)

selectc=db.prepare('select id from companies where name=$1')
insertc=db.prepare('insert into companies (name) values ($1) returning id')
insertp=db.prepare('insert into products (code, company_id) values ($1, $2)')

with db.xact():
    for l in reader:
        cid=selectc.first(l[1])
        if not cid:
            cid=insertc.first(l[1])
        insertp(l[0], cid)
print('finished!')