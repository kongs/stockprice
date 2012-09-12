import csv
from helpers.dbconnect import getdb


insert_sql='''INSERT INTO products(
            code)
    VALUES ($1);
'''
db=getdb()
filepath=r'../data/list.csv'

insert_product=db.prepare(insert_sql)
reader = csv.reader(open(filepath, 'r'))

with db.xact():
    for r in reader:
        insert_product(r[0]+'.ss')
        
print("finished")

