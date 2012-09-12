#!/usr/bin/env python
#coding:utf-8
# Author:  kongs <kongshuai89@gmail.com>
# Purpose: update ohlc tables
# Created: Saturday, September 08, 2012
# TODO: weekly and monthly data.


import csv
import logging
import time
import os
#import os.path
from datetime import date
from datetime import timedelta
from datetime import datetime
from random import shuffle

import postgresql

from helpers.pullprice import pull_price
from helpers import dbconnect

dbname='stock'
logfile='../log/upate.log'
os.makedirs(os.path.dirname(logfile), exist_ok=True)

# set up logging to file - see previous section for more details
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M',
                    filename=logfile,
                    filemode='a')
# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# set a format which is simpler for console use
formatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M')
# tell the handler to use this format
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger('').addHandler(console)

#----------------------------------------------------------------------
def _update(ulist=None, retry=3, peroid='d', db=None,
           mindatedelta=0, loopafter=60*2, cmpdte=None):
    """
    update ohlc_%peroid automatically
    """
    retry=retry-1
    pre_str='(%d atts) ' % (retry)
    if len(ulist)==0:
        logging.info(pre_str + 'Finished update with no fails!')
        return True
    if retry<0:
        logging.info(pre_str + 'Finished update with %d fails'%len(ulist))
        for item in [sorted(ulist, key=lambda u: u[1])]:
            logging.error('fail '+str(item))
        return False

    shuffle(ulist)
    if not db:
        db=dbconnect.getdb()
    istcmd=dbconnect.InsertOhlc(db, peroid)
    fails=[]

    if not cmpdte:
        td=date.today()
        if peroid=='d':
            cmpdte=td-timedelta({7:2, 1:3}.get(td.isoweekday(), 1)+mindatedelta)
        elif peroid=='w':
            cmpdte=td-timedelta(td.weekday())
            if td.isoweekday()<6:   #weekday, find last last monday.
                cmpdte=cmpdte-timedelta(7)
        elif peroid=='m':
            cmpdte=(td.replace(day=1)-timedelta(1)).replace(day=1)
        else:
            raise Exception
    lenulist=len(ulist)
    cur=0
    for row in ulist:
        cur=cur+1
        fromd=None
        if row[2]:
            if row[2]>=cmpdte:
                logging.debug(pre_str +'%-5s up to date.(%d/%d)' % (row[1], cur, lenulist))
                continue
            else:
                fromd=row[2]
        r=pull_price(code=row[1], startd=fromd, period=peroid)
        if not r:
            fails.append(row)
            logging.error(pre_str + '%s failed.(%d/%d)' % (row[1], cur, lenulist) )
            continue
        if fromd:
            r=r.splitlines()[2:]    #remove header & first data
        else:
            r=r.splitlines()[1:]    #remove header only
        r.reverse()                 #put older data ahead
        reader=csv.reader(r)
        try:
            with db.xact():
                for line in reader:
                    if len(line)>0:
                        istcmd(row[0], datetime.strptime(line[0], '%Y-%m-%d'),
                                    line[1], line[2], line[3], line[4], int(line[5]), line[6])
            #print(pre_str, row[1], reader.line_num-1, peroid, 'records from', fromd, 'inserted!')
            logging.info(pre_str + '%-4s %4d %s records from %s inserted.(%d/%d)'
                  %(row[1], reader.line_num-1, peroid, fromd, cur, lenulist))
        except postgresql.exceptions.NumericRangeError as e:
            logging.error(e)
    if retry >0:
        logging.info(pre_str + 'Current loop end. %d %s fails. restart %.2f min later.' %
              (len(fails), peroid, loopafter/60))
        time.sleep(loopafter)
    _update(fails, retry, peroid, db, mindatedelta, loopafter, cmpdte)

def update(updateperoid='dwm', retry=3, cmpdte=None,
           mindatedelta=0, loopafter=60*2):
    db=dbconnect.getdb()
    for p in updateperoid:
        updatelist=dbconnect.SelectOhlcStat(db, p)()
        _update(ulist=updatelist, retry=retry, peroid=p, cmpdte=cmpdte, db=db,
                mindatedelta=mindatedelta, loopafter=loopafter)
        

def main():
    update(updateperoid='m', retry=3, mindatedelta=10)
    
if __name__== '__main__':
    main()