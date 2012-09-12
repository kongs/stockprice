#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys
import urllib.request
from xml.etree import ElementTree

from helpers.dbconnect import getdb
from helpers.dbconnect import UpdateCompany

def iterfind(m, i, e='</td>'):
    try:
        while not m in next(i): continue
        res=''
        while not e in res:
            res += next(i)
        return res
    except StopIteration:
        print('Warning: Find None!')
        return None
    
def readvalue(s):
    try:
        if 'span' in s: 
            tmp=DateMatch.findall(s)
            return '/'.join(['-',tmp[0]] if len(tmp)==1 else tmp[1:])
        if 'http' in s: return UrlMatch.findall(s)[0]
        return ElementTree.fromstring(s.replace('<BR>', '/').replace('&nbsp;', '')).text
    except Exception as e:
        print('Fatal coping ', *row)
        print(e)
        fails.append(list(row).append(str(e)))
        return None
    
def requestCompany(url, fields):
    info=[]
    re = urllib.request.Request(url)
    try: rs = str(urllib.request.urlopen(re).read(), encoding='gbk').splitlines()
    except Exception as e:
        print(url[-6:],e)
        pass

    for field in fields:
        info.append(readvalue(iterfind(field, iter(rs))))
    return info

fields=['公司代码:', '股票代码(A股/B股):', '上市日(A股/B股):', 
        '可转债简称（代码）:', '公司简称(中/英):', '公司全称(中/英):',
        '注册地址:', '(门类/大类/中类):', '所属省/直辖市:',
        'A股状态/B股状态:', '网址:']

DateMatch=re.compile('[0-9-]{10}')
UrlMatch=re.compile('http://[-.\w]*')

company_infos=[]
fails=[]

db=getdb()
select_sql='select "Code", "SName_local" from companies where "ExchangeID"=1'
SelectCompCode=db.prepare(select_sql)

CompanyCodes=SelectCompCode()

total=len(CompanyCodes)
  
for row in CompanyCodes:
    total-=1
    print('Now requesting for:', row[1]+',', total, 'left')
    url=r'http://www.sse.com.cn/sseportal/webapp/datapresent/SSEQueryListCmpAct?reportName=QueryListCmpRpt&COMPANY_CODE='
    url += row[0]
    company_infos.append(requestCompany(url, fields))

UpdateCompanyInfo=UpdateCompany(db)

for info in company_infos:
    _info=[]
    _info.append(info[0])           #Company code
    


print('finished!')
print('total fails:', len(fails))
for fail in fails: print(fail)

