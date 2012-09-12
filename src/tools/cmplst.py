import csv
from pprint import pprint

amex=r'../data/clstamex.csv'
nasdaq=r'../data/clstnasdaq.csv'
nyse=r'../data/clstnyse.csv'

ncm=r'../data/nasdaq-cm.csv'
ngm=r'../data/nasdaq-gm.csv'
ngs=r'../data/nasdaq-gs.csv'
nadr=r'../data/nasdaq-adr.csv'

def getlst(file, col=0):
    r=csv.reader(open(file, 'r'))
    next(r)
    return([l[col] for l in r if len(l)>=2 ])

#################
# symbol
#################
col=0   #for symbol

ln=getlst(nasdaq, col)
sn=set(ln)

lncm=getlst(ncm, col)
sncm=set(lncm)

lngm=getlst(ngm, col)
sngm=set(lngm)

lngs=getlst(ngs, col)
sngs=set(lngs)

lnadr=getlst(nadr, col)
snadr=set(lnadr)

#print(len(lncm), len(sncm))
#print(len(lngm), len(sngm))
#print(len(lngs), len(sngs))
#print(len(lnadr), len(snadr))

print('ncm & ngm: ', len(sncm & sngm))
print('ncm & ngs: ', len(sncm & sngs))
print('ngm & ngs: ', len(sngm & sngs))
sn_union=sncm | sngm | sngs


print(len(sn_union))
print(len(sn))
delta=sn_union-sn
print(len(delta), delta)
print(delta & sncm)
print(delta & sngm)
print(delta & sngs)


print('WAG' in sncm)
print('WAG' in sngm)
print('WAG' in sngs)
print('WAG' in snadr)
#cmpcol=1
#
#cmpf1=ngm
#cmpf2=ngs
#
#r1=csv.reader(open(cmpf1, 'r'))
#next(r1)
#
#
#for l1 in r1:
#    r2=csv.reader(open(cmpf2, 'r'))
#    next(r2)
#    for l2 in r2:
#        if l1[cmpcol]==l2[cmpcol]:
#            print([r1.line_num, r2.line_num])
#            print(l1)
#            print(l2)
print('finished!')