import postgresql
PEROID='dwm'

class PeroidException(Exception):
    def __init__(self, peroid):
        Exception.__init__(self)
        self.peroid=peroid

def getdb(db='stock', cs='pq://localhost/'):
    return postgresql.open(cs+db)

def InsertCompany(db):
    return db.prepare('''INSERT INTO companies(
            "ExchangeID", "Code", "SName_local", "SName_eng", "Name_local", 
            "Name_eng", "RegAddr", "AshareCode", "AshareShort", "AshareDate", 
            "BshareCode", "BshareShort", "BshareDate", "Country", "Region", 
            "Province", "City", "Sector", "Website")
    VALUES ($1, $2, $3, $4, $5, $6, 
            $7, $8, $9, $10, $11, 
            $12, $13, $14, $15, $16, 
            $17, $18, $19);
    ''')
    
def UpdateCompany(db):
    return db.prepare('''UPDATE companies
    set "SName_local"=$2, "SName_eng"=$3,
    "Name_local"=$4, "Name_eng"=$5,
    "RegAddr"=$6, "AshareCode"=$7, "BshareCode"=$8,
    "AshareDate"=$9, "BshareDate"=$10,
    "Country"=$11, "Region"=$12,
    "Province"=$13, "City"=$14,
    "Sector"=$15
    Where "Code"=$1
    ''')
        
def InsertOhlc(db, peroid='d'):
    if peroid in PEROID and len(peroid)==1:
        return db.prepare('''INSERT INTO ohlc_%s(
                product_id, date, open, high, low, close, volume, 
                adjclose)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
        ''' % peroid)
    raise PeroidException(peroid)

def SelectOhlcStat(db, peroid='d'):
    if peroid in PEROID and len(peroid)==1:
        return db.prepare('''SELECT p.id, p.code, max(h.date) 
        FROM products p	LEFT OUTER JOIN ohlc_%s h ON p.id=h.product_id
        --where p.id < 916
    	GROUP BY p.id
    	ORDER BY p.code ''' %  peroid)
    raise PeroidException(peroid)
