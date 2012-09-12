startyear=1981
endyear=2013
step=5
basename='ohlc_w'
filepath='partition.sql'

def getsql_yearly():
    ##################################################
    # step 1: generate parent table
    # already have. skip
    
    with open(filepath ,'w') as tf:
        #1. create table
        for y in range(startyear, endyear+1):
            #1. create table
            s='''CREATE TABLE %s_%d (
        CHECK (extract(year from date) = %d)
    ) INHERITS (%s);'''
            print(s%(basename, y, y, basename), file=tf)
        print('', file=tf)
            
        #2. create insert functio
        fhead='''CREATE OR REPLACE FUNCTION %s_insert_trigger()
    RETURNS TRIGGER AS $$
    BEGIN
        IF ( extract(year from NEW.date) = %d ) THEN
            INSERT INTO %s_%d VALUES (NEW.*);'''%(basename, endyear, basename, endyear)
        print(fhead, file=tf)
        s='''    ELSIF ( extract(year from NEW.date) = %d ) THEN
            INSERT INTO %s_%d VALUES (NEW.*);'''
        
        for y in range(endyear-1, startyear-1, -1):  #endyear is included in fhead
            print(s%(y, basename, y), file=tf)
            
        ftail='''    ELSE
            RAISE EXCEPTION 'Date out of range.';
    END IF;
    RETURN NULL;
    END;
    $$
    LANGUAGE plpgsql;
    '''
        print(ftail, file=tf)
    
        #3 trigger
        ts='''CREATE TRIGGER insert_%s_trigger
    BEFORE INSERT ON %s
    FOR EACH ROW EXECUTE PROCEDURE %s_insert_trigger();
    '''
        print(ts%(basename, basename, basename), file = tf)
    
    
        
        #4. create index
        for y in range(startyear, endyear+1):
            print('CREATE INDEX %s_%d_idx ON %s_%d (product_id, date);'
                  %(basename, y, basename, y), file=tf)

def getsql_years():
    ##################################################
    # step 1: generate parent table
    # already have. skip
    
    with open(filepath ,'w') as tf:
        #1. create table
        for y in range(startyear, endyear+1, step):
            #1. create table
            s='''CREATE TABLE %s_%d (
        CHECK (extract(year from date) > %d and  
        extract(year from date) <= %d)
    ) INHERITS (%s);'''
            print(s%(basename, int(y/step)*step+1, int(y/step)*step, (int(y/step)+1)*step, basename), file=tf)
        print('', file=tf)
            
        #2. create insert functio
        fhead='''CREATE OR REPLACE FUNCTION %s_insert_trigger()
    RETURNS TRIGGER AS $$
    BEGIN
        IF ( extract(year from NEW.date) > %d and extract(year from NEW.date) <= %d) THEN
            INSERT INTO %s_%d VALUES (NEW.*);''' % (
            basename, int(endyear/step)*step, (int(endyear/step)+1)*step, basename, int(endyear/step)*step+1)
        print(fhead, file=tf)
        s='''    ELSIF ( extract(year from NEW.date) > %d and extract(year from NEW.date) <= %d ) THEN
            INSERT INTO %s_%d VALUES (NEW.*);'''
        
        for y in range(endyear-step, startyear, -step):  #endyear is included in fhead
            print( s%(int(y/step)*step, (int(y/step)+1)*step, basename, int(y/step)*step+1), file=tf)
            
        ftail='''    ELSE
            RAISE EXCEPTION 'Date out of range.';
    END IF;
    RETURN NULL;
    END;
    $$
    LANGUAGE plpgsql;
    '''
        print(ftail, file=tf)
    
        #3 trigger
        ts='''CREATE TRIGGER insert_%s_trigger
    BEFORE INSERT ON %s
    FOR EACH ROW EXECUTE PROCEDURE %s_insert_trigger();
    '''
        print(ts%(basename, basename, basename), file = tf)
    
    
        
        #4. create index
        for y in range(startyear, endyear+1, step):
            print('CREATE INDEX %s_%d_idx ON %s_%d (product_id, date);'
                  %(basename, y, basename, y), file=tf)

if __name__ == '__main__':
    getsql_years()
    print('finished.')
    