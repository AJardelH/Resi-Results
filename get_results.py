import pandas as pd
import sqlalchemy as db
import requests
import psycopg2
from sqlalchemy import create_engine
from config import config
from get_access_token import get_access_token
from get_date import get_auction_date
from datetime import datetime 

def create_engine_from_settings():
    settings = config()
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**settings))
    return engine

def get_sales_temp():
    engine = create_engine_from_settings()
    conn = engine.connect()
    auction_date = get_auction_date()
    access_token = get_access_token()
    auth_headers = {'Authorization':'Bearer ' +access_token,
            'scope':'api_salesresults_read'
            }
    end_url = f'https://api.domain.com.au/v1/salesResults/{cities}/listings'
    r = requests.get(end_url, headers= auth_headers)  
    sales_response = r.json()
    df = pd.json_normalize(sales_response)
    df = df[df.id != 0]
    df.columns = df.columns.str.replace('geoLocation.','', regex=False)
    df = df.drop(columns='agencyId')
    df.columns = df.columns.str.lower()
    df.insert(0,'auctiondate', auction_date)
    #print(type(auction_date))
    settings = config()
    df.to_sql('temptable', con=conn, if_exists='append', index=False)
    conn = psycopg2.connect(**settings)
    conn.commit()
    conn.close()
    index = df.index
    number_of_rows = len(index)
    print(f'{number_of_rows} temp records inserted for {cities}')


def temp_table_to_perm():
    settings = config()
    sqls = (
        '''
        CREATE TABLE if not exists auctionresults (
        auctiondate DATE NOT NULL,
        id INTEGER NOT NULL,
        propertyDetailsUrl TEXT NOT NULL,
        price TEXT,
        result TEXT,
        unitnumber TEXT,
        streetnumber TEXT,
        streetname TEXT,
        streettype TEXT,
        suburb TEXT,
        postcode TEXT,
        state TEXT,
        propertytype TEXT,
        bedrooms TEXT,
        bathrooms TEXT,
        carspaces TEXT,  
        agencyid TEXT,
        agencyname TEXT,
        agent TEXT,
        agencyprofilepageurl TEXT,
        latitude NUMERIC, 
        longitude NUMERIC
        
        )
        '''
        ,
        '''
        CREATE TABLE if not exists temptable (
        auctiondate DATE NOT NULL,
        id INTEGER NOT NULL,
        propertyDetailsUrl TEXT NOT NULL,
        price TEXT,
        result TEXT,
        unitnumber TEXT,
        streetnumber TEXT,
        streetname TEXT,
        streettype TEXT,
        suburb TEXT,
        postcode TEXT,
        state TEXT,
        propertytype TEXT,
        bedrooms TEXT,
        bathrooms TEXT,
        carspaces TEXT,  
        agencyid TEXT,
        agencyname TEXT,
        agent TEXT,
        agencyprofilepageurl TEXT,
        latitude NUMERIC, 
        longitude NUMERIC
        )
        '''
        ,
        '''
        INSERT INTO auctionresults 
        SELECT * FROM temptable
        WHERE NOT EXISTS (SELECT * FROM auctionresults
        WHERE temptable.id = auctionresults.id
        AND temptable.auctiondate = auctionresults.auctiondate);
        DROP TABLE temptable;
        '''
    )
    conn = psycopg2.connect(**settings)
    cur = conn.cursor()
    for sql in sqls:
         cur.execute(sql)
      
    cur.close()
    conn.commit()    

# cities_list = ['Canberra']
cities_list = ['Melbourne','Sydney','Canberra','Brisbane','Adelaide']

for i in cities_list:
     cities = i
     get_sales_temp()
   
temp_table_to_perm()
