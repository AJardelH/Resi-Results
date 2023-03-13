import pandas as pd
import sqlalchemy as db
import requests
import psycopg2
from datetime import date
from sqlalchemy import create_engine
from config import authorization
from config import config

#Get access token from Domain Website API using client ID & secret from config file. 
#https://developer.domain.com.au/docs/latest/authentication/oauth
def get_access_token():
    auth_params = authorization()
    auth_url = 'https://auth.domain.com.au/v1/connect/token'
    get_auth = requests.post(auth_url, data = {**auth_params})
    if get_auth.ok:
        None
    else: raise Exception('Error with authorization')
    return get_auth.json()['access_token']

#Get current metadata for sales result data, interested in the auction date. 
#https://developer.domain.com.au/docs/latest/apis/pkg_properties_locations/references/salesresults_head
def get_auction_date():
    access_token = get_access_token()
    auth_headers = {'Authorization':'Bearer ' +access_token,
            'scope':'api_salesresults_read'
            }
    date_url = 'https://api.domain.com.au/v1/salesResults/_head'
    date_req = requests.get(date_url, headers= auth_headers)
    if date_req.ok:
        None
    else: raise Exception('Error with getting auction date')
    date_response = date_req.json()
    string_date = date_response['auctionedDate']
    auction_date = date.fromisoformat(string_date)
    return auction_date

#Create SQL engine for amazon rds db from config
def create_engine_from_settings():
    settings = config()
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**settings))
    return engine

#Need to itereate through cities as API only allows one city per call. 
#depending on data input by Domain some id's are Null. Removed these as they caused duplicates in testing
#strip the geoLocation tag off the longitude and latitude values for simplicity
#manually assign dtype for to_sql as was causing type errors during testing
#insert parsed dataframe into temp table
#https://developer.domain.com.au/docs/latest/apis/pkg_properties_locations/references/salesresults_listings

def get_sales_temp():
    cities_list = ['Melbourne','Sydney','Canberra','Brisbane','Adelaide']
    for i in cities_list:
        cities = i
        engine = create_engine_from_settings()
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
        df.to_sql('temptable', engine, if_exists='append', index=False,
            dtype={'auctiondate':db.types.DATE(),
                'id':db.types.Integer(),
                'propertydetailsurl':db.types.Text(),
                'price': db.types.Integer(),
                'result':db.types.Text(),
                'unitnumber':db.types.Text(),
                'streetnumber':db.types.Text(),
                'streetname':db.types.Text(),
                'streettype':db.types.Text(),
                'suburb':db.types.Text(),
                'postcode':db.types.Integer(),
                'state':db.types.Text(),
                'propertytype':db.types.Text(),
                'bedrooms':db.types.Integer(),
                'bathrooms':db.types.Integer(),
                'carspaces':db.types.Integer(),
                'agencyname':db.types.Text(),
                'agent':db.types.Text(),
                'agencyprofilepageurl':db.types.Text(),
                'latitude':db.types.Numeric(),
                'longitude':db.types.Numeric()}
        )
        #print(df)
        conn = None
        try: 
            conn = psycopg2.connect(**settings)
            conn.commit()
            conn.close()
            index = df.index
            number_of_rows = len(index)
            print(f'{number_of_rows} records inserted for {cities} inserted into temp table')
        except (Exception, psycopg2.DatabaseError) as error:
                print(error)
        finally:
            if conn is not None:
                    conn.close()   

#Copy temp table to permanent table where criteria are not duplicates
def temp_table_to_perm():
    settings = config()
    sqls = (
        '''
        CREATE TABLE if not exists auctionresults (
        auctiondate DATE NOT NULL,
        id INTEGER NOT NULL,
        propertydetailsurl TEXT NOT NULL,
        price INTEGER,
        result TEXT,
        unitnumber TEXT,
        streetnumber TEXT,
        streetname TEXT,
        streettype TEXT,
        suburb TEXT,
        postcode INTEGER,
        state TEXT,
        propertytype TEXT,
        bedrooms INTEGER,
        bathrooms INTEGER,
        carspaces INTEGER,  
        agencyname INTEGER,
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
        propertydetailsurl TEXT NOT NULL,
        price INTEGER,
        result TEXT,
        unitnumber TEXT,
        streetnumber TEXT,
        streetname TEXT,
        streettype TEXT,
        suburb TEXT,
        postcode INTEGER,
        state TEXT,
        propertytype TEXT,
        bedrooms INTEGER,
        bathrooms INTEGER,
        carspaces INTEGER,  
        agencyname TEXT,
        agent TEXT,
        agencyprofilepageurl TEXT,
        latitude NUMERIC, 
        longitude NUMERIC
        )
        '''
        ,
        '''
        INSERT INTO auctionresults(
        auctiondate,
        id,
        postcode,
        propertydetailsurl,
        price, 
        result,
        unitnumber,
        streetnumber,
        streetname,
        streettype,
        suburb,
        state,
        propertytype,
        bedrooms,
        bathrooms,
        carspaces,
        agencyname,
        agent,
        agencyprofilepageurl,
        latitude,
        longitude
        )
        SELECT 
        auctiondate,
        id,
        postcode,
        propertydetailsurl,
        price,
        result,
        unitnumber,
        streetnumber,
        streetname,
        streettype,
        suburb,
        state,
        propertytype,
        bedrooms,
        bathrooms,
        carspaces,
        agencyname,
        agent,
        agencyprofilepageurl, 
        latitude, 
        longitude
        FROM temptable
        WHERE NOT EXISTS (
            SELECT *
            FROM auctionresults
            WHERE auctionresults.id = temptable.id
            AND auctionresults.auctiondate = temptable.auctiondate
            );
        DROP TABLE temptable
        '''
    )
    conn = None
    for sql in sqls:
        try:
            conn = psycopg2.connect(**settings)
            cur = conn.cursor()
            cur.execute(sql)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()