import pandas as pd
import sqlalchemy as db
import requests
import psycopg2
from datetime import date
from sqlalchemy import create_engine
from config import authorization
from config import config


def get_access_token():
    auth_params = authorization()
    auth_url = 'https://auth.domain.com.au/v1/connect/token'
    get_auth = requests.post(auth_url, data={**auth_params})
    if get_auth.ok:
        None
    else:
        raise Exception('Error with authorization')
    return get_auth.json()['access_token']


def get_auction_date():
    access_token = get_access_token()
    auth_headers = {'Authorization': 'Bearer ' + access_token,
                    'scope': 'api_salesresults_read'}
    date_url = 'https://api.domain.com.au/v1/salesResults/_head'
    date_req = requests.get(date_url, headers=auth_headers)
    if date_req.ok:
        None
    else:
        raise Exception('Error with getting auction date')
    date_response = date_req.json()
    string_date = date_response['auctionedDate']
    auction_date = date.fromisoformat(string_date)
    return auction_date


def create_engine_from_settings():
    settings = config()
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**settings))
    return engine


def get_max_id():
    max_id_query = "SELECT MAX(id) FROM auctionresults;"
    engine = create_engine_from_settings()
    max_id = pd.read_sql(max_id_query, engine).iloc[0, 0]
    return max_id


def get_sales_temp():
    max_id = get_max_id()
    cities_list = ['Melbourne','Sydney','Canberra','Brisbane','Adelaide']
    for city in cities_list:
        engine = create_engine_from_settings()
        auction_date = get_auction_date()
        access_token = get_access_token()
        auth_headers = {'Authorization': 'Bearer ' + access_token,
                        'scope': 'api_salesresults_read'}
        end_url = f'https://api.domain.com.au/v1/salesResults/{city}/listings'
        r = requests.get(end_url, headers=auth_headers)
        sales_response = r.json()
        df = pd.json_normalize(sales_response)
        df = df.rename(columns={'id': 'propertyid'})
        df = df.dropna(subset=['propertyid']) 
        df['id'] = range(max_id + 1, max_id + 1 + len(df))
        df.set_index('id', inplace=True)
        df.columns = df.columns.str.replace('geoLocation.', '', regex=False)
        df = df.drop(columns='agencyId')
        df.columns = df.columns.str.lower()
        df.insert(0, 'auctiondate', auction_date)

        duplicates = df[df.duplicated(subset=['propertyid', 'auctiondate'], keep=False)]
        if not duplicates.empty:
            print(f"Duplicate entries found for {city}. Ignoring {len(duplicates)} duplicate(s).")
            df = df.drop_duplicates(subset=['propertyid', 'auctiondate'], keep=False)

        settings = config()
        df.to_sql('temptable', engine, if_exists='append', index=True,
                  dtype={'auctiondate': db.types.DATE(),
                         'propertyid': db.types.Integer(),
                         'propertydetailsurl': db.types.Text(),
                         'price': db.types.Integer(),
                         'result': db.types.Text(),
                         'unitnumber': db.types.Text(),
                         'streetnumber': db.types.Text(),
                         'streetname': db.types.Text(),
                         'streettype': db.types.Text(),
                         'suburb': db.types.Text(),
                         'postcode': db.types.Integer(),
                         'state': db.types.Text(),
                         'propertytype': db.types.Text(),
                         'bedrooms': db.types.Integer(),
                         'bathrooms': db.types.Integer(),
                         'carspaces': db.types.Integer(),
                         'agencyname': db.types.Text(),
                         'agent': db.types.Text(),
                         'agencyprofilepageurl': db.types.Text(),
                         'latitude': db.types.Numeric(),
                         'longitude': db.types.Numeric()}
                  )

        conn = None
        try:
            conn = psycopg2.connect(**settings)
            conn.commit()
            conn.close()
            index = df.index
            number_of_rows = len(index)
            print(f'{number_of_rows} records inserted for {city} inserted into temp table')
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()


def temp_table_to_perm():
    settings = config()
    sqls = (
        '''
        CREATE TABLE if not exists auctionresults (
        auctiondate DATE NOT NULL,
        propertyid INTEGER NOT NULL,
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
        longitude NUMERIC,
        PRIMARY KEY (auctiondate, propertyid, result, price)
        )
        '''
        ,
        '''
        CREATE TABLE if not exists temptable (
        auctiondate DATE NOT NULL,
        propertyid INTEGER NOT NULL,
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
        propertyid,
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
        propertyid,
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
            WHERE auctionresults.propertyid = temptable.propertyid
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


def lambda_handler(event, context):
     get_sales_temp()
     temp_table_to_perm()