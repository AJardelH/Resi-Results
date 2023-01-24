import psycopg2
from config import config

def create_tables():
    '''create tables in the PostgreSQL database'''
    commands = (
        '''
        CREATE TABLE if not exists temptable (
            auctionDate DATE NOT NULL,
            id INTEGER NOT NULL,
            propertyDetailsUrl TEXT NOT NULL,
            price INTEGER,
            result TEXT,
            unitNumber TEXT,
            streetNumber TEXT,
            streetName TEXT,
            streetType TEXT,
            suburb TEXT,
            postcode SMALLINT,
            state TEXT,
            propertyType TEXT,
            bedrooms SMALLINT,
            bathrooms SMALLINT,
            carspaces SMALLINT,  
            agencyId INT,
            agencyName TEXT,
            agent TEXT,
            agencyProfilePageUrl TEXT,
            latitude NUMERIC, 
            longitude NUMERIC
            )
        ''',
        '''
        CREATE TABLE if not exists auctionresults (
            auctionDate DATE NOT NULL,
            id INTEGER NOT NULL,
            propertyDetailsUrl TEXT NOT NULL,
            price INTEGER,
            result TEXT,
            unitNumber TEXT,
            streetNumber TEXT,
            streetName TEXT,
            streetType TEXT,
            suburb TEXT,
            postcode SMALLINT,
            state TEXT,
            propertyType TEXT,
            bedrooms SMALLINT,
            bathrooms SMALLINT,
            carspaces SMALLINT,  
            agencyId INT,
            agencyName TEXT,
            agent TEXT,
            agencyProfilePageUrl TEXT,
            latitude NUMERIC, 
            longitude NUMERIC
            )
        ''',
    )

    conn = None

    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        for command in commands:
            cur.execute(command)

        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    finally:
        if conn is not None:
                conn.close()

if __name__ == '__main__':
    create_tables()
