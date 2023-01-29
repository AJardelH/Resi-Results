import psycopg2
from config import config

def create_tables():
    '''create tables in the PostgreSQL database'''
    commands = (
        '''
        CREATE TABLE if not exists temptable (
            auctiondate DATE NOT NULL,
            id INTEGER NOT NULL,
            propertydetailsurl TEXT NOT NULL,
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
            agencyid INT,
            agencyname TEXT,
            agent TEXT,
            agencyprofilepageUrl TEXT,
            latitude NUMERIC, 
            longitude NUMERIC
            
            )
        ''',
        '''
        CREATE TABLE if not exists auctionresults (
            auctiondate DATE NOT NULL,
            id INTEGER NOT NULL,
            propertydetailsurl TEXT NOT NULL,
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
            agencyprofilepageUrl TEXT,
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

#if __name__ == '__main__':
    #create_tables()
