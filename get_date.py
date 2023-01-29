import requests
from get_access_token import get_access_token
from datetime import date

def get_auction_date():
    access_token = get_access_token()
    auth_headers = {'Authorization':'Bearer ' +access_token,
            'scope':'api_salesresults_read'
            }
    date_url = 'https://api.domain.com.au/v1/salesResults/_head'
    d = requests.get(date_url, headers= auth_headers)
    date_response = d.json()
    string_date = date_response['auctionedDate']
    auction_date = date.fromisoformat(string_date)
    return auction_date


#print(type(get_auction_date()))
