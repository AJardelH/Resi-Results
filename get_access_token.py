import requests
from config import authorization

def get_access_token():
    auth_params = authorization()
    auth_url = 'https://auth.domain.com.au/v1/connect/token'
    get_auth = requests.post(auth_url, data = {**auth_params})
    if get_auth.ok:
        None
    else: raise Exception('Error with authorization')
    return get_auth.json()['access_token']

    
if __name__ == '__main__':
    access_token =get_access_token()
    print(access_token)
    