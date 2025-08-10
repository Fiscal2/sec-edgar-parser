import os
import requests

HEADERS = os.getenv('HEADERS')

class GetRequest:
    def __init__(self, url):
        response = requests.get(url, headers=HEADERS)
        response.encoding = 'utf-8'
        if response.status_code != requests.codes.ok:
            raise RequestException('{}: {}'.format(response.status_code, response.text))
        
        self.response = response

class RequestException(Exception):
    pass