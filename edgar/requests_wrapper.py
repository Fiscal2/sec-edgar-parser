import json
import os
import requests

HEADERS = {
    "User-Agent": "Zac G zacharyross3@gmail.com",  # use real contact email
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov"
}

class GetRequest:
    def __init__(self, url):
        response = requests.get(url, headers=HEADERS)
        response.encoding = 'utf-8'
        if response.status_code != requests.codes.ok:
            raise RequestException('{}: {}'.format(response.status_code, response.text))
        
        self.response = response

class RequestException(Exception):
    pass