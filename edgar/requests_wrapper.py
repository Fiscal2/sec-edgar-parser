import requests

headers = {
            "User-Agent": "Zac G AdminContact@samplecompanydomain.com",  # Use real contact email
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov"
        }

class GetRequest:
    def __init__(self, url):
        response = requests.get(url, headers=headers)
        response.encoding = 'utf-8'
        if response.status_code != requests.codes.ok:
            raise RequestException('{}: {}'.format(response.status_code, response.text))
        
        self.response = response

class RequestException(Exception):
    pass