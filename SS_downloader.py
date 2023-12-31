from datetime import date, datetime, timedelta
import hmac
import hashlib
import json
from _config import filename_mapping

from urllib.request import urlopen, Request
import requests

from pymongo import MongoClient

URI = 'https://transform.shadowserver.org/api2/'
KEY = 'YOUR API KEY'
SECRET = 'YOUR SECRET KEY'

TIMEOUT = 45

# Import the MongoClient class from the pymongo library.
client = MongoClient("mongodb://localhost:27017")

# Get the database and collection objects.
db = client["feedsdic"]
collection = db["feedsdic"]

def api_call(method, request) -> requests.Response:
    """
    Call the specified api method with a request dictionary.

    """

    url = URI + method

    request['apikey'] = KEY
    request_string = json.dumps(request)

    secret_bytes = bytes(str(SECRET), 'utf-8')
    request_bytes = bytes(request_string, 'utf-8')

    hmac_generator = hmac.new(secret_bytes, request_bytes, hashlib.sha256)
    hmac2 = hmac_generator.hexdigest()

    response = requests.post(url, data=request_bytes, headers={'HMAC2': hmac2})

    return response

def retry_download_until_success() -> requests.Response:
    try:
        return api_call('reports/download', {"id": list_data["id"]})
    except:
        return retry_download_until_success()

if __name__ == '__main__':
    try:
        
        sday = date(2023, 12, 1)
        eday = date(2023, 12, 31)

        difference = eday - sday

        for i in range(difference.days + 1):
            res = api_call('reports/list', {"date":str(sday + timedelta(days=i)), "report":"dominican-republic"})
            
            if res.status_code != 200:
                continue
            
            lists = json.loads(res.content)

            if not isinstance(lists, list):
                continue

            for list_data in lists:
                try:
                    download_response = api_call('reports/download', {"id": list_data["id"]})
                except:
                    download_response = retry_download_until_success()

                if download_response.status_code != 200:
                    continue

                list_content = json.loads(download_response.content)
                feedname = filename_mapping.get(list_data["type"], None)

                for content in list_content:
                    content["feed_name"] = feedname

                if "scan" in list_data["type"]:
                    for content in list_content:
                        if "cert_serial_number" in content:
                            content["cert_serial_number"] = str(content["cert_serial_number"])

                collection.insert_many(list_content, bypass_document_validation=True)

    except Exception as e:
        exit(format(e))

