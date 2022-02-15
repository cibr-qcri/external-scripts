import json
import re
from datetime import datetime
from elasticsearch import Elasticsearch
from hashlib import sha256


def get_btc(body):
    btc_addr_pat = re.compile(
        r"\b(1[a-km-zA-HJ-NP-Z1-9]{25,34})\b|\b(3[a-km-zA-HJ-NP-Z1-9]{25,34})\b|\b(bc1[a-zA-HJ-NP-Z0-9]{25,39})\b")
    btc = []
    btc_list = re.findall(btc_addr_pat, body)
    if len(btc_list) > 0:
        for item in btc_list[0]:
            if item:
                btc.append(item)
    return btc


def convert_to_es_format(record):
    id = record['tweet']['id']
    body = record['tweet']['body']
    summary = record['actor']['summary']
    tag = {
        'timestamp': int(datetime.now().timestamp() * 1000),
        'type': 'service',
        'source': 'twitter',
        "info": {
            "domain": 'www.twitter.com',
            "url": record['tweet']['link'],
            "tags": {
                "cryptocurrency": {
                    "address": {
                        "btc": get_btc(str(body) + str(summary))
                    }
                },
                "actor": {
                    'id': record['actor']['id'],
                    'link': record['actor']['link'],
                    'preferred_username': record['actor']['preferred_username'],
                    'display_name': record['actor']['display_name'],
                    'summary': record['actor']['summary'],
                },
            },
            "body": body
        }
    }
    es.index('twitter-crawler', body=tag, id=sha256(id.encode("utf-8")).hexdigest())


if __name__ == '__main__':
    es = Elasticsearch(["10.96.4.19"], scheme="http", port=9200, timeout=50, max_retries=10, retry_on_timeout=True)
    with open('user_accountInfo_twitter.json') as file:
        for line in file:
            for record in json.loads(line)['matches']:
                convert_to_es_format(record)
