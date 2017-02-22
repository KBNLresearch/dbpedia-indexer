#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import record
import requests
import time

URIS_NL = 'uris_nl.txt'
URIS_EN = 'uris_en.txt'
LOG = 'log.txt'

SOLR_URL = 'http://sara-backup:8082/solr/dbpedia/update/json/docs'

headers = {'Content-Type': 'application/json'}

def skip(uri):
    mode = 'ab' if os.path.exists(LOG) else 'wb'
    with open(LOG, mode) as fh:
        fh.write(uri)

for f in [URIS_EN, URIS_EN]:
    with open(f, 'rb') as fh:
        for uri in fh:
            retries = 0
            payload = None
            while not payload and retries < 5:
                try:
                    r = record.index(uri[:-1].decode('utf-8'))
                    payload = json.dumps(r, ensure_ascii=False)
                    print(payload)
                except:
                    retries += 1
                    time.sleep(1)
                    continue
            if not payload:
                skip(uri)
                continue
            '''
            try:
                response = requests.post(SOLR_URL, data=payload, headers=headers)
                print(response.text)
            except:
                skip(uri)
            '''