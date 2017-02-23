#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import record
import requests
import time

TEST = 'uris_test.txt'
URIS_NL = 'uris_nl.txt'
URIS_EN = 'uris_en.txt'
LOG = 'log.txt'

SOLR_URL = 'http://sara-backup:8082/solr/dbpedia/update/json/docs'

headers = {'Content-Type': 'application/json'}

def skip(uri, msg):
    mode = 'ab' if os.path.exists(LOG) else 'wb'
    with open(LOG, mode) as fh:
        fh.write(uri.encode('utf-8') + ' '.encode('utf-8')
                + msg.encode('utf-8') + '\n'.encode('utf-8'))

for f in [URIS_NL, URIS_EN]:
    with open(f, 'rb') as fh:
        for uri in fh:
            uri = uri.decode('utf-8')
            uri = uri.split()[0]

            retries = 0
            payload = None
            while not payload and retries < 5:
                try:
                    r = record.index(uri)
                    payload = json.dumps(r, ensure_ascii=False).encode('utf-8')
                    print(payload)
                except:
                    retries += 1
                    time.sleep(1)
                    continue
            if not payload:
                skip(uri, 'VOS error')
                continue

            try:
                response = requests.post(SOLR_URL, data=payload, headers=headers)
                print(response.text)
                response = response.json()
                status = response['responseHeader']['status']
                if status != 0:
                    raise
            except:
                skip(uri, 'SOLR error')

