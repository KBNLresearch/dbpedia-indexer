#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import record
import requests

SOLR_URL = 'http://sara-backup:8082/solr/dbpedia/update/json/docs'

headers = {'Content-Type': 'application/json'}

with open('uris_nl.txt', 'rb') as fh:
    for line in fh:
        try:
            rec = record.index(line[:-1].decode('utf-8'))
            payload = json.dumps(rec, ensure_ascii=False).encode('utf-8')
            print(payload)
            r = requests.post(SOLR_URL, data=payload, headers=headers)
            print(r.text)
        except:
            with open('uris_nl_skipped.txt', 'ab') as fh:
                fh.write(line)
            continue

