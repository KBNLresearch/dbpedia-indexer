#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import record
import requests

SOLR = "http://sara-backup:8082/solr/dbpedia/"
SOLR_ADD = SOLR + 'update/json/docs'

headers = {'Content-Type': 'application/json'}

with open('uris_nl.txt', 'r') as fh:
    for line in fh:
        try:
            r = record.index(line[:-1].decode('utf-8'))
            payload = json.dumps(r, ensure_ascii=False).encode('utf-8')
            print(payload)
            r = requests.post(SOLR_ADD, data=payload, headers=headers)
            print(r.text)
        except:
            with open('uris_nl_skipped.txt', 'a') as fh:
                fh.write(line)
            continue

