#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import record
import requests

SOLR = "http://sara-backup:8082/solr/dbpedia/"
SOLR_ADD = SOLR + 'update/json/docs'

headers = {'Content-Type': 'application/json'}

with open('identifiers_nl.txt', 'rb') as fh:
    for line in fh:
        payload = json.dumps(record.index(line[:-1]), ensure_ascii=False)
        r = requests.post(SOLR_ADD, data=payload, headers=headers)

        print(r.text)

