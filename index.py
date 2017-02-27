#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# DBpedia indexer
#
# Copyright (C) 2017 Koninklijke Bibliotheek, National Library of
# the Netherlands
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import json
import os
import record
import requests
import time

LOG = 'log.txt'

SOLR_UPDATE_URL = 'http://sara-backup:8082/solr/dbpedia/update'
SOLR_JSON_URL = SOLR_UPDATE_URL + '/json/docs'

headers = {'Content-Type': 'application/json'}

def skip(uri, msg):
    '''
    Log problematic URIs.
    '''
    mode = 'ab' if os.path.exists(LOG) else 'wb'
    with open(LOG, mode) as fh:
        fh.write(uri.encode('utf-8') + ' '.encode('utf-8')
                + msg.encode('utf-8') + '\n'.encode('utf-8'))

def index_list(f):
    '''
    Retrieve document for each URI on the list and send it to Solr.
    '''
    with open(f, 'rb') as fh:
        i = 0
        for uri in fh:
            # Keep a counter
            i += 1
            if i % 10 == 0:
                print('Processing record:' + str(i))
            # Commit after every 100 requests
            if i % 100 == 0:
                r = requests.get(SOLR_UPDATE_URL + '?commit=true')
                print('Committing changes...')
                print(r.text)
            uri = uri.decode('utf-8')
            uri = uri.split()[0]

            retries = 0
            payload = None
            while not payload and retries < 5:
                try:
                    r = record.get_document(uri)
                    payload = json.dumps(r, ensure_ascii=False).encode('utf-8')
                    # print(payload)
                except:
                    retries += 1
                    time.sleep(1)
                    continue
            if not payload:
                skip(uri, 'VOS error')
                continue

            try:
                response = requests.post(SOLR_JSON_URL, data=payload, headers=headers)
                # print(response.text)
                response = response.json()
                status = response['responseHeader']['status']
                if status != 0:
                    raise
            except:
                skip(uri, 'SOLR error')

if __name__ == "__main__":
    index_list('uris_nl.txt')
    index_list('uris_en.txt')
