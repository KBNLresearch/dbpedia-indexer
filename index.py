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
import sys
import time

SOLR_UPDATE_URL = 'http://linksolr1.kbresearch.nl/dbpedia/update'
SOLR_JSON_URL = SOLR_UPDATE_URL + '/json/docs'
LOG = 'log.txt'

def skip(i, uri, msg):
    '''
    Log problematic URIs.
    '''
    mode = 'ab' if os.path.exists(LOG) else 'wb'
    with open(LOG, mode) as fh:
        fh.write(uri.encode('utf-8') + (' ' + str(i) + ' ' + msg +
            '\n').encode('utf-8'))

def index_list(f, start=0, stop=None):
    '''
    Retrieve document for each URI on the list and send it to Solr.
    '''
    with open(f, 'rb') as fh:
        i = 0
        for uri in fh:

            # Keep a counter
            i += 1
            if i % 10 == 0:
                print('Processing file {}, record {}'.format(f, i))

            # Start from a specific line number
            if i < start:
                continue
            if stop and i > stop:
                continue

            # Commit after every 100 requests
            if i % 100 == 0:
                r = requests.get(SOLR_UPDATE_URL + '?commit=true')
                print('Committing changes...')
                print(r.text)

            # Get URI
            uri = uri.decode('utf-8')
            uri = uri.split()[0]

            # Get the record data from record.py
            retries = 0
            payload = None
            while not payload and retries < 5:
                try:
                    r = record.get_document(uri)
                    payload = json.dumps(r, ensure_ascii=False).encode('utf-8')
                    #print(payload)
                except:
                    retries += 1
                    time.sleep(1)
                    continue
            if not payload:
                skip(i, uri, 'VOS error')
                continue

            # Send the record data to Solr
            try:
                response = requests.post(SOLR_JSON_URL, data=payload,
                    headers={'Content-Type': 'application/json'}, timeout=60)
                #print(response.text)
                response = response.json()
                status = response['responseHeader']['status']
                assert status == 0
            except:
                skip(i, uri, 'SOLR error')

    # Commit at end of file
    r = requests.get(SOLR_UPDATE_URL + '?commit=true')
    print('Committing changes...')
    print(r.text)

if __name__ == '__main__':

    fname = 'uris_nl.txt' if len(sys.argv) < 2 else sys.argv[1]
    start = 0 if len(sys.argv) < 3 else int(sys.argv[2])
    stop = None if len(sys.argv) < 4 else int(sys.argv[3])

    index_list(fname, start, stop)

