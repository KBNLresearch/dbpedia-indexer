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

# Standard library imports
import json
import logging
import os
import sys
import time

# Third-party library imports
import requests

# DBpedia Indexer imports
import record


SOLR_UPDATE_URL = 'http://linksolr1.kbresearch.nl/dbpedia/update'
SOLR_JSON_URL = SOLR_UPDATE_URL + '/json/docs'

logging.basicConfig(level=logging.INFO)
logging.getLogger('requests').setLevel(logging.WARNING)

formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')

handler = logging.FileHandler('index.log', mode='w')
handler.setFormatter(formatter)
handler.setLevel(logging.ERROR)

logger = logging.getLogger(__name__)
logger.addHandler(handler)


def commit():
    resp = requests.get(SOLR_UPDATE_URL + '?commit=true')
    logger.info('Committing changes...')
    logger.info(resp.text)


def index_list(f, start=0, stop=None):
    '''
    Retrieve document for each URI on the list and send it to Solr.
    '''
    with open(f, 'rb') as fh:
        for i, uri in enumerate(fh):

            # Start from a specific line number
            if i < start or (stop and i > stop):
                continue
            else:
                # Report every 10 requests
                if i % 10 == 0:
                    logger.info('Processing file {}, record {}'.format(f, i))

                # Commit every 100 requests
                if i % 100 == 0:
                    commit()

                # Get URI
                uri = uri.decode('utf-8')
                uri = uri.split()[-1]

                # Get data to be indexed
                retries = 0
                payload = None
                while not payload and retries < 5:
                    try:
                        resp = record.get_document(uri)
                        payload = json.dumps(resp, ensure_ascii=False)
                        payload = payload.encode('utf-8')
                    except Exception as e:
                        time.sleep(1)
                        retries += 1
                        continue

                if not payload:
                    msg = 'VOS error for URI: {}'.format(uri)
                    logger.error(msg)
                    continue

                # Send the record data to Solr
                try:
                    headers = {'Content-Type': 'application/json'}
                    resp = requests.post(SOLR_JSON_URL, data=payload,
                                         headers=headers, timeout=60).json()

                    status = resp['responseHeader']['status']
                    if status != 0:
                        raise Exception()

                except Exception as e:
                    msg = 'SOLR error for URI: {}'.format(uri)
                    logger.error(msg)

    # Commit at end of file
    commit()


if __name__ == '__main__':
    fname = 'uris_nl.txt' if len(sys.argv) < 2 else sys.argv[1]
    start = 0 if len(sys.argv) < 3 else int(sys.argv[2])
    stop = None if len(sys.argv) < 4 else int(sys.argv[3])

    index_list(fname, start, stop)
