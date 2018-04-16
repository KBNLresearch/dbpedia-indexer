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
import argparse
import json
import logging
import sys
import time

# Third-party library imports
import requests

# DBpedia Indexer imports
import record
import update


SOLR_UPDATE_URL = 'http://linksolr1.kbresearch.nl/dbpedia/update'
SOLR_JSON_URL = SOLR_UPDATE_URL + '/json/docs'

logging.basicConfig(level=logging.INFO)
logging.getLogger('requests').setLevel(logging.WARNING)

formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')

handler = logging.FileHandler('index.log', mode='a')
handler.setFormatter(formatter)
handler.setLevel(logging.ERROR)

logger = logging.getLogger(__name__)
logger.addHandler(handler)


def commit():
    '''
    Commit changes to Solr index.
    '''
    logger.info('Committing changes...')
    resp = requests.get(SOLR_UPDATE_URL + '?commit=true')


def index_list(in_file, action='full', start=0, stop=0):
    '''
    Retrieve document for each URI on the list and send it to Solr.
    '''
    with open(in_file, 'rb') as fh:
        for i, uri in enumerate(fh):

            # Start from a specific line number
            if i < start or (stop and i > stop):
                continue
            else:
                # Report every 10 requests
                if i % 10 == 0:
                    logger.info('Processing file {}, record {}'.format(in_file,
                                                                       i))

                # Commit every 100 requests
                if i % 100 == 0:
                    commit()

                # Get URI
                uri = uri.decode('utf-8')
                uri = uri.split()[-1]

                # Get data to be indexed
                retries = 0
                payload = None
                skip = False

                while not payload and retries < 5:
                    try:
                        if action == 'full':
                            doc = record.get_document(uri)
                        elif action == 'ocr':
                            doc = update.get_document_ocr(uri)
                        elif action == 'topics':
                            doc = update.get_document_topics(uri)
                        elif action == 'last_part':
                            doc = update.get_document_last_part(uri)
                            if not doc:
                                skip = True
                                break
                        elif action == 'remove_last_part':
                            doc = update.get_document_remove_last_part(uri)
                            if not doc:
                                skip = True
                                break
                        elif action == 'abstract':
                            doc = update.get_document_abstract(uri)
                        elif action == 'abstract_norm':
                            doc = update.get_document_abstract_norm(uri)
                        elif action == 'vectors':
                            doc = update.get_document_vectors(uri)
                        elif action == 'vectors_bin':
                            doc = update.get_document_vectors_bin(uri)
                        elif action == 'remove_vectors_bin':
                            doc = update.get_document_remove_vectors_bin(uri)

                        payload = json.dumps(doc, ensure_ascii=False)
                        payload = payload.encode('utf-8')

                    except Exception as e:
                        time.sleep(1)
                        retries += 1
                        continue

                if skip:
                    # logger.info('Skipping URI: {}'.format(uri))
                    continue

                if not payload:
                    msg = 'VOS error for URI: {}'.format(uri)
                    logger.error(msg)
                    continue

                # Send the data to Solr
                # logger.info('Indexing URI: {}'.format(uri))

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
    parser = argparse.ArgumentParser()

    parser.add_argument('--input', required=False, type=str,
                        default='uris_nl.txt', help='path to input file')
    parser.add_argument('--action', required=False, type=str,
                        default='full', help='type of indexer action')
    parser.add_argument('--start', required=False, type=int,
                        default=0, help='start position in input file')
    parser.add_argument('--stop', required=False, type=int,
                        default=0, help='stop position in input file')

    args = parser.parse_args()

    index_list(vars(args)['input'], vars(args)['action'],
               vars(args)['start'], vars(args)['stop'])
