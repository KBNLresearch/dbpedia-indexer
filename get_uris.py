#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import requests

from bottle import request
from bottle import route
from bottle import default_app

VIRTUOSO_URL = 'http://openvirtuoso.kbresearch.nl/sparql?
DEFAULT_GRAPH_URI = 'http://nl.dbpedia.org'

application = default_app()

def get_uris(lang='nl'):
    '''
    Retrieve all relevant resource uris for specified language.
    '''
    if lang == 'nl':
        query = '''
        SELECT DISTINCT ?s WHERE {
            ?s <http://www.w3.org/2000/01/rdf-schema#comment> ?o .
            FILTER(
                REGEX(?s, "^http://nl.dbpedia.org/resource/.{2,}", "i") &&
                !REGEX(?s, "(doorverwijspagina)", "i")
            )
        }
        '''
    else:
        query = '''
        SELECT DISTINCT ?s WHERE {
            ?s <http://www.w3.org/2000/01/rdf-schema#comment> ?o .
            FILTER(
                REGEX(?s, "http://dbpedia.org/resource/.{2,}", "i") &&
                !REGEX(?s, "(disambiguation)", "i")
            )
            MINUS {
                ?t <http://www.w3.org/2002/07/owl#sameAs> ?s .
                ?t <http://www.w3.org/2000/01/rdf-schema#comment> ?q .
                FILTER(
                    REGEX(?t, "^http://nl.dbpedia.org/resource/.{2,}", "i")
                )
            }
        }
        '''
    query = ' '.join(query.split())
    count_query = query.replace('DISTINCT ?s', 'count(DISTINCT ?s) as ?count')

    payload = {
        'default-graph-uri': DEFAULT_GRAPH_URI,
        'format': 'json',
        'query': count_query
    }
    response = requests.get(VIRTUOSO_URL, params=payload)
    count = int(response.json().get('results').get('bindings')[0].get('count').get('value'))
    print('Total number of records found: ' + str(count))

    limit = 1000000
    offset = 0
    while offset < count:
        print('Retrieving batch with offset: ' + str(offset))
        payload['query'] = query + ' LIMIT ' + str(limit) + ' OFFSET ' + str(offset)
        response = requests.get(VIRTUOSO_URL, params=payload)
        save_uris(response.json(), lang)
        offset += limit

def save_uris(record, lang='nl'):
    '''
    Save uris to plain text file, one uri per line.
    '''
    filename = 'uris_' + lang + '.txt'
    mode = 'ab' if os.path.exists(filename) else 'wb'
    print('Saving batch of length: ' + str(len(record.get('results').get('bindings'))))
    with open(filename, mode) as fh:
        for triple in record.get('results').get('bindings'):
            fh.write(triple.get('s').get('value').encode('utf-8') + '\n'.encode('utf-8'))

if __name__ == "__main__":
    get_uris('nl')
    get_uris('en')

