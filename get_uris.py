#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import requests
import sys

from bottle import request
from bottle import route
from bottle import default_app

VIRTUOSO_URL = "http://openvirtuoso.kbresearch.nl/sparql?"

application = default_app()

def get_uris(lang='nl'):
    '''
    Retrieve all resource uris for specified language.
    '''
    if lang == 'nl':
        query = '''
        SELECT DISTINCT ?s WHERE {
            ?s <http://www.w3.org/2000/01/rdf-schema#comment> ?p .
            FILTER(
                regex(?s, "^http://nl.dbpedia.org/resource/.{2,}", "i") &&
                !regex(?s, "^http://nl.dbpedia.org/resource/Categorie:", "i") &&
                !regex(?s, "(doorverwijspagina)", "i")
            )
        }
        '''
    else:
        query = '''
        SELECT DISTINCT ?s WHERE {
            ?s <http://www.w3.org/2000/01/rdf-schema#comment> ?p .
            FILTER(
                regex(?s, "http://nl.dbpedia.org/resource/.{2,}", "i") &&
                !regex(?s, "http://nl.dbpedia.org/resource/Categorie:", "i")
            )
        }
        '''
    query = ' '.join(query.split())

    count_query = query.replace('DISTINCT ?s', 'count(DISTINCT ?s) as ?count')

    payload = {
        'default-graph-uri': 'http://nl.dbpedia.org',
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
    Save URIs to plain text file, one URI per line.
    '''
    filename = 'uris_' + lang + '.txt'
    mode = 'ab' if os.path.exists(filename) else 'wb'
    print('Saving batch of length: ' + str(len(record.get('results').get('bindings'))))
    with open(filename, mode) as fh:
        for triple in record.get('results').get('bindings'):
            print(triple.get('s').get('value').encode('utf-8'))
            fh.write(triple.get('s').get('value').encode('utf-8') + '\n'.encode('utf-8'))

if __name__ == "__main__":
    #get_uris(nl)
    get_uris(en)

