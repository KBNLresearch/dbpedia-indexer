#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import requests

from bottle import request
from bottle import route
from bottle import default_app

VIRTUOSO_URL = "http://openvirtuoso.kbresearch.nl/sparql?"
#VIRTUOSO_URL += "default-graph-uri=http://dbpedia.org&format=json&query="

application = default_app()

def get_uris(lang='nl'):
    '''
    Retrieve all resource uris for specified language.
    '''
    payload = {
        'default-graph-uri': 'http://dbpedia.org',
        'format': 'json'
    }
    if lang == 'nl':
        payload['query'] = '''
        SELECT DISTINCT ?s WHERE {
            ?s <http://www.w3.org/2000/01/rdf-schema#label> ?p .
            FILTER(
                regex(?s, "http://nl.dbpedia.org/resource/.{2,}", "i") &&
                !regex(?s, "http://nl.dbpedia.org/resource/Categorie:", "i")
            )
        }
        '''

    response = requests.get(VIRTUOSO_URL, params=payload)
    #print(response.text)
    return response.json()

def save_uris(record):
    '''
    Format Virtuoso JSON response for further processing.
    '''
    with open('uris_nl.txt', 'w') as fh:
        for triple in record.get('results').get('bindings'):
            print(triple.get('s').get('value').encode('utf-8'))
            fh.write(triple.get('s').get('value').encode('utf-8') + '\n')

if __name__ == "__main__":
    result = get_uris()
    save_uris(result)
    #print(result)

