#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import pprint
import requests
import sys

from bottle import request
from bottle import route
from bottle import default_app

VIRTUOSO_URL = 'http://openvirtuoso.kbresearch.nl/sparql?'
DEFAULT_GRAPH_URI = 'http://nl.dbpedia.org'

SAME_AS_PROP = 'http://www.w3.org/2002/07/owl#sameAs'

application = default_app()

def get_record(uri):
    '''
    Retrieve all (relevant) triples with specified uri as subject.
    '''
    query = '''
    SELECT ?p ?o WHERE {
        <%(uri)s> ?p ?o .
        FILTER (isLiteral(?o) || regex(?o,'www.wiki') ||
            regex(?o,'//nl.') || regex(?o,'//dbp'))
    }
    ''' % {"uri": uri}

    payload = {'default-graph-uri': DEFAULT_GRAPH_URI, 'format': 'json',
            'query': query}

    response = requests.get(VIRTUOSO_URL, params=payload)
    return response.json()

def transform(record):
    '''
    Format Virtuoso JSON response for further processing.
    '''
    new_record = {}
    for triple in record.get('results').get('bindings'):
        key = triple.get('p').get('value')
        value = triple.get('o').get('value')
        if key in new_record:
            new_record[key].append(value)
        else:
            new_record[key] = [value]
    return new_record

def merge(records):
    '''
    Merge multiple records into one.
    '''
    new_record = {}
    for record in records:
        for key, value in record.items():
            if key in new_record:
                new_record[key] += value
            else:
                new_record[key] = value
    return new_record

def clean(record, uri):
    '''
    Extract and clean up the data that is to be indexed.
    '''
    new_record = {}

    # Identifier
    new_record['id'] = uri

    # Language
    new_record['lang'] = 'nl' if uri.startswith('http://nl.') else 'en'

    # Ambiguity flag
    if ' (' in uri and ')' in uri:
        new_record['ambig'] = 1
    else:
        new_record['ambig'] = 0

    # Label
    field = 'http://www.w3.org/2000/01/rdf-schema#label'
    if field in record:
        new_record['label'] = record[field][0]
    if 'label' in new_record:
        new_record['label_str'] = new_record['label']

    # Name variants
    field = 'http://nl.dbpedia.org/property/naam'
    if field in record:
        new_record['alt_label'] = record[field]
    if 'alt_label' in new_record:
        new_record['alt_label_str'] = new_record['alt_label']

    return new_record

@route('/')
def index(uri=None):
    '''
    Retrieve and process all info about specified uri.
    '''
    if not uri:
        uri = request.query.get('uri')

    # Get original record
    records = []
    record = get_record(uri)
    record = transform(record)
    records.append(record)

    # Check for English record if original was Dutch
    if uri.startswith('http://nl.dbpedia.org/resource/'):
        same_as_uris = [u for u in record.get(SAME_AS_PROP) if
                u.startswith('http://dbpedia.org/resource/')]
        if same_as_uris:
            for same_as_uri in same_as_uris:
                same_as_record = transform(get_record(same_as_uri))
                records.append(same_as_record)

    # Merge records into one
    record = merge(records)
    #pprint.pprint(record)
    #sys.exit()
    record = clean(record, uri)
    #pprint.pprint(record)

    return record

if __name__ == "__main__":
    result = index('http://nl.dbpedia.org/resource/Julie_&_Ludwig')
    pprint.pprint(result)

