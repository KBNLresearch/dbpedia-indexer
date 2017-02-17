#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import requests

from bottle import request
from bottle import route
from bottle import default_app

VIRTUOSO_URL = "http://openvirtuoso.kbresearch.nl/sparql?"
VIRTUOSO_URL += "default-graph-uri=http://dbpedia.org&format=json&query="

application = default_app()

def get_record(uri):
    '''
    Retrieve all (relevant) triples with specified uri as subject.
    '''
    payload = {}
    payload['default-graph-uri'] = 'http://dbpedia.org'
    payload['format'] = 'json'
    payload['query'] = '''
    SELECT ?p ?o WHERE {
        <%(uri)s> ?p ?o .
        FILTER (isLiteral(?o) || regex(?o,'www.wiki') ||
            regex(?o,'//nl.') || regex(?o,'//dbp'))
    }
    ''' % {"uri":uri}
    response = requests.get(VIRTUOSO_URL, params=payload)
    record = json.loads(str(response.text))
    return record

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
    if not uri:
        uri = request.query.get('uri')
    record = get_record(uri)
    record = transform(record)
    record = merge([record])
    record = clean(record, uri)
    return record

if __name__ == "__main__":
    result = index('http://nl.dbpedia.org/resource/Julie_&_Ludwig')
    print(result)

