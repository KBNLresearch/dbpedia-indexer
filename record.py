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
FORMAT = 'json'

PROP_ABSTRACT = 'http://www.w3.org/2000/01/rdf-schema#comment'
PROP_BIRTH_DATE = 'http://dbpedia.org/ontology/birthDate'
PROP_BIRTH_NAME = 'http://dbpedia.org/ontology/birthName'
PROP_BIRTH_PLACE = 'http://dbpedia.org/ontology/birthPlace'
PROP_DEATH_DATE = 'http://dbpedia.org/ontology/deathDate'
PROP_DEATH_PLACE = 'http://dbpedia.org/ontology/deathPlace'
PROP_LABEL = 'http://www.w3.org/2000/01/rdf-schema#label'
PROP_LINK = 'http://dbpedia.org/ontology/wikiPageWikiLink'
PROP_NAME = 'http://xmlns.com/foaf/0.1/name'
PROP_REDIRECT = 'http://dbpedia.org/ontology/wikiPageRedirects'
PROP_SAME_AS = 'http://www.w3.org/2002/07/owl#sameAs'
PROP_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'


def get_prop(uri, prop, subject=True):
    '''
    Retrieve all property values with specified uri as either subject or object.
    '''
    subj = '<' + uri + '>' if subject else '?x'
    obj = '?x' if subject else '<' + uri + '>'
    query = '''
    SELECT ?x WHERE {
        %(subj)s <%(prop)s> %(obj)s .
    }
    ''' % {'subj': subj, 'prop': prop, 'obj': obj}
    query = ' '.join(query.split())

    payload = {'default-graph-uri': DEFAULT_GRAPH_URI, 'format': FORMAT,
            'query': query}
    response = requests.get(VIRTUOSO_URL, params=payload)
    response = response.json()

    values = []
    for triple in response.get('results').get('bindings'):
        values.append(triple.get('x').get('value'))

    return values

def get_record(uri):
    '''
    Retrieve all (relevant) triples with specified uri as subject.
    '''
    query = '''
    SELECT ?p ?o WHERE {
        <%(uri)s> ?p ?o .
    }
    ''' % {'uri': uri}
    query = ' '.join(query.split())

    payload = {'default-graph-uri': DEFAULT_GRAPH_URI, 'format': FORMAT,
            'query': query}

    response = requests.get(VIRTUOSO_URL, params=payload)
    response = response.json()

    record = {}
    for triple in response.get('results').get('bindings'):
        key = triple.get('p').get('value')
        value = triple.get('o').get('value')
        if key in record:
            record[key].append(value)
        else:
            record[key] = [value]

    redirects = get_prop(uri, PROP_REDIRECT, False)
    if redirects:
        record[PROP_REDIRECT] = redirects

    inlinks = len(get_prop(uri, PROP_LINK, False))
    record['inlinks'] = [inlinks]

    return record

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

def normalize(s):
    '''
    Normalize string by removing punctuation and capitalization.
    '''
    chars = ['.', ',', ':', '?', '!', ';', '-', '\u2013', '"', "'"]
    for c in chars:
        s = s.replace(c, ' ')
    s = ' '.join(s.split())
    s = s.lower()
    return s

def uri_to_string(uri):
    '''
    Transform a dbpedia resource uri into a string.
    '''
    s = uri.split('/resource/')[-1]
    s = s.replace('_', ' ')
    if ' (' in s and ')' in s:
        s = s.split(' (')[0]
    s = ' '.join(s.split())
    return s

def clean(record, uri):
    '''
    Extract and clean up the data that is to be indexed.
    '''
    new_record = {}

    new_record['id'] = uri
    new_record['label'] = record[PROP_LABEL][0]
    new_record['abstract'] = record[PROP_ABSTRACT][0]
    new_record['lang'] = 'nl' if uri.startswith('http://nl.') else 'en'
    new_record['inlinks'] = max(record['inlinks'])

    # Ambiguity flag
    if ' (' in uri and ')' in uri:
        new_record['ambig'] = 1
    else:
        new_record['ambig'] = 0

    # Normalized pref label
    pref_label = normalize(new_record['label'])
    new_record['pref_label'] = pref_label
    new_record['pref_label_str'] = pref_label

    # Normalized alt labels
    alt_label = []

    cand = record[PROP_LABEL][1:]
    if PROP_NAME in record:
        cand += record[PROP_NAME]
    if PROP_BIRTH_NAME in record:
        cand += record[PROP_BIRTH_NAME]
    if PROP_REDIRECT in record:
        cand += [uri_to_string(u) for u in record[PROP_REDIRECT]]

    for l in cand:
        l_norm = normalize(l)
        if l_norm != pref_label:
            if l_norm not in alt_label:
                alt_label.append(l_norm)

    for l in alt_label:
        if len(set(l.split()) & set(pref_label.split())) == len(l.split()):
            alt_label.remove(l)

    new_record['alt_label'] = alt_label
    new_record['alt_label_str'] = alt_label

    # Type
    if PROP_TYPE in record:
        new_record['dbo_type'] = list(set([t.split('/')[-1] for t in
            record[PROP_TYPE] if t.startswith('http://dbpedia.org/ontology/')]))
        new_record['schema_type'] = list(set([t.split('/')[-1] for t in
            record[PROP_TYPE] if t.startswith('http://schema.org/')]))

    # Keywords
    # E.g. http://nl.dbpedia.org/resource/Categorie:Amerikaans_hoogleraar
    if PROP_LINK in record:
        keywords = []
        for link in record[PROP_LINK]:
            if link.startswith('http://nl.dbpedia.org/resource/Categorie:'):
                keywords += [k for k in
                    normalize(uri_to_string(link)).split()[1:] if len(k) >= 5]
        keywords = list(set(keywords))
        for k in pref_label.split():
            if k in keywords:
                keywords.remove(k)
    new_record['keyword'] = keywords

    # Birth and death date
    if PROP_BIRTH_DATE in record:
        new_record['birth_year'] = min([int(y.split('-')[0]) for y in
            record[PROP_BIRTH_DATE]])
    if PROP_DEATH_DATE in record:
        new_record['death_year'] = max([int(y.split('-')[0]) for y in
            record[PROP_DEATH_DATE]])

    # Birth and death place
    if PROP_BIRTH_PLACE in record:
        places = []
        for p in record[PROP_BIRTH_PLACE]:
            if p.startswith('http://nl.dbpedia.org/resource/'):
                places.append(normalize(uri_to_string(p)))
        new_record['birth_place'] = list(set(places))

    if PROP_DEATH_PLACE in record:
        places = []
        for p in record[PROP_DEATH_PLACE]:
            if p.startswith('http://nl.dbpedia.org/resource/'):
                places.append(normalize(uri_to_string(p)))
        new_record['death_place'] = list(set(places))

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
    records.append(record)

    # Check for English record if original was Dutch
    if uri.startswith('http://nl.dbpedia.org/resource/'):
        same_as_uris = [u for u in record.get(PROP_SAME_AS) if
                u.startswith('http://dbpedia.org/resource/')]
        if same_as_uris:
            for same_as_uri in same_as_uris:
                records.append(get_record(same_as_uri))

    # Merge records into one
    record = merge(records)
    pprint.pprint(record)
    #sys.exit()
    record = clean(record, uri)
    #pprint.pprint(record)

    return record

if __name__ == "__main__":
    result = index('http://nl.dbpedia.org/resource/Ronald_Reagan')
    pprint.pprint(result)

