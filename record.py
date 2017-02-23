#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import pprint
import requests
import xml.etree.ElementTree as ET

from bottle import request
from bottle import route
from bottle import default_app

from unidecode import unidecode

VIRTUOSO_URL = 'http://openvirtuoso.kbresearch.nl/sparql?'
DEFAULT_GRAPH_URI = 'http://nl.dbpedia.org'
FORMAT = 'xml'

PROP_ABSTRACT = 'http://www.w3.org/2000/01/rdf-schema#comment'
PROP_ALIAS = 'http://dbpedia.org/ontology/alias'
PROP_BIRTH_DATE = 'http://dbpedia.org/ontology/birthDate'
PROP_BIRTH_NAME = 'http://dbpedia.org/ontology/birthName'
PROP_BIRTH_PLACE = 'http://dbpedia.org/ontology/birthPlace'
PROP_DEATH_DATE = 'http://dbpedia.org/ontology/deathDate'
PROP_DEATH_PLACE = 'http://dbpedia.org/ontology/deathPlace'
PROP_GIVEN_NAME = 'http://xmlns.com/foaf/0.1/givenName'
PROP_LABEL = 'http://www.w3.org/2000/01/rdf-schema#label'
PROP_LINK = 'http://dbpedia.org/ontology/wikiPageWikiLink'
PROP_LONG_NAME = 'http://dbpedia.org/ontology/longName'
PROP_NAME = 'http://xmlns.com/foaf/0.1/name'
PROP_NICK_NAME = 'http://xmlns.com/foaf/0.1/nick'
PROP_REDIRECT = 'http://dbpedia.org/ontology/wikiPageRedirects'
PROP_SAME_AS = 'http://www.w3.org/2002/07/owl#sameAs'
PROP_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'

application = default_app()

def get_prop(uri, prop, subject=True):
    '''
    Retrieve all property values with specified uri as either subject or object.
    '''
    print(uri, prop, subject)
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

    root = ET.fromstring(response.text)

    values = []
    for result in root[1]:
        values.append(result[0][0].text)

    return values

def get_record(uri):
    '''
    Retrieve all (relevant) triples with specified uri as subject.
    '''
    print(uri)
    query = '''
    SELECT ?p ?o WHERE {
        <%(uri)s> ?p ?o .
        FILTER(isLiteral(?o) || regex(?o, 'http://dbpedia.org') ||
            regex(?o, 'http://nl.dbpedia.org')||regex(?o, 'http://schema.org'))
    }
    ''' % {'uri': uri}
    query = ' '.join(query.split())

    payload = {'default-graph-uri': DEFAULT_GRAPH_URI, 'format': FORMAT,
            'query': query}

    response = requests.get(VIRTUOSO_URL, params=payload)

    root = ET.fromstring(response.text)

    record = {}
    for result in root[1]:
        key = result[0][0].text
        value = result[1][0].text
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
    chars = ['.', ',', ':', '?', '!', ';', '-', '\u2013']
    for c in chars:
        s = s.replace(c, ' ')
    s = unidecode(s)
    s = s.lower()
    s = ' '.join(s.split())
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

    # Normalized last part
    parts = pref_label.split()
    last_part = None
    for part in reversed(parts):
        if not part.isdigit():
            last_part = part
            break
    if last_part:
        new_record['last_part'] = last_part
        new_record['last_part_str'] = last_part

    # Normalized alt labels
    alt_label = []

    cand = record[PROP_LABEL][1:]
    props = [PROP_NAME, PROP_NICK_NAME, PROP_BIRTH_NAME, PROP_GIVEN_NAME,
        PROP_LONG_NAME, PROP_ALIAS]
    for p in props:
        if p in record:
            cand += record[p]
    if PROP_REDIRECT in record:
        cand += [uri_to_string(u) for u in record[PROP_REDIRECT]]

    for l in cand:
        l_norm = normalize(l)
        if l_norm != pref_label:
            if l_norm not in alt_label:
                alt_label.append(l_norm)

    for l in alt_label[:]:
        if len(set(l.split()) & set(pref_label.split())) == len(l.split()):
            alt_label.remove(l)

    for l in alt_label[:]:
        if (l.find('/') > -1 or l.find('|') > -1 or l.find('(') > -1 or
                l.find(')') > -1):
            alt_label.remove(l)

    new_record['alt_label'] = alt_label
    new_record['alt_label_str'] = alt_label

    # Types
    if PROP_TYPE in record:
        dbo_types = list(set([t.split('/')[-1] for t in
            record[PROP_TYPE] if t.startswith('http://dbpedia.org/ontology/')
            and t.find('Wikidata:') < 0]))
        if dbo_types:
            new_record['dbo_type'] = dbo_types
        schema_types = list(set([t.split('/')[-1] for t in
            record[PROP_TYPE] if t.startswith('http://schema.org/')]))
        if schema_types:
            new_record['schema_type'] = schema_types

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
        cand = []
        for date in record[PROP_BIRTH_DATE]:
            try:
                # E.g. -013-10-07+01:00
                cand.append(int(date[:4]))
            except:
                continue
        if cand:
            new_record['birth_year'] = min(cand)
    if PROP_DEATH_DATE in record:
        cand = []
        for date in record[PROP_DEATH_DATE]:
            try:
                # E.g. -013-10-07+01:00
                cand.append(int(date[:4]))
            except:
                continue
        if cand:
            new_record['death_year'] = min(cand)

    # Birth and death place
    if PROP_BIRTH_PLACE in record:
        places = [normalize(uri_to_string(p)) for p in record[PROP_BIRTH_PLACE]
            if p.startswith('http://nl.dbpedia.org/resource/')]
        if not places:
            places = [normalize(uri_to_string(p)) for p in record[PROP_BIRTH_PLACE]
                if p.startswith('http://dbpedia.org/resource/')]
        new_record['birth_place'] = list(set(places))

    if PROP_DEATH_PLACE in record:
        places = [normalize(uri_to_string(p)) for p in record[PROP_DEATH_PLACE]
            if p.startswith('http://nl.dbpedia.org/resource/')]
        if not places:
            places = [normalize(uri_to_string(p)) for p in record[PROP_DEATH_PLACE]
                if p.startswith('http://dbpedia.org/resource/')]
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
        same_as_uris = []
        if PROP_SAME_AS in record:
            same_as_uris = [u for u in record.get(PROP_SAME_AS) if
                    u.startswith('http://dbpedia.org/resource/')]
        if same_as_uris:
            for same_as_uri in same_as_uris:
                records.append(get_record(same_as_uri))

    # Merge records into one
    record = merge(records)
    record = clean(record, uri)
    return record

if __name__ == "__main__":
    result = index('http://nl.dbpedia.org/resource/Drusus_Claudius_Nero')
    pprint.pprint(result)

