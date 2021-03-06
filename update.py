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
import base64
import json
import os
import pprint
import struct
import sys

# Third-party library imports
import requests

# Import DAC modules
sys.path.insert(0, os.path.join(*[os.path.dirname(
    os.path.realpath(__file__)), '..', 'dac', 'dac']))
import dictionary
import utilities

SOLR_URL = 'http://linksolr1.kbresearch.nl/dbpedia/query?'
TOPICS_URL = 'http://kbresearch.nl/topics/?'
W2V_URL = 'http://kbresearch.nl/word2vec/vectors?'


def get_current(uri):
    payload = {}
    payload['q'] = 'id:"{}"'.format(uri)
    payload['wt'] = 'json'

    resp = requests.get(SOLR_URL, params=payload, timeout=60).json()

    return resp['response']['docs'][0]


def get_document_ocr(uri):

    doc = get_current(uri)

    if 'pref_label' in doc:
        pref_label_ocr = utilities.normalize_ocr(doc['pref_label'])
        doc['pref_label_ocr'] = doc['pref_label_str_ocr'] = pref_label_ocr

    if 'alt_label' in doc:
        alt_label_ocr = [utilities.normalize_ocr(label) for label in
                         doc['alt_label']]
        doc['alt_label_ocr'] = doc['alt_label_str_ocr'] = alt_label_ocr

    if 'last_part' in doc:
        last_part_ocr = utilities.normalize_ocr(doc['last_part'])
        doc['last_part_ocr'] = doc['last_part_str_ocr'] = last_part_ocr

    return doc


def get_document_topics(uri):

    doc = get_current(uri)

    resp = requests.get(TOPICS_URL, params={'url': uri}, timeout=300)
    if resp.status_code != 200:
        raise Exception('Error retrieving topics')

    resp = resp.json()

    for t in resp['topics']:
        doc['topic_{}'.format(t)] = float('{0:.3f}'.format(
            resp['topics'][t]))

    for t in resp['types']:
        doc['dbo_type_{}'.format(t)] = float('{0:.3f}'.format(
            resp['types'][t]))

    return doc


def get_document_last_part(uri):

    doc = get_current(uri)

    if ('dbo_type' not in doc and doc['dbo_type_person'] >= 0.75 and
            'last_part' not in doc):
        last_part = utilities.get_last_part(doc['pref_label'],
                                            exclude_first_part=True)
        if last_part:
            doc['last_part'] = last_part
            doc['last_part_str'] = last_part

            last_part_ocr = utilities.normalize_ocr(doc['last_part'])
            doc['last_part_ocr'] = last_part_ocr
            doc['last_part_str_ocr'] = last_part_ocr

            return doc

    return None


def get_document_remove_last_part(uri):

    doc = get_current(uri)

    if ('last_part' in doc and 'dbo_type' in doc and 'Person' not in
            doc['dbo_type']):

        del doc['last_part']
        del doc['last_part_str']
        del doc['last_part_ocr']
        del doc['last_part_str_ocr']

        return doc

    return None


def get_document_abstract(uri):

    doc = get_current(uri)

    if 'abstract' not in doc:
        doc['abstract'] = '.'

    return doc


def get_document_abstract_norm(uri):

    doc = get_current(uri)

    bow = utilities.tokenize(doc['abstract'], max_sent=5)

    doc['abstract_norm'] = ' '.join(bow)
    doc['abstract_token'] = list(set([t for t in bow if len(t) > 5]))[:15]

    return doc


def get_document_vectors(uri):

    doc = get_current(uri)
    del doc['_version_']

    # Wikidata
    if 'uri_wd' in doc:
        payload = {'source': doc['uri_wd'].split('/')[-1]}
        response = requests.get(W2V_URL, params=payload, timeout=300)
        data = response.json()
        if data['vectors']:
            data = [float('{0:.3f}'.format(f)) for f in data['vectors'][0]]
            doc['vector'] = json.dumps(data)

    # Abstract and keyword tokens
    tokens = []
    if 'abstract_token' in doc:
        tokens.extend(doc['abstract_token'])
    if 'keyword' in doc:
        tokens.extend(doc['keyword'])

    if tokens:
        if 'pref_label' in doc:
            tokens = [t for t in tokens if t not in doc['pref_label'].split()]
        tokens = [t for t in tokens if t not in dictionary.unwanted and
                  len(t) >= 5]

        payload = {'source': ' '.join(list(set(tokens)))}
        response = requests.get(W2V_URL, params=payload, timeout=300)
        data = response.json()['vectors']
        if data:
            doc['abstract_vector'] = [json.dumps([float('{0:.3f}'.format(f))
                                                  for f in v]) for v in data]

    return doc


def get_document_vectors_bin(uri):

    doc = get_current(uri)
    del doc['_version_']

    # Wikidata
    if 'uri_wd' in doc:
        payload = {'source': doc['uri_wd'].split('/')[-1]}
        response = requests.get(W2V_URL, params=payload, timeout=300)
        data = response.json()

        if data['vectors']:
            listFloatIn = data['vectors'][0]
            bufIn = struct.pack('!%sd' % len(listFloatIn), *listFloatIn)
            doc['vector_bin'] = base64.b64encode(bufIn).decode('ascii')

    # Abstract and keyword tokens
    tokens = []
    if 'abstract_token' in doc:
        tokens.extend(doc['abstract_token'])
    if 'keyword' in doc:
        tokens.extend(doc['keyword'])

    if tokens:
        if 'pref_label' in doc:
            tokens = [t for t in tokens if t not in doc['pref_label'].split()]
        tokens = [t for t in tokens if t not in dictionary.unwanted and
                  len(t) >= 5]

        payload = {'source': ' '.join(list(set(tokens)))}
        response = requests.get(W2V_URL, params=payload, timeout=300)
        data = response.json()

        if data['vectors']:
            doc['abstract_vector_bin'] = []
            for v in data['vectors']:
                bufIn = struct.pack('!%sd' % len(v), *v)
                doc['abstract_vector_bin'].append(base64.b64encode(bufIn).decode('ascii'))

    return doc


def get_document_remove_vectors_bin(uri):
    doc = get_current(uri)

    if 'vector_bin' in doc:
        del doc['vector_bin']
    if 'abstract_vector_bin' in doc:
        del doc['abstract_vector_bin']
    return doc


def normalize_consonants(s):
    if len(s) >= 2 and s[-1] == s[-2]:
        return s[:-1]
    return s


def get_document_normalize_consonants(uri):
    doc = get_current(uri)

    if 'pref_label' in doc:
        doc['pref_label'] = doc['pref_label_str'] = normalize_consonants(
                doc['pref_label'])
    if 'alt_label' in doc:
        doc['alt_label'] = doc['alt_label_str'] = [normalize_consonants(s) for
                s in doc['alt_label']]
    if 'wd_alt_label' in doc:
        doc['wd_alt_label'] = doc['wd_alt_label_str'] = [normalize_consonants(s) for
                s in doc['wd_alt_label']]
    if 'last_part' in doc:
        doc['last_part'] = doc['last_part_str'] = normalize_consonants(
                doc['last_part'])

    if 'pref_label' in doc:
        pref_label_ocr = utilities.normalize_ocr(doc['pref_label'])
        doc['pref_label_ocr'] = doc['pref_label_str_ocr'] = pref_label_ocr

    if 'alt_label' in doc:
        alt_label_ocr = [utilities.normalize_ocr(label) for label in
                         doc['alt_label']]
        doc['alt_label_ocr'] = doc['alt_label_str_ocr'] = alt_label_ocr

    if 'last_part' in doc:
        last_part_ocr = utilities.normalize_ocr(doc['last_part'])
        doc['last_part_ocr'] = doc['last_part_str_ocr'] = last_part_ocr

    return doc


if __name__ == '__main__':
    if len(sys.argv) > 1:
        uri = sys.argv[1]
    else:
        uri = 'http://nl.dbpedia.org/resource/Albert_Einstein'

    doc = get_document_normalize_consonants(uri)
    pprint.pprint(doc)

