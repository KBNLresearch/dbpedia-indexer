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
import os
import pprint
import re
import sys
import urllib
import xml.etree.ElementTree as ET

# Third-party library imports
import requests

# Import DAC modules
sys.path.insert(0, os.path.join(*[os.path.dirname(
    os.path.realpath(__file__)), '..', 'dac', 'dac']))
import utilities

VIRTUOSO_URL = 'http://openvirtuoso.kbresearch.nl/sparql?'
WD_URL = 'https://www.wikidata.org/wiki/Special:EntityData/{}.json'
TOPICS_URL = 'http://kbresearch.nl/topics/?'

DEFAULT_GRAPH_URI = 'http://nl.dbpedia.org'
FORMAT = 'xml'

PROP_ABSTRACT = 'http://dbpedia.org/ontology/abstract'
PROP_ALIAS = 'http://dbpedia.org/ontology/alias'
PROP_BIRTH_DATE = 'http://dbpedia.org/ontology/birthDate'
PROP_BIRTH_NAME = 'http://dbpedia.org/ontology/birthName'
PROP_BIRTH_PLACE = 'http://dbpedia.org/ontology/birthPlace'
PROP_COMMENT = 'http://www.w3.org/2000/01/rdf-schema#comment'
PROP_DEATH_DATE = 'http://dbpedia.org/ontology/deathDate'
PROP_DEATH_PLACE = 'http://dbpedia.org/ontology/deathPlace'
PROP_DISAMBIGUATES = 'http://dbpedia.org/ontology/wikiPageDisambiguates'
PROP_GIVEN_NAME = 'http://xmlns.com/foaf/0.1/givenName'
PROP_LABEL = 'http://www.w3.org/2000/01/rdf-schema#label'
PROP_LINK = 'http://dbpedia.org/ontology/wikiPageWikiLink'
PROP_LONG_NAME = 'http://dbpedia.org/ontology/longName'
PROP_NAME = 'http://xmlns.com/foaf/0.1/name'
PROP_NICK_NAME = 'http://xmlns.com/foaf/0.1/nick'
PROP_REDIRECT = 'http://dbpedia.org/ontology/wikiPageRedirects'
PROP_SAME_AS = 'http://www.w3.org/2002/07/owl#sameAs'
PROP_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'


def get_prop(uri, prop, subject=True):
    '''
    Retrieve all property values with specified uri as either subject or
    object.
    '''
    subj = '<' + uri + '>' if subject else '?x'
    obj = '?x' if subject else '<' + uri + '>'
    query = '''
    SELECT ?x WHERE {
        %(subj)s <%(prop)s> %(obj)s .
    }
    ''' % {'subj': subj, 'prop': prop, 'obj': obj}
    query = ' '.join(query.split())

    payload = {
        'default-graph-uri': DEFAULT_GRAPH_URI,
        'format': FORMAT,
        'query': query
        }
    response = requests.get(VIRTUOSO_URL, params=payload)
    s = re.sub('&#([0-9]+);', '', response.text)
    root = ET.fromstring(s)

    values = []
    for result in root[1]:
        if result[0][0].text:
            values.append(result[0][0].text)

    return values


def get_record(uri):
    '''
    Retrieve all (relevant) triples with specified uri as subject.
    '''
    query = '''
    SELECT ?p ?o WHERE {
        <%(uri)s> ?p ?o .
        FILTER(isLiteral(?o) || regex(?o, 'http://dbpedia.org') || regex(?o,
        'http://www.wikidata.org/entity') || regex(?o, 'http://nl.dbpedia.org')
        || regex(?o, 'http://schema.org'))
    }
    ''' % {'uri': uri}
    query = ' '.join(query.split())

    payload = {
        'default-graph-uri': DEFAULT_GRAPH_URI,
        'format': FORMAT,
        'query': query
        }
    response = requests.get(VIRTUOSO_URL, params=payload)
    s = re.sub('&#([0-9]+);', '', response.text)
    root = ET.fromstring(s)

    record = {}
    for result in root[1]:
        key = result[0][0].text
        value = result[1][0].text
        if value:
            if key in record:
                record[key].append(value)
            else:
                record[key] = [value]

    redirects = get_prop(uri, PROP_REDIRECT, False)
    if redirects:
        record[PROP_REDIRECT] = redirects

    disambiguations = get_prop(uri, PROP_DISAMBIGUATES, False)
    if disambiguations:
        record[PROP_DISAMBIGUATES] = disambiguations

    inlinks = len(get_prop(uri, PROP_LINK, False))
    record['inlinks'] = [inlinks]

    record = collapse(record, [PROP_ABSTRACT, PROP_COMMENT])
    record = collapse(record, [PROP_NAME, PROP_BIRTH_NAME, PROP_GIVEN_NAME,
                               PROP_LONG_NAME, PROP_ALIAS, PROP_NICK_NAME])

    return record


def collapse(record, fields):
    '''
    Collapse a list of fields onto the first one.
    '''
    if fields[0] not in record:
        record[fields[0]] = []
    for f in fields[1:]:
        if f in record:
            record[fields[0]] += record[f]
    for f in fields[1:]:
        if f in record:
            del record[f]
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


def remove_spec(s):
    '''
    Remove the specification between brackets, if any, from a string.
    '''
    if ' (' in s and s.endswith(')'):
        s = s.split(' (')[0]
    return s


def uri_to_string(uri, spec=False):
    '''
    Transform a dbpedia resource uri into a string.
    '''
    uri = urllib.parse.unquote(uri)
    s = uri.split('/resource/')[-1]
    s = s.replace('_', ' ')

    if ' (' in s and s.endswith(')'):
        if spec:
            s = s.split(' (')[1][:-1]
        else:
            s = s.split(' (')[0]
    elif spec:
        return None
    s = ' '.join(s.split())

    return s


def get_wd_aliases(wd_uri):
    '''
    Get additional alternative names form Wikidata web service.
    '''
    wd_id = wd_uri.split('/')[-1]
    url = WD_URL.format(wd_id)
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
    except Exception as e:
        return []
    try:
        alias_dict = data.get('entities').get(wd_id).get('aliases').get('nl')
        return [a.get('value') for a in alias_dict]
    except Exception as e:
        return []


def ddd_jsru(preflabel):
    '''
    Count the number of times the label appears in the newspaper corpus.
    '''
    JSRU = 'http://jsru.kb.nl/sru/'
    JSRU += 'sru?x-collection=DDD_artikel&recordSchema=dcx&query='
    JSRU += 'cql.serverChoice exact "%s"&maximumRecords=0'

    jsru = requests.get(JSRU % preflabel)
    jsru_data = ET.fromstring(jsru.text)

    for item in jsru_data.iter():
        if item.tag.endswith('numberOfRecords'):
            return(item.text)

    return None


def transform(record, uri):
    '''
    Extract the relevant data and return a Solr document dict.
    '''
    document = {}

    # The main DBpedia URI as document id
    document['id'] = uri

    # Other language, Wikidata uris
    if uri.startswith('http://nl.'):
        document['uri_nl'] = uri
    else:
        document['uri_en'] = uri

    if PROP_SAME_AS in record:
        for u in record[PROP_SAME_AS]:
            if uri.startswith('http://nl.'):
                if u.startswith('http://dbpedia.org/resource/'):
                    document['uri_en'] = u
            if u.startswith('http://www.wikidata.org/entity/'):
                document['uri_wd'] = u

    # The first (i.e. Dutch if available) label
    document['label'] = record[PROP_LABEL][0]

    # Normalized pref label, based on the label without specification
    # between brackets
    pref_label = utilities.normalize(remove_spec(document['label']))
    document['pref_label'] = pref_label
    document['pref_label_str'] = pref_label

    # The first (i.e. Dutch if available) abstract
    try:
        document['abstract'] = record[PROP_ABSTRACT][0]
    except Exception as e:
        document['abstract'] = '.'

    bow = utilities.tokenize(doc['abstract'], max_sent=5)
    doc['abstract_norm'] = ' '.join(bow)
    doc['abstract_token'] = list(set([t for t in bow if len(t) > 5]))[:15]

    # Language of the (primary) resource description
    document['lang'] = 'nl' if uri.startswith('http://nl.') else 'en'

    # Number of links and inlinks (max of Dutch and English counts)
    if PROP_LINK in record:
        document['outlinks'] = len(record[PROP_LINK])

    document['inlinks'] = max(record['inlinks'])

    # Number of times label appears in newspaper index
    document['inlinks_newspapers'] = ddd_jsru(pref_label)

    # Set ambiguity flag if specification between brackets present in URI and
    # save the specification
    if '_(' in uri and uri.endswith(')'):
        document['ambig'] = 1
        document['spec'] = utilities.normalize(uri_to_string(uri, True))
    else:
        document['ambig'] = 0

    # Normalized alt labels extracted form various name fields as well as
    # redirects
    cand = record[PROP_LABEL][1:]
    cand += record[PROP_NAME]

    if PROP_REDIRECT in record:
        # Exclude English redirects if there are too many
        if len([u for u in record[PROP_REDIRECT] if
                u.startswith('http://dbpedia.org/resource/')]) > 100:
            cand += [uri_to_string(u) for u in record[PROP_REDIRECT] if
                     u.startswith('http://nl.dbpedia.org/resource/')]
        else:
            cand += [uri_to_string(u) for u in record[PROP_REDIRECT]]

    # Include disambiguations for acronyms
    if PROP_DISAMBIGUATES in record:
        for u in record[PROP_DISAMBIGUATES]:
            s = uri_to_string(u)
            if len(s) >= 2 and len(s) <= 5 and s.isupper():
                cand.append(s)

    # Include Wikidata aliases
    if document.get('uri_wd'):
        wd_cand = get_wd_aliases(document.get('uri_wd'))
        cand += wd_cand

        wd_alt_label = clean_labels(wd_cand, pref_label)
        document['wd_alt_label'] = wd_alt_label
        document['wd_alt_label_str'] = wd_alt_label

    alt_label = clean_labels(cand, pref_label)
    document['alt_label'] = alt_label
    document['alt_label_str'] = alt_label

    # Keywords extracted from Dutch DBpedia category links, e.g.
    # http://nl.dbpedia.org/resource/Categorie:Amerikaans_hoogleraar
    # should return ['amerikaans', 'hoogleraar']
    if PROP_LINK in record:
        keywords = []
        for link in record[PROP_LINK]:
            if link.startswith('http://nl.dbpedia.org/resource/Categorie:'):
                s = uri_to_string(link).split('Categorie:')[1]
                # Crude stop word filtering. Use list instead?
                keywords += [k for k in utilities.normalize(s).split() if
                             len(k) >= 5]
        keywords = list(set(keywords))
        for k in pref_label.split():
            if k in keywords:
                keywords.remove(k)
        document['keyword'] = keywords

    # DBpedia ontology and schema.org types
    if PROP_TYPE in record:
        document['dbo_type'] = list(set(
            [t.split('/')[-1] for t in record[PROP_TYPE] if
             t.startswith('http://dbpedia.org/ontology/')
             and t.find('Wikidata:') < 0 and t.find('%') < 0]))
        document['schema_type'] = list(set(
            [t.split('/')[-1] for t in record[PROP_TYPE] if
             t.startswith('http://schema.org/')]))

    # Predicted topics and types
    resp = requests.get(TOPICS_URL, params={'url': uri}, timeout=300)
    if resp.status_code != 200:
        raise Exception('Error retrieving topics')

    resp = resp.json()

    for t in resp['topics']:
        document['topic_{}'.format(t)] = resp['topics'][t]

    for t in resp['types']:
        document['dbo_type_{}'.format(t)] = resp['types'][t]

    # Probable last name, for persons only
    if (('dbo_type' in document and 'Person' in document['dbo_type']) or
        ('dbo_type' not in document and document['dbo_type_person'] >= 0.75)):
        last_part = utilities.get_last_part(pref_label,
                                            exclude_first_part=True)
        if last_part:
            document['last_part'] = last_part
            document['last_part_str'] = last_part

    # Birth and death dates, taking the minimum of multiple birth date options
    # and the maximum of multiple death dates
    # E.g. -013-10-07+01:00
    if PROP_BIRTH_DATE in record:
        cand = []
        for date in record[PROP_BIRTH_DATE]:
            try:
                cand.append(int(date[:4]))
            except Exception as e:
                continue
        if cand:
            document['birth_year'] = min(cand)

    if PROP_DEATH_DATE in record:
        cand = []
        for date in record[PROP_DEATH_DATE]:
            try:
                cand.append(int(date[:4]))
            except Exception as e:
                continue
        if cand:
            document['death_year'] = max(cand)

    # Birth and death places, giving preference to Dutch options
    nl_resource = 'http://nl.dbpedia.org/resource/'
    en_resource = 'http://dbpedia.org/resource/'

    if PROP_BIRTH_PLACE in record:
        places = [utilities.normalize(uri_to_string(p)) for p in
                  record[PROP_BIRTH_PLACE] if p.startswith(nl_resource)]
        if not places:
            places = [utilities.normalize(uri_to_string(p)) for p in
                      record[PROP_BIRTH_PLACE] if p.startswith(en_resource)]
        document['birth_place'] = list(set(places))

    if PROP_DEATH_PLACE in record:
        places = [utilities.normalize(uri_to_string(p)) for p in
                  record[PROP_DEATH_PLACE] if p.startswith(nl_resource)]
        if not places:
            places = [utilities.normalize(uri_to_string(p)) for p in
                      record[PROP_DEATH_PLACE] if p.startswith(en_resource)]
        document['death_place'] = list(set(places))

    # OCR tolerant labels
    if 'pref_label' in document:
        pref_label_ocr = utilities.normalize_ocr(document['pref_label'])
        document['pref_label_ocr'] = pref_label_ocr
        document['pref_label_str_ocr'] = pref_label_ocr

    if 'alt_label' in document:
        alt_label_ocr = [utilities.normalize_ocr(label) for label in
                         document['alt_label']]
        document['alt_label_ocr'] = alt_label_ocr
        document['alt_label_str_ocr'] = alt_label_ocr

    if 'last_part' in document:
        last_part_ocr = utilities.normalize_ocr(document['last_part'])
        document['last_part_ocr'] = last_part_ocr
        document['last_part_str_ocr'] = last_part_ocr

    return document


def clean_labels(cand, pref_label):

    alt_label = []

    # Exclude some unwanted candidates
    unwanted = ['/', '|']
    for s in unwanted:
        for c in cand[:]:
            if c and c.find(s) > -1:
                cand.remove(c)

    # Exclude identical alt labels and alt labels identical to the pref label
    alt_label = []
    for l in cand:
        l_norm = utilities.normalize(remove_spec(l))
        if l_norm and l_norm != pref_label:
            if l_norm not in alt_label:
                alt_label.append(l_norm)

    # Exclude alt labels that contain the same words as the pref label
    for l in alt_label[:]:
        if len(set(l.split()) & set(pref_label.split())) == len(l.split()):
            if len(pref_label.split()) == len(l.split()):
                alt_label.remove(l)

    return alt_label


def get_document(uri=None):
    '''
    Retrieve and process all info about specified uri.
    '''
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
    document = transform(record, uri)
    return document


if __name__ == '__main__':
    if len(sys.argv) > 1:
        result = get_document(sys.argv[1])
    else:
        result = get_document('http://nl.dbpedia.org/resource/Albert_Einstein')
    pprint.pprint(result)
