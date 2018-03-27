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
import sys

# Third-party library imports
import requests

# Import DAC modules
sys.path.insert(0, os.path.join(*[os.path.dirname(
    os.path.realpath(__file__)), '..', 'dac', 'dac']))
import utilities


SOLR_URL = 'http://linksolr1.kbresearch.nl/dbpedia/query?'


def get_document(uri):

    payload = {}
    payload['q'] = 'id:"{}"'.format(uri)
    payload['wt'] = 'json'

    resp = requests.get(SOLR_URL, params=payload, timeout=60).json()

    doc = resp['response']['docs'][0]

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

    doc = get_document(uri)
    pprint.pprint(doc)
