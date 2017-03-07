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

import json
import requests
import time

SOLR_UPDATE_URL = 'http://sara-backup:8082/solr/dbpedia/update'

headers = {'Content-Type': 'application/json'}

def delete_list(new_f, old_f):
    '''
    Delete Solr document for each URI on the old list that is no longer present
    on the new list.
    '''
    with open(new_f, 'rb') as fh:
        new_list = fh.read().decode('utf-8').split()
        print(len(new_list))

    with open(old_f, 'rb') as fh:
        old_list = fh.read().decode('utf-8').split()
        print(len(old_list))

    diff = list(set(old_list) - set(new_list))
    print(len(diff))
    print(diff[:10])

    for i, uri in enumerate(diff):
        payload = json.dumps({'delete': uri}, ensure_ascii=False).encode('utf-8')
        #print(payload)
        response = requests.post(SOLR_UPDATE_URL, data=payload, headers=headers)
        #print(response.text)
        if i % 100 == 0:
            print('Processed ' + str(i) + ' of ' + str(len(diff)))
            print('Committing changes...')
            r = requests.get(SOLR_UPDATE_URL + '?commit=true')
            print(r.text)
            time.sleep(1)

    # Commit at end of file
    print('Committing changes...')
    r = requests.get(SOLR_UPDATE_URL + '?commit=true')
    print(r.text)

if __name__ == "__main__":
    delete_list('uris_nl.txt', 'uris_nl.txt.old')
    delete_list('uris_en.txt', 'uris_en.txt.old')

