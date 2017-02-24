# DBpedia indexer

Collection of Python scripts to build a Solr index from a subset of the Dutch and English DBpedia dumps for use with our [entity linker] (https://github.com/jlonij/disambiguation). The dumps were taken from the [2016-04 Downloads] (http://wiki.dbpedia.org/downloads-2016-04) dataset and loaded into a local Virtuoso triple store.

## Usage

Building the Solr index from the Virtuoso graph is a two-step process:

1. Generating lists of the Dutch and English resource URIs that are to be indexed: `./get_uris.py`

2. Retreiving the data for the URIs on the lists and sending it to Solr: `./index.py`, where the extraction of the relevant fields from the Virtuoso response(s) for each resource takes place in `record.py`.

