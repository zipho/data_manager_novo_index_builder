#!/usr/bin/env python
# Z. Mashologu (SANBI-UWC)
# import dict as dict
from __future__ import print_function
import os
import sys
import urllib2
import logging
import argparse
import shlex
from subprocess import check_call, CalledProcessError

log = logging.getLogger(__name__)

from json import loads, dumps

def get_dbkey_id_name(params, dbkey_description=None):
    dbkey = params['param_dict']['dbkey']
    # TODO: ensure sequence_id is unique and does not already appear in location file
    sequence_id = params['param_dict']['sequence_id']
    if not sequence_id:
        sequence_id = dbkey  # uuid.uuid4() generate and use an uuid instead?

    sequence_name = params['param_dict']['sequence_name']
    if not sequence_name:
        sequence_name = dbkey_description
        if not sequence_name:
            sequence_name = dbkey
    return dbkey, sequence_id, sequence_name


def _make_novocraft_index(fasta_filename, target_directory):
    if os.path.exists(target_directory) and not os.path.isdir(target_directory):
        print("Output directory path already exists but is not a directory: {}".format(target_directory),
              file=sys.stderr)
    elif not os.path.exists(target_directory):
        os.mkdir(target_directory)

    if 'GALAXY_SLOTS' in os.environ:
        nslots = os.environ['GALAXY_SLOTS']
    else:
        nslots = 1

    #cmdline_str = 'STAR --runMode genomeGenerate --genomeDir {} --genomeFastaFiles {} --runThreadN {}'.format(
    #    target_directory,
    #    fasta_filename,
    #    nslots)
    #cmdline = shlex.split(cmdline_str)
    cmdline = ('touch', '{}/foo.nix'.format(target_directory))
    try:
        check_call(cmdline)
    except CalledProcessError:
        print("Error building RNA STAR index", file=sys.stderr)
    return (target_directory)


def download_from_url(params, target_directory):
    # TODO: we should automatically do decompression here
    urls = filter(bool, map(lambda x: x.strip(), params['param_dict']['reference_source']['user_url'].split('\n')))
    fasta_reader = [urllib2.urlopen(url) for url in urls]

    _make_novocraft_index(fasta_reader, target_directory)


def download_from_history( params, target_directory):
    # TODO: allow multiple FASTA input files
    input_filename = params['param_dict']['reference_source']['input_fasta']

    _make_novocraft_index(input_filename, target_directory)

REFERENCE_SOURCE_TO_DOWNLOAD = dict(url=download_from_url, history=download_from_history)

def main():
    parser = argparse.ArgumentParser(description="Generate Novo-align genome index and JSON describing this")
    parser.add_argument('output_filename')
    parser.add_argument('--dbkey_description')
    parser.add_argument('--data_table_name', default='novocraft_index')
    args = parser.parse_args()

    filename = args.output_filename

    params = loads(open(filename).read())
    target_directory = params['output_data'][0]['extra_files_path']
    os.makedirs(target_directory)

    dbkey, sequence_id, sequence_name = get_dbkey_id_name(params, dbkey_description=args.dbkey_description)
    if dbkey in [None, '', '?']:
        raise Exception('"%s" is not a valid dbkey. You must specify a valid dbkey.' % (dbkey))

    # Fetch the FASTA
    REFERENCE_SOURCE_TO_DOWNLOAD[params['param_dict']['reference_source']['reference_source_selector']]\
        (params, target_directory)

    data_table_entry = dict(value=sequence_id, dbkey=dbkey, name=sequence_name, path=target_directory)

    output_datatable_dict = dict(data_tables={args.data_table_name: [data_table_entry]})
    open(filename, 'wb').write(dumps(output_datatable_dict))

if __name__ == "__main__": main()
