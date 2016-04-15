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

DEFAULT_DATA_TABLE_NAME = "novocraft_index"

def get_dbkey_id_name(params, dbkey_description=None):
    # TODO: ensure sequence_id is unique and does not already appear in location file
    sequence_id = params['param_dict']['sequence_id']
    sequence_name = params['param_dict']['sequence_name']
    if not sequence_name:
        sequence_name = dbkey_description
    return sequence_id, sequence_name

def _make_novocraft_index(data_manager_dict, fasta_filename, target_directory, sequence_id, sequence_name, data_table_name=DEFAULT_DATA_TABLE_NAME):
    if os.path.exists(target_directory) and not os.path.isdir(target_directory):
        print("Output directory path already exists but is not a directory: {}".format(target_directory),
              file=sys.stderr)
    elif not os.path.exists(target_directory):
        os.mkdir(target_directory)

    nix_file = sequence_id + ".nix"
    index_filename = os.path.join(target_directory, nix_file)
    cmdline_str = 'novoindex {} {}'.format(index_filename, fasta_filename)
    cmdline = shlex.split(cmdline_str)

    try:
        check_call(cmdline)
    except CalledProcessError:
        print("Error building RNA STAR index", file=sys.stderr)

    data_table_entry = dict( value=sequence_id, dbkey=sequence_id, name=sequence_name, path=index_filename )
    _add_data_table_entry( data_manager_dict, data_table_name, data_table_entry )

def _add_data_table_entry( data_manager_dict, data_table_name, data_table_entry ):
    data_manager_dict['data_tables'] = data_manager_dict.get( 'data_tables', {} )
    data_manager_dict['data_tables'][ data_table_name ] = data_manager_dict['data_tables'].get( data_table_name, [] )
    data_manager_dict['data_tables'][ data_table_name ].append( data_table_entry )
    return data_manager_dict

def download_from_url( data_manager_dict, params, target_directory, sequence_id, sequence_name, data_table_name=DEFAULT_DATA_TABLE_NAME ):
    # TODO: we should automatically do decompression here
    urls = filter(bool, map(lambda x: x.strip(), params['param_dict']['reference_source']['user_url'].split('\n')))
    fasta_reader = [urllib2.urlopen(url) for url in urls]

    _make_novocraft_index(data_manager_dict, fasta_reader, target_directory, sequence_id, sequence_name, data_table_name)

def download_from_history( data_manager_dict, params, target_directory,  sequence_id, sequence_name, data_table_name=DEFAULT_DATA_TABLE_NAME ):
    # TODO: allow multiple FASTA input files
    input_filename = params['param_dict']['reference_source']['input_fasta']

    _make_novocraft_index(data_manager_dict, input_filename, target_directory, sequence_id, sequence_name, data_table_name )

REFERENCE_SOURCE_TO_DOWNLOAD = dict(url=download_from_url, history=download_from_history)

def main():
    parser = argparse.ArgumentParser(description="Generate Novo-craft genome index and JSON describing this")
    parser.add_argument('output_filename')
    parser.add_argument('--dbkey_description')
    parser.add_argument('--data_table_name', default='novocraft_index')
    args = parser.parse_args()

    filename = args.output_filename

    params = loads(open(filename).read())
    target_directory = params['output_data'][0]['extra_files_path']
    os.makedirs(target_directory)
    data_manager_dict = {}

    sequence_id, sequence_name = get_dbkey_id_name(params, dbkey_description=args.dbkey_description)

    # Fetch the FASTA
    REFERENCE_SOURCE_TO_DOWNLOAD[params['param_dict']['reference_source']['reference_source_selector']]\
        (data_manager_dict, params, target_directory, sequence_id, sequence_name, data_table_name=args.data_table_name or DEFAULT_DATA_TABLE_NAME )

    open(filename, 'wb').write(dumps( data_manager_dict ))

if __name__ == "__main__": main()
