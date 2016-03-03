#!/usr/bin/env python
# Z. Mashologu (SANBI-UWC)
#import dict as dict
import os
import shutil
import optparse
import urllib2
import logging
log = logging.getLogger( __name__ )

from json import loads, dumps

def cleanup_before_exit( tmp_dir ):
    if tmp_dir and os.path.exists( tmp_dir ):
        shutil.rmtree( tmp_dir )

def _stream_fasta_to_file( fasta_stream, target_directory, params, close_stream=True ):
    fasta_base_filename = "%s.fa" % sequence_id
    fasta_filename = os.path.join( target_directory, fasta_base_filename )
    fasta_writer = open( fasta_filename, 'wb+' )

    if isinstance( fasta_stream, list ) and len( fasta_stream ) == 1:
        fasta_stream = fasta_stream[0]

    if isinstance( fasta_stream, list ):
        last_char = None
        for fh in fasta_stream:
            if last_char not in [ None, '\n', '\r' ]:
                fasta_writer.write( '\n' )
            while True:
                data = fh.read( CHUNK_SIZE )
                if data:
                    fasta_writer.write( data )
                    last_char = data[-1]
                else:
                    break
            if close_stream:
                fh.close()
    else:
        while True:
            data = fasta_stream.read( CHUNK_SIZE )
            if data:
                fasta_writer.write( data )
            else:
                break
        if close_stream:
            fasta_stream.close()

    fasta_writer.close()

    return dict( path=fasta_base_filename )

def download_from_url( data_manager_dict, params, target_directory, dbkey, sequence_id, sequence_name ):
    #TODO: we should automatically do decompression here
    urls = filter( bool, map( lambda x: x.strip(), params['param_dict']['reference_source']['user_url'].split( '\n' ) ) )
    fasta_reader = [ urllib2.urlopen( url ) for url in urls ]

    data_table_entry = _stream_fasta_to_file( fasta_reader, target_directory, params )
    _add_data_table_entry( data_manager_dict, data_table_entry )

def download_from_history( data_manager_dict, params, target_directory):
    #TODO: allow multiple FASTA input files
    input_filename = params['param_dict']['reference_source']['input_fasta']
    if isinstance( input_filename, list ):
        fasta_reader = [ open( filename, 'rb' ) for filename in input_filename ]
    else:
        fasta_reader = open( input_filename )

    data_table_entry = _stream_fasta_to_file( fasta_reader, target_directory, params )
    _add_data_table_entry( data_manager_dict, data_table_entry )

def copy_from_directory( data_manager_dict, params, target_directory ):
    input_filename = params['param_dict']['reference_source']['fasta_filename']
    create_symlink = params['param_dict']['reference_source']['create_symlink'] == 'create_symlink'
    if create_symlink:
        data_table_entry = _create_symlink( input_filename, target_directory )
    else:
        if isinstance( input_filename, list ):
            fasta_reader = [ open( filename, 'rb' ) for filename in input_filename ]
        else:
            fasta_reader = open( input_filename )
        data_table_entry = _stream_fasta_to_file( fasta_reader, target_directory, params )
    _add_data_table_entry( data_manager_dict, data_table_entry )

def _create_symlink( input_filename, target_directory ):
    fasta_base_filename = "%s.fa" % sequence_id
    fasta_filename = os.path.join( target_directory, fasta_base_filename )
    os.symlink( input_filename, fasta_filename )
    return dict( path=fasta_base_filename )

REFERENCE_SOURCE_TO_DOWNLOAD = dict( url=download_from_url, history=download_from_history, directory=copy_from_directory )

def main():
    #Parse Command Line
    parser = optparse.OptionParser()
    parser.add_option( '-d', '--data_table_name' )
    (options, args) = parser.parse_args()

    filename = args[0]

    params = loads( open( filename ).read() )
    target_directory = params[ 'output_data' ][0]['extra_files_path']
    os.mkdir( target_directory )
    data_manager_dict = {}

    #Fetch the FASTA
    REFERENCE_SOURCE_TO_DOWNLOAD[ params['param_dict']['reference_source']['reference_source_selector'] ]( data_manager_dict, params, target_directory )

    #save info to json file
    open( filename, 'wb' ).write( dumps( data_manager_dict ) )

if __name__ == "__main__": main()