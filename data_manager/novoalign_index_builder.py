#!/usr/bin/env python
#Z Mashologu (SANBI-UWC)

import sys
import os
import tempfile
import shutil
import optparse
import urllib2
#import uuid
from ftplib import FTP
import tarfile
import zipfile
import gzip
import bz2

from json import loads, dumps

def main():
    #Parse Command Line
    parser = optparse.OptionParser()
    parser.add_option( '-d', '--dbkey_description', dest='dbkey_description', action='store', type="string", default=None, help='dbkey_description' )
    (options, args) = parser.parse_args()

    filename = args[0]

    params = loads( open( filename ).read() )
    target_directory = params[ 'output_data' ][0]['extra_files_path']
    os.mkdir( target_directory )
    data_manager_dict = {}

    dbkey, sequence_id, sequence_name = get_dbkey_id_name( params, dbkey_description=options.dbkey_description )

    if dbkey in [ None, '', '?' ]:
        raise Exception( '"%s" is not a valid dbkey. You must specify a valid dbkey.' % ( dbkey ) )

    #Fetch the FASTA
    REFERENCE_SOURCE_TO_DOWNLOAD[ params['param_dict']['reference_source']['reference_source_selector'] ]( data_manager_dict, params, target_directory, dbkey, sequence_id, sequence_name )

    #save info to json file
    open( filename, 'wb' ).write( dumps( data_manager_dict ) )

if __name__ == "__main__": main()
