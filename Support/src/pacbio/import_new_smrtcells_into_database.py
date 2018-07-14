#!/usr/bin/env python3
'''
The MIT License (MIT)

Copyright (c) <2018> <DresdenConceptGenomeCenter>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

Use Python Naming Conventions
https://www.python.org/dev/peps/pep-0008/#naming-conventions

contact: andreas.petzold(at)tu-dresden.de
'''
''' python modules '''
import logging

from argparse import ArgumentParser as ArgumentParser
from argparse import RawDescriptionHelpFormatter

from time import time

from os import stat
from os.path import basename
from os.path import join as path_join

import json

from glob import glob

from re import match
from re import search

import mysql.connector

from helper.helper_logger import MainLogger
from helper.database import Database
from helper.io_module import check_directory
from helper.io_module import list_subdirectories
from helper.io_module import is_archived_directory

from helper.support_information import SupportInformation as SI

from pacbio.smrtcell import SmrtCell

class Parser(object):
    def __init__(self):
        self.__parser = ArgumentParser(description="""
        Imports new SmrtCell datasets into the database.
        """, formatter_class=RawDescriptionHelpFormatter, 
        add_help = False, prog = 'import_new_smrtcells_into_database.py')
        
        self.__base_path = '/projects/sequencing/pacbio1/'
        self.__max_days = 30
        self.__raw_data_dirs = []
        
        self.initialiseParser()

        self.__logger = logging.getLogger('support.import_new_smrtcells_into_database')
        self.parse()
        return
    
    '''
    Set up the command line parser.
    '''        
    def initialiseParser(self):
        self.__parser.add_argument('-h', '--help', dest = 'h', help = 'Display help message', action= 'store_true')
        self.__parser.add_argument('-b', '--base_path', dest = 'base_path', type = str, default=self.__base_path, help = 'Base path for searching for raw data directories (default: %(default).')
        self.__parser.add_argument('-d', '--max-days', dest='max_days', metavar='DAYS', type = str,default = self.__max_days, help = 'Skip raw data directories older than --max-days days (default: %(default)).')
        self.__parser.add_argument('-r', '--raw-data-dir', dest='raw_data_dirs', metavar='DIRECTORY',nargs='+', type = int, help = 'Manually provide raw data paths (can be used multiple times).')

    '''
    Start parsing.
    @param inputstring: input string to parse arguments from (optional, default: from args)
    ''' 
    def parse(self, inputstring = None):
        if inputstring == None:
            self.__options = self.__parser.parse_args()
        else:
            self.__options = self.__parser.parse_args(inputstring)
    
    '''
    Returns raw data directories that were set manually.
    @return: a list with directories
    @rtype: list of str
    ''' 
    def get_raw_data_dirs(self):
        return self.__raw_data_dirs
          
    '''
    Helper function for log messages.
    @param level: the log level (debug, info, warning, error, critical)
    @param message: the log message
    '''
    def show_log(self, level, message):
        if level == 'debug':
            self.__logger.debug(message)
        elif level == 'info':
            self.__logger.info(message)
        elif level == 'warning':
            self.__logger.warning(message)
        elif level == 'error':
            self.__logger.error(message)
        elif level == 'critical':
            self.__logger.critical(message)
    
    '''
    Does all input checks.
    '''
    def checks(self):
        # is base_path a valid path?
        base_path = self.__options.base_path
        canonical_dir = check_directory(base_path)
        if not canonical_dir:
            self.show_log('error', 'Path "'+dir+'" is no directory or does not exist!')
            exit(2)
        
        # is max_days reasonable?   
        max_days = self.__options.max_days
        if max_days<0:
            self.show_log('error', 'Invalid value given for --max-days: '+str(max_days)+'!')
            exit(2)
        self.__max_days = max_days
        
        # do specified raw_data_dirs exist?
        if self.__options.raw_data_dirs:
            raw_data_dirs = self.__options.raw_data_dirs
            for d in raw_data_dirs:
                canonical_dir = check_directory(d)
                if not canonical_dir:
                    self.show_log('error', 'Path "'+dir+'" is no directory or does not exist!')
                    exit(2)
                self.__raw_data_dirs.append(canonical_dir)
    
    '''
    If raw data directories are not provided by the user, look for directories and subdirectories
    starting at the base_path and skipping all older than max_days days. Ignore
    archived directories and .* directories.
    '''
    def look_automatically_for_raw_data_dirs(self):
        now = time()
        
        for d in list_subdirectories(self.__base_path):
            d_name = basename(d)
            if d_name =="." or d_name ==".." or search(r'tmp',d_name) or search(r'^\.',d_name) or search('r\s+',d_name): continue # special directory names we do not want
            
            for sd in list_subdirectories(d):
                sd_name = basename(sd)
                if sd_name == "." or sd_name == ".." or search(r'tmp',sd_name) or search(r'^\.',sd_name) or search('r\s+',sd_name): continue # special directory names we do not want
                if is_archived_directory(sd): continue # ignore everything archived
                if stat(sd).st_mtime < now - (self.__max_days * 86400): continue # skip all subdirectories older than max_days days
                
                # this indicates that the transfer is done
                transfer_done = glob(path_join(sd,'*.transferdone'))
                if len(transfer_done)>0:
                    self.__raw_data_dirs.append(sd)
                    
    '''
    Main method. Does checks and populates raw_data_dir if needed
    '''    
    def main(self):
        self.checks()
        self.look_automatically_for_raw_data_dirs()
        
        if len(self.get_raw_data_dirs()) == 0:
            self.show_log('info', 'No valid raw data directories found.')


class SmrtCellImporter(object):
    def __init__(self,raw_data_dir):
        self.__logger = logging.getLogger('support.import_new_smrtcells_into_database')
        self.__raw_data_dir = raw_data_dir
        self.__subreadset = None
        self.__is_valid = False
        self.__xml_file = None
                
        # grab the *.metadata.xml or *.subreadset.xml file in the raw data directory
        xml_files = self.grab_subread_xml()
        if len(xml_files) == 0:
            self.show_log('info', 'Raw data directory "'+self.get_raw_data_dir()+'" does not contain a *.subreadset.xml or *.metadata.xml file and will be skipped!')
            return
        elif len(xml_files) > 1:
            self.show_log('warn', 'Raw data directory "'+self.get_raw_data_dir()+'" contains multiple *.subreadset.xml or *.metadata.xml files and will be skipped!')
            return
        self.__xml_file = xml_files[0]
            
        
        # load/parse the xml file
        self.__subreadset = SmrtCell(self.get_xml_file())
        if not self.get_smrtcell().is_valid():
            self.show_log('error', 'The xml file "'+self.get_xml_file()+'" in the raw data directory "'++'" could not be parsed!')
            exit(2)
            
        # if all valid
        self.__is_valid =  self.get_smrtcell().is_valid()
        self.show_log('info','Found valid XML "'+basename(self.get_xml_file())+'" in raw data directory "'+self.get_raw_data_dir()+'".')
        
        # now ready to do work
        return
    
    '''
    Inserts the SmrtCell object as a new flowcell into the database.
    @param: a database object
    @return: a flowcell id
    @rtype: integer
    ''' 
    def insert_flowcell_in_db(self, db):
        # skip if it is not valid
        if not self.__is_valid:
            return None
        
        # insert flowcell into database       
        smrtcell = self.get_smrtcell()
        
        # additional information is stored in a dict and then converted into json
        additional_information = {}
        additional_information['RUN_NAME'] = smrtcell.get_run_name()
        additional_information['RUN_ID'] = smrtcell.get_run_id()
        additional_information['TEMPLATE_PREP_KIT_NAME'] = smrtcell.get_template_prep_kit_name()
        additional_information['BINDING_KIT_NAME'] = smrtcell.get_binding_kit_name()
        additional_information['SEQUENCING_PLATE_KIT_NAME'] = smrtcell.get_sequencing_plate_kit_name()
        additional_information['MOVIE_LENGTH'] = smrtcell.get_movie_length()
        additional_information['INSERT_SIZE'] = smrtcell.get_insert_size()
        additional_information['IMMOBILIZATION_TIME'] = smrtcell.get_immobilisation_time()
        additional_information['STAGE_HOTSTART_ENABLED'] = smrtcell.get_stage_hotstart_enabled()
        additional_information['SEQUENCING_MACHINE'] = smrtcell.get_instrument_code()
        additional_information['SEQUENCING_DATE'] = smrtcell.get_sequencing_date()
        additional_information['INST_CTRL_VER'] = smrtcell.get_instrument_control_software_version()
        additional_information['SIG_PROC_VER'] = smrtcell.get_signal_processing_software_version()
        additional_information['PRIMARY_PROTOCOL_NAME'] = smrtcell.get_primary_protocol_name()
        additional_information['PRIMARY_PROTOCOL_CONFIG'] = smrtcell.get_primary_protocol_config()
        additional_information['ADAPTER_SEQUENCES'] = smrtcell.get_adapter_sequences()
        additional_information['SAMPLE_NAME'] = smrtcell.get_sample_name()
        additional_information_json = json.dumps(additional_information)
        
        try:
            flowcell_id = db.insert_flowcell(
                                      code = smrtcell.get_cellpac_barcode(),
                                      flowcell_type = "PacBio Sequel SMRT Cell",
                                      production_date = smrtcell.get_sequencing_date(),
                                      sequencing_date = smrtcell.get_sequencing_date(),
                                      machine_code = smrtcell.get_instrument_code(),
                                      is_barcoded = len(smrtcell.get_biosample_names())>1,
                                      notes = smrtcell.get_notes(),
                                      number = smrtcell.get_instrument_code()+'_'+smrtcell.get_sequencing_date()+'_'+str(self.__subreadset.get_collection_number()),
                                      archiving_date = None,
                                      archiving_status = 0,
                                      compartment_count = 1,
                                      compartment_capacity = 0.45,
                                      activity = 1,
                                      raw_data_path = smrtcell.get_raw_data_path(),
                                      pipelining_status = 'open',
                                      flowcell_status = 'finished',
                                      additional_information = additional_information_json)
        except mysql.connector.Error as err:
            self.show_log('error', 'Inserting a new flowcell into the database failed: "'+format(err)+'"! Will roll back recent changes!')
            db.rollbackConnection()
            db.closeConnection()
            exit(2)
            
        self.show_log('info','Inserted new flowcell with id "'+str(flowcell_id)+'".')
                    
        # attach product to flowcell
        movie_length = smrtcell.get_movie_length()
        if movie_length % 60 > 0:
            movie_length = round(movie_length/60,1)
        else:
            movie_length = movie_length/60
        product_name = smrtcell.get_binding_kit_name()+", movie: "+str(movie_length)+" h"
        
        try:
            db.attach_product_to_flowcell_by_name(
                                                   flowcell_id = flowcell_id,
                                                   product_name = product_name)
        except mysql.connector.Error as err:
            self.show_log('error', 'Attaching the product "'+product_name+'" to the flowcell "'+str(flowcell_id)+'" failed: "'+format(err)+'"! Most likely, the product still needs to be added to the database. Will roll back recent changes!')
            db.rollbackConnection()
            db.closeConnection()
            exit(2)       
        
        self.show_log('info','Attached product "'+product_name+'" to flowcell with id "'+str(flowcell_id)+'".')
                
        # return flowcell_id
        return flowcell_id
    
    '''
    Inserts the biosamples of the SmrtCell object into the database. Biosample names 
    must be library ids (L1234 etc).
    @param: a database object
    @param: a flowcell id
    @return: a list with track ids
    @rtype: list of integer
    '''             
    def insert_tracks_in_db(self, db, flowcell_id):  
        # skip if it is not valid
        if not self.__is_valid:
            return None
        
        track_ids = []
              
        # skip if it is not valid
        if not self.__is_valid:
            return []
           
        # get biosample names
        smrtcell = self.get_smrtcell()
        biosample_names = smrtcell.get_biosample_names()
        if len(biosample_names)==0:
            self.show_log('error', 'The subreadset xml file does not contain biosample names and is most likely not valid! Will roll back recent changes!')
            db.rollbackConnection()
            db.closeConnection()
            exit(2)
        
        # now go through names, validate and insert tracks
        for name in biosample_names:
            lib_pattern_match = match(r'^L(\d+)$',name)
            if not lib_pattern_match:
                self.show_log('error', 'The biosample name "'+name+'" found in the subreadset xml file does not match the standard library pattern (e.g. L1234)! Will roll back recent changes!')
                db.rollbackConnection()
                db.closeConnection()
                exit(2)
            
            libid = lib_pattern_match.group(1)
            resultset = db.query_libraries(where_string = 'Libraries.ID = %s',where_values = [libid])
            if not resultset:
                self.show_log('error', 'The library "'+name+'" found in the subreadset xml file cannot be found in the database! Will roll back recent changes!')
                db.rollbackConnection()
                db.closeConnection()
                exit(2)
            library = resultset[0]
            
            try:
                track_id = db.insert_track(
                            flowcell_id = flowcell_id,
                            compartment = 1,
                            library_id = library['Libraries.ID'],
                            status = 'finished',
                            control = 'N',
                            molarity = 0,
                            recipe = 'SE',
                            operator = 'pacbio',
                            notes = smrtcell.get_notes(),
                            compartment_consumption = 1,
                            accounting_status = 'open',
                            accounting_date = None,
                            activity = 1,
                            client_access = library['Libraries.CLIENT_ACCESS'],
                            price_user_group = library['PriceUserGroups.NAME'],
                            price_discount_level = library['PriceDiscountLevels.NAME'],
                            price_product_table = library['PriceProductTables.NAME'])
            except mysql.connector.Error as err:
                self.show_log('error', 'Inserting a new track into database for library "'+str(library['Libraries.ID'])+'",failed: "'+format(err)+'". Will roll back recent changes!')
                db.rollbackConnection()
                db.closeConnection()
                exit(2)
            track_ids.append(track_id)
            
            self.show_log('info','Inserted new track with id "'+str(track_id)+'" for sample "'+name+'".')
        
        return track_ids 
 

    '''
    Returns the associated SmrtCell object.
    @return: a SmrtCell object
    @rtype: SmrtCell
    ''' 
    def get_smrtcell(self):
        return self.__subreadset
        
    '''
    Tests if the SmrtCellImporter object is valid, i.e. can be inserted into the database.
    @return: true if the SmrtCell object is valid otherwise false
    @rtype: bool
    ''' 
    def is_valid(self):
        return self.__is_valid

    '''
    Returns the raw data directory.
    @return: the raw data directory
    @rtype: str
    ''' 
    def get_raw_data_dir(self):
        return self.__raw_data_dir

    '''
    Returns the xml file of the raw data directory.
    @return: the xml file
    @rtype: str
    ''' 
    def get_xml_file(self):
        return self.__xml_file

    '''
    Grabs all *.metadata.xml or *.subreadset.xml files in the raw data directory.
    @return: a list with xml file paths
    @rtype: list of str
    ''' 
    def grab_subread_xml(self):
        files_grabbed = []
        for files in ('*.metadata.xml','*.subreadset.xml'):
            files_grabbed.extend(glob(path_join(self.__raw_data_dir,files)))
        return files_grabbed

    '''
    Helper function for log messages.
    @param level: the log level (debug, info, warning, error, critical)
    @param message: the log message
    '''
    def show_log(self, level, message):
        if level == 'debug':
            self.__logger.debug(message)
        elif level == 'info':
            self.__logger.info(message)
        elif level == 'warning':
            self.__logger.warning(message)
        elif level == 'error':
            self.__logger.error(message)
        elif level == 'critical':
            self.__logger.critical(message)
    


if __name__ == '__main__':   
    # set up logger and database connection
    mainlog = MainLogger('support')
    dbinst = Database(SI.DB_HOST, SI.DB_USER, SI.DB_PW, SI.DB)
    dbinst.setConnection()

    # parse command line arguments
    parser = Parser()
    parser.main()    
    
    # iterate through raw data directories provided by parser
    for d in parser.get_raw_data_dirs():
        # is already in database?
        flowcells = dbinst.query_flowcell_with_raw_data_path(d)
        if len(flowcells) > 0:  continue
        
        # parse subreadset xml
        smrtcell_importer = SmrtCellImporter(d)
        if not smrtcell_importer.is_valid(): continue
        
        # insert flowcell and tracks
        flowcell_id = smrtcell_importer.insert_flowcell_in_db(dbinst)
        track_ids = smrtcell_importer.insert_tracks_in_db(dbinst,flowcell_id)
        
        # now add job what to do next
        # -MD5SUM
        # -tar and gz
        # -encryp?
                
        # commit here so that if something fails the complete flowcell is removed
        dbinst.commitConnection()
      
    dbinst.closeConnection()
    mainlog.close()
