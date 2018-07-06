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

from datetime import datetime

from unicodedata import normalize

from helper.io_module import check_file

from pbcore.io import SubreadSet

'''
Represents a PacBio SMRT cell. Provides easy methods for accessing sequencing and metadata.
'''
class SmrtCell(object):
    
    '''
    Initializes a new SmrtCell object from a smrtcell xml file
    @param xml_file: the path to a subreadset.xml file of a smrtcell
    '''
    def __init__(self,xml_file):
        self.__logger = logging.getLogger('support.smrtcell')
        self.__is_valid = False
        
        self.__xml_file = check_file(xml_file)
        if not self.__xml_file:
            self.show_log('error', 'XML file '+self.__xml_file+' does not exist or is not a file!')
            return
        
#TODO: read xml content from encrypted file        
        self.__subreadset = None
        try:
            self.__subreadset = SubreadSet(self.__xml_file)
        except IOError as err:
            self.show_log('error', 'Parsing of XML file '+self.__xml_file+' was not successful: '+err+'!')
            return
        
        self.__is_valid = True
        

    '''
    Tests if the SmrtCell object is valid.
    @return: return true if the SmrtCell object is valid otherwise false
    @rtype: bool
    '''    
    def is_valid(self):
        return self.__is_valid

    '''
    Returns the name of the SmrtCell object.
    @return: the name
    @rtype: str
    '''    
    def get_name(self):
        return self.__subreadset.name if self.__is_valid else None

    '''
    Returns the total number of reads in the SmrtCell object.
    @return: the number of reads
    @rtype: integer
    '''    
    def get_total_number_of_reads(self):
        return int(self.__subreadset.metadata.numRecords) if self.__is_valid else None

    '''
    Returns the total number of bp in the SmrtCell object.
    @return: the number of bp
    @rtype: integer
    '''    
    def get_total_sum_of_bp(self):
        return int(self.__subreadset.metadata.totalLength) if self.__is_valid else None

    '''
    Returns the number of collections ('sequencing runs') in the SmrtCell object.
    Should be 1 in almost all cases. If not, all other functions have an optional argument 
    to specify the collection.
    @return: the number of sequencing runs
    @rtype: integer
    '''    
    def get_number_of_collections(self):
        return len(self.__subreadset.metadata.collections) if self.__is_valid else None
 
    '''
    Checks if a provided collection index is valid, i.e. can access a collection.
    Do not confuse with collection index.
    @param collection_index: the index of the collection
    @return: true if collection index is valid otherwise false
    @rtype: bool
    '''    
    def check_collection_index(self,collection_index):
        return self.__is_valid and collection_index >= 0 and collection_index<len(self.__subreadset.metadata.collections)  

    '''
    Returns the names of the samples that were loaded onto this SmrtCell.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: a list with sample names
    @rtype: list of str
    '''    
    def get_biosample_names(self,collection_index=0):
        biosample_names = []
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            num_biosamples = len(self.__subreadset.metadata.collections[collection_index].wellSample.bioSamples)
            for i in range(0,num_biosamples):
                biosample_names.append(self.__subreadset.metadata.collections[collection_index].wellSample.bioSamples[i].name)
        return biosample_names

    '''
    Returns the cell index.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the cell index
    @rtype: integer
    '''    
    def get_cell_index(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            return int(self.__subreadset.metadata.collections[collection_index].cellIndex)
        else:
            return None

    '''
    Returns the collection number.
    Do not confuse with collection index.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the collection number
    @rtype: integer
    '''    
    def get_collection_number(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            return int(self.__subreadset.metadata.collections[collection_index].collectionNumber)
        else:
            return None
        
    '''
    Returns the raw data path.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the raw data path
    @rtype: str
    '''    
    def get_raw_data_path(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            return self.__subreadset.metadata.collections[collection_index].primary.outputOptions.collectionPathUri
        else:
            return None
    
    '''
    Returns the run id.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the run id
    @rtype: str
    '''    
    def get_run_id(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            return self.__subreadset.metadata.collections[collection_index].runDetails.timeStampedName
        else:
            return None

    '''
    Returns the run name.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the run name
    @rtype: str
    '''    
    def get_run_name(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            return self.__subreadset.metadata.collections[collection_index].runDetails.name
        else:
            return None

    '''
    Returns the cellpac barcode.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the cellpac barcode
    @rtype: str
    '''    
    def get_cellpac_barcode(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            return self.__subreadset.metadata.collections[collection_index].cellPac.barcode
        else:
            return None

    '''
    Returns the cellpac lot number.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the cellpac lot number
    @rtype: str
    '''    
    def get_cellpac_lot_number(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            return self.__subreadset.metadata.collections[collection_index].cellPac.lotNumber
        else:
            return None
 
    '''
    Returns the instrument code.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the instrument code
    @rtype: str
    '''    
    def get_instrument_code(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            return self.__subreadset.metadata.collections[collection_index].instrumentName
        else:
            return None
 
    '''
    Returns the sequencing date.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the sequencing date as string
    @rtype: str
    '''    
    def get_sequencing_date(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            val = self.__subreadset.metadata.collections[collection_index].createdAt
            reduced_date_string, suffix = val.split(".")
            suffix = suffix
            return datetime.strptime(reduced_date_string, "%Y-%m-%dT%H:%M:%S").strftime('%Y-%m-%d')
        else:
            return None
 
    '''
    Returns the well name.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the well name
    @rtype: str
    '''    
    def get_well_name(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            return self.__subreadset.metadata.collections[collection_index].wellSample.name
        else:
            return None

    '''
    Returns the concentration.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the concentration
    @rtype: float
    '''    
    def get_concentration(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            return float(self.__subreadset.metadata.collections[collection_index].wellSample.concentration)
        else:
            return None
    
    '''
    Returns the UseCount property.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the UseCount property
    @rtype: str
    '''    
    def get_usecount(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            return self.__subreadset.metadata.collections[collection_index].wellSample.useCount
        else:
            return None
    
    '''
    Returns the instrument control software version.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the instrument control software version
    @rtype: str
    '''    
    def get_instrument_control_software_version(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            return self.__subreadset.metadata.collections[collection_index].instCtrlVer
        else:
            return None

    '''
    Returns the signal processing software version.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the signal processing software version
    @rtype: str
    '''    
    def get_signal_processing_software_version(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            return self.__subreadset.metadata.collections[collection_index].sigProcVer
        else:
            return None

    '''
    Returns the notes (i.e. additional free text description).
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the notes
    @rtype: str
    '''    
    def get_notes(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            return self.__subreadset.metadata.collections[collection_index].wellSample.description
        else:
            return None
        
    '''
    Returns the automation parameters.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: a hash with key - value pairs
    @rtype: dict of str
    '''     
    def get_automation_parameters(self,collection_index=0):
        automation_parameters = {}
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            for i in range(0,len(self.__subreadset.metadata.collections[collection_index].automation.automationParameters)):
                hashed_data = self.__subreadset.metadata.collections[collection_index].automation.automationParameters[i].metadata
                automation_parameters[hashed_data['Name']] =  hashed_data['SimpleValue']
        return automation_parameters
    
    '''
    Returns the movie length in minutes.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the movie length in minutes
    @rtype: integer
    '''    
    def get_movie_length(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            automation_parameters = self.get_automation_parameters(collection_index)
            return int(automation_parameters['MovieLength'])
        else:
            return None
    
    '''
    Returns the immobilization time in minutes.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the immobilization time in minutes
    @rtype: integer
    '''    
    def get_immobilisation_time(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            automation_parameters = self.get_automation_parameters(collection_index)
            return int(automation_parameters['ImmobilizationTime'])
        else:
            return None
    '''
    Returns the insert size.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the insert size
    @rtype: integer
    '''    
    def get_insert_size(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            automation_parameters = self.get_automation_parameters(collection_index)
            return int(automation_parameters['InsertSize'])
        else:
            return None

    '''
    Returns true if hot start was enabled otherwise false.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: true if hot start was enabled false otherwise
    @rtype: bool
    '''    
    def get_stage_hotstart_enabled(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            return bool(self.__subreadset.metadata.collections[collection_index].wellSample.stageHotstartEnabled)
        else:
            return None
    
    '''
    Returns the primary protocol name
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the primary protocol name
    @rtype: str
    '''    
    def get_primary_protocol_name(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            return self.__subreadset.metadata.collections[collection_index].primary.automationName
        else:
            return None

    '''
    Returns the primary protocol config
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the primary protocol config
    @rtype: str
    '''    
    def get_primary_protocol_config(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            return self.__subreadset.metadata.collections[collection_index].primary.configFileName
        else:
            return None

    '''
    Returns the adapter sequences used in template prep
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: a dictionary with left adapter sequence and right adapter sequence
    @rtype: dict of str
    '''    
    def get_adapter_sequences(self,collection_index=0):
        adapter_sequences = {}
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            adapter_sequences['LeftAdapterSequence'] = self.__subreadset.metadata.collections[collection_index].templatePrepKit.leftAdaptorSequence
            adapter_sequences['RightAdapterSequence'] = self.__subreadset.metadata.collections[collection_index].templatePrepKit.rightAdaptorSequence
        return adapter_sequences

    '''
    Returns the sample name. Deprecated, use get_biosample_names instead.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the sample name
    @rtype: str
    '''    
    def get_sample_name(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            return self.__subreadset.metadata.collections[collection_index].wellSample.name
        else:
            return None   
        
    '''
    Returns the name of the Sequel binding kit.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the binding kit name
    @rtype: str
    '''    
    def get_binding_kit_name(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            name = self.__subreadset.metadata.collections[collection_index].bindingKit.name
            return normalize('NFKD',name).encode('ascii','ignore')
            return 
        else:
            return None
                
    '''
    Returns the name of the Sequel template prep kit.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the template prep kit name
    @rtype: str
    '''    
    def get_template_prep_kit_name(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            name = self.__subreadset.metadata.collections[collection_index].templatePrepKit.name
            return normalize('NFKD',name).encode('ascii','ignore')
            return 
        else:
            return None

    '''
    Returns the name of the Sequel sequencing plate kit.
    @param collection_index: the index of the collection (optional, zero-based, default: 0)
    @return: the sequencing plate kit name
    @rtype: str
    '''    
    def get_sequencing_plate_kit_name(self,collection_index=0):
        if self.__is_valid:
            assert self.check_collection_index(collection_index),'Specified collection index is invalid!'
            name = self.__subreadset.metadata.collections[collection_index].sequencingKitPlate.name
            return normalize('NFKD',name).encode('ascii','ignore')
            return 
        else:
            return None
                    
    '''
    Make all paths encoded in the SmrtCell object relative.
    @param outdir: a directory from which the paths should originate (optional, default: ".")
    '''    
    def make_paths_relative(self,outdir="."):
        if self.__is_valid:
            self.__subreadset.makePathsRelative(outdir)

    '''
    Make all paths encoded in the SmrtCell object absolute.
    '''    
    def make_paths_absolute(self):
        if self.__is_valid:
            self.__subreadset.makePathsAbsolute()
    
    '''
    Write new (subreadset) xml file for SmrtCell object.
    @param filename: a file name
    '''
    def write(self,filename):
        if self.__is_valid:
            self.__subreadset.write(filename)
        
    
    '''
    Helper function for log messages
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

