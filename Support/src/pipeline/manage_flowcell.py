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

contact: mathias.lesche(at)tu-dresden.de
'''
from networkx.classes import function

''' python modules ''' 
import logging

from argparse import ArgumentParser as ArgumentParser
from argparse import RawDescriptionHelpFormatter

from os import listdir

from os.path import join as pathjoin

from sys import argv

''' own modules '''
from helper.helper_logger import MainLogger

from helper.database import Database

from helper.io_module import create_directory
from helper.io_module import write_list
from helper.io_module import read_file_get_list

from helper.support_information import SupportInformation as SI

from sequencing.machine import Machine 
from sequencing.flowcell import IlluminaFlowcell


class Parser(object):
    def __init__(self):
        self.__parser = ArgumentParser(description="""
        Script which prepares files and folders for the Demultiplex operation
        on the CMCB and ZIH site.

        """, formatter_class=RawDescriptionHelpFormatter)
        self.initialiseParser()
        self.__prepare = False
        self.__finish = False
        self.__whattodo = ''

   
        self.__logger = logging.getLogger('support.manage_flowcell')
        self.parse()
 
    def initialiseParser(self):
        self.__parser.add_argument('-m', '--mode', type=str, metavar='STRING', dest='whattodo', default='p', choices=('p'), help='(p)repare demultiplexing (default: prepare)')
        self.__parser.add_argument('-f', '--from', metavar='STRING', dest='fromhere', default='', type = self.test_location, help='where is the raw data cmcb or zih')
        self.__parser.add_argument('-t', '--to', metavar='STRING', dest='to', default='', type = self.test_location, help='where is the demultiplex process cmcb or zih')

    def parse(self, inputstring = None):
        if inputstring == None:
            self.__options = self.__parser.parse_args()
        else:
            self.__options = self.__parser.parse_args(inputstring)

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

    def test_location(self, location):
        if location not in ('', 'cmcb', 'zih'):
            self.show_log('error', "If -m p is selected, one can only choose 'cmcb' or 'zih' for the -f and -t option.")
            exit(2)
  
    def main(self):
        if len(argv) == 1:
            self.__parser.print_help()
            exit(2)
        
        self.__whattodo = self.__options.whattodo
        self.__from = self.__options.fromhere
        self.__to = self.__options.to
        
        if self.__whattodo == 'p':
            self.__prepare = True
            if self.__from not in SI.STORAGESITE:
                self.show_log('error', "For -m option, choose either 'cmcb' or 'zih' for -f/--from")
                exit(2)
            if self.__to not in SI.STORAGESITE:
                self.show_log('error', "For -m option, choose either 'cmcb' or 'zih' for -t/--to")
                exit(2)
     

    def get_from(self):
        return self.__from

    def get_to(self):
        return self.__to
    
    def get_prepare(self):
        return self.__prepare

    fromhere = property(get_from)
    to = property(get_to)
    prepare = property(get_prepare)



class ManageFlowcell(object):
    def __init__(self, dbinst, origin = '', sendto = ''):
        self.__statuslist = [1, 2, 3] # 1 .. fresh, 2 .. on sequencer, 3 .. finished
        self.__statusdict = {1: 'fresh', 2: 'on sequencer', 3: 'sequencing finished', 'fresh': 1, 'on sequencer': 2, 'sequencing finished': 3}
        self.__pipestatuslist = ['open', 'done']
        self.__pipestatusdict = {'open': 'pipelining open', 'done': 'pipelining done', 'pipelining open': 'open', 'pipelining done': 'done'}
        self.__dbinst = dbinst
    
        self.__flowcelllist = []
        self.__s = '  '
    
        self.__origin = origin
        self.__sendto = sendto
    
        self.__logger = logging.getLogger('support.manage_flowcell')

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

    def prepare_machine_inst(self, medict):
        if medict['PLATFORM_ID'] == 1:
            platform = 'illumina'
        elif medict['PLATFORM_ID'] == 3:
            platform = 'pacbio'
        
        return Machine(medict['NAME'], medict['CODE'], medict['DEFAULT_STORAGE'], "/scratch/ngs_cmcb/sequencing/illumina/{0}".format(medict['NAME']), platform, medict['ID'])

    '''
    check if the storage site exists and return a dictionary with pathes for this storage site.
    Raise AssertionError, with the name of function raising it, if it doesn't.
    function returns a dictionary with paths.
    @param location: string
    @param functionname: string
    @return: dictionary
    '''
    def get_storage_dict(self, location, functionname):        
        if location in SI.STORAGEDICT.keys():
            return SI.STORAGEDICT[location]
        else:
            raise AssertionError("pipeline status: '{0}' is a wrong location for '{1}'".format(location, functionname))
    
    '''
    check if the storage site exists and raise AssertionError, with the name of function raising it, if it doesn't
    @param location: string
    @param functionname: string
    '''
    def check_storagesite(self, location, functionname):
        if location not in SI.STORAGESITE:
            raise AssertionError("pipeline status: '{0}' is a wrong location for '{1}'".format(location, functionname))
        
        
    '''
    small function designed to find flowcells having a certain sequencing status, pipestatus and an
    existing folder on a storage device. if this is true, flowcell is added to a list.
    @param status: integer
    @param pipestatus: string
    @param fcloc: location of the rawdata
    '''
    def find_flowcell_add_list(self, seqstatus, pipestatus, fcloc):
        self.check_storagesite(fcloc, '{0}.{1}'.format(self.__class__.__name__, self.find_flowcell_add_list.__name__))
        
        entries = self.__dbinst.query_flowcell_with_status(seqstatus, pipestatus)
        
        if len(entries) == 0:
            self.show_log('info','pipeline status: No flowcells are in ({0}, {1}) mode. Nothing to do!'.format(self.__statusdict[seqstatus], self.__pipestatusdict[pipestatus]))
            return
        
        for fcdict in entries:
            minst = self.prepare_machine_inst(self.__dbinst.query_machines_with_machineid(fcdict['MACHINE_ID']))
            
            if minst.platform == 'illumina':
                allfcs = listdir(minst.get_rawstorage_path(fcloc)) # retrieve all flowcells directories belonging to this flowcell
                flowcell = [i for i in allfcs if fcdict['CODE'] in i and 'archived' not in i]
                if len(flowcell) == 0:
                    self.show_log('error','Cannot find path. Check if path or flowcell code is correct for {0} and machine {1}'.format(fcdict['CODE'], minst.name))
                    continue
                
                fcinst = IlluminaFlowcell(minst, fcdict['CODE'], flowcell[0], fcdict['ID'])
                fcinst.pipestatus, fcinst.seqstatus = pipestatus, seqstatus
                self.show_log('info', "pipeline status: '{0}' from machine '{1}' with status ({2}, {3}) is added to list".format(fcdict['CODE'], minst.name, self.__statusdict[seqstatus], self.__pipestatusdict[pipestatus]))
                self.__flowcelllist.append(fcinst)
            else:
                pass

    '''
    small function that prepare flowcells for pipelining. The criteria of the selected flowcell
    is the sequencing status and pipeline status. Then there is an individual set up for illumina
    or pacbio. these are submitted to the database and the status in the database will be changed.
    fcloc describes if it is zih or cmcb and trackstatus tracks with these ids are used
    @param fcinst: flowcell instance
    @param seqstatus: integer
    @param pipestatus: string
    @param fcloc: string
    @param trackstatus: tuple
    @return: flowcell instance
    '''
    def prepare_flowcell_pipelining(self, fcinst, seqstatus = 2, pipestatus = 'open', trackstatus = (1,2), fcloc = 'cmcb',):
        self.check_storagesite(fcloc, '{0}.{1}'.format(self.__class__.__name__, self.find_flowcell_add_list.__name__))
        if fcinst.seqstatus == seqstatus and fcinst.pipestatus == pipestatus:
            if fcinst.machine.platform == 'illumina':
                if not fcinst.is_RTAcomplete(fcloc):
                    self.show_log('info', "pipeline status: '{0}' from machine '{1}' is ready for pipelining".format(fcinst.code, fcinst.machine.name))
                    return fcinst, 'running'
                

                fcinst.parse_runinfo_file(fcloc)
                fcinst.issingle = True if len(fcinst.indexlist) == 1 else False
                fcinst.build_lanedict(self.__dbinst, trackstatus)
                fcinst.loopLanes_buildBC_buildBasesMask()
                fcinst.prepare_samplesheet(fcloc)
                fcinst.seqstatus = 3
#                 self.__dbinst.update_path_number_into_flowcells(fcinst.dbid, fcinst.cmcbpath, fcinst.zihpath, fcinst.number)
#                 self.__dbinst.commitConnection()
            elif fcinst.machine.platform == 'pacbio':
                self.show_log('info', "pipeline status: '{0}' from machine '{1}' is ready for pipelining".format(fcinst.code, fcinst.machine.name))
                return fcinst, 'running'

            self.show_log('info', "flowcell status: '{0}' from machine '{1}' is ready for pipelining".format(fcinst.code, fcinst.machine.name))
        return fcinst, 'pipeline'
    
    '''
    function write the samplesheet to the raw data directory on either cmcb or zih site. it returns a False
    if the location is wrong or there are now samplesheets. Otherwise true is returned.
    @param fcinst: flowcell instance
    @param fclos:  string
    @return: boolean
    ''' 
    def write_samplessheet(self, fcinst, fcloc = 'cmcb'):
        self.check_storagesite(fcloc, '{0}.{1}'.format(self.__class__.__name__, self.write_samplessheet.__name__))
        
        if len(fcinst.samplesheetdict) == 0:
            self.show_log('warning', '{0}.{1} flowcell {2} has no samplesheet'.format(self.__class__.__name__, self.write_samplessheet.__name__, fcinst.code))
            return False

        samplesheetdir = pathjoin(fcinst.get_pathdict_with_location(fcloc)['machinepath'], 'Samplesheet')
#         create_directory(samplesheetdir)        
        for sname, ssheetlist in fcinst.samplesheetdict.items():
            sname = '{0}.csv'.format(sname)
            fullname = pathjoin(samplesheetdir, sname)
#             write_list(ssheetlist, fullname)
            self.show_log('info', "pipeline status: Samplesheet '{0}' for '{1}' has been written".format(sname, fcinst.code))
        return True

    '''
    function reads in the standard snakemake yml config and already
    modifies it. Pipeline path, FQ storage path and bcl version are added.
    This is taken from the Information class
    @param bclversion: string
    @param fromhere: dictionary of paths
    @param whereto: dictionary of paths
    @param machinecode: string
    @return: list
    '''
    def create_snakeconfig_basic(self, bclversion, fromhere, whereto, machinecode = ''):
        snakelist = read_file_get_list(pathjoin(fromhere['FILEFOLDER'], SI.SNAKE_BCL_YML_FILE))
        snakelist[0] = snakelist[0].replace('PIPESTORE', whereto['PIPESTORAGEPATH'])
        snakelist[1] = snakelist[1].replace('FQSTORAGE', pathjoin(whereto['FQSTORAGE'], machinecode))        
        snakelist[2] = snakelist[2].replace('BCLVERSION', bclversion)
        return snakelist

    
    '''
    function builds the snakemake file and saves it to a folder 'Snakefile' in the raw sequencing folder
    of the flowcell.
    '''
    
    def build_write_snakemakeconfig(self, fcinst, bclversion, fromhere, whereto):
        wheretodict = self.get_storage_dict(whereto, '{0}.{1}'.format(self.__class__.__name__, self.build_write_snakemakeconfig.__name__))
        fromheredict = self.get_storage_dict(fromhere, '{0}.{1}'.format(self.__class__.__name__, self.build_write_snakemakeconfig.__name__))

        if whereto == 'zih':
            snakelist = self.create_snakeconfig_basic(bclversion, fromheredict, wheretodict, fcinst.machine.code)
        else:
            snakelist = self.create_snakeconfig_basic(bclversion, fromheredict, wheretodict)

        snakelist.append('{0}{1}:'.format(self.__s, fcinst.name))        
        snakelist.append('{0}samplesheethome: {1}'.format(2*self.__s, pathjoin(fcinst.get_pathdict_with_location(whereto)['machinepath'], 'Samplesheet')))
        snakelist.append('{0}rawfolder: {1}'.format(2*self.__s, fcinst.get_pathdict_with_location(whereto)['machinepath']))

        if len(fcinst.readlist) == 1: snakelist.append('{0}single: 1'.format(2*self.__s))
        else: snakelist.append('{0}single: 0'.format(2*self.__s))
        
        snakelist.append('{0}csvrun:'.format(2*self.__s))

        for ssheetname, ssheetlist in fcinst.samplesheetdict.items():
#             add the settings for the above sample sheet to the snakemake config file
            snakelist.append('{0}{1}:'.format(3*self.__s, ssheetname))
            snakelist.append('{0}mismatches: {1}'.format(4*self.__s, ','.join(['1']*len(fcinst.indexlist))))
            snakelist.append('{0}basesmask: {1}'.format(4*self.__s, ssheetlist[1]))
        
        snakedir = pathjoin(fcinst.get_pathdict_with_location(fromhere)['machinepath'], 'Snakemake')
#         create_directory(samplesheetdir)     
        snakefile = pathjoin(snakedir, SI.SNAKE_BCL_YML_FILE)
#         write_list(snakelist, snakefile)
        self.show_log('info', "pipeline status: Snakefile for '{0}' has been written".format(fcinst.code))
        return True

#     def prepare(self):
    
           
    def set_flowcelllist_with_index(self, fcinstance, index):
        self.__flowcelllist[index] = fcinstance     
    
    def get_flowcelllist(self):
        return self.__flowcelllist

    flowcelllist = property(get_flowcelllist)

if __name__ == '__main__':
    mainlog = MainLogger('support')
    parseinst = Parser()
    parseinst.main()
    
    dbinst = Database(SI.DB_HOST, SI.DB_USER, SI.DB_PW, SI.DB)
    dbinst.setConnection()
    
    inst = ManageFlowcell(dbinst)
    
    
#     if parseinst.prepare: inst.prepare()
#     TODO: transfer this in the prepare function
#     TODO: how to handle the sequencing, pipestatus, trackstatus? via argparse?
# TODO: how to handle bcl2fastq version? via argparse?
#         inst.find_flowcell_add_list(3, 'open', 'cmcb')
#      
#         for fcinst in inst.flowcelllist:
#             fcinst, runstatus = inst.prepare_flowcell_pipelining(fcinst, 3, trackstatus = (1,2,3), fcloc = 'cmcb')
#             if runstatus == 'pipeline':
#                 if inst.write_samplessheet(fcinst, 'cmcb'):
#                     if inst.build_write_snakemakeconfig(fcinst, '2.19.1', 'cmcb', 'zih'):
#                         pass
#                     dbinst.update_sequencing_pipelinestatus_into_flowcells(fcinst.dbid, fcinst.seqstatus, fcinst.pipestatus)
#                     TODO: write this function
#                     dbinst.set_job_transer_open
                    
  
    

            
    
#     dbinst.commitConnection()
    dbinst.closeConnection()
    mainlog.close()
    