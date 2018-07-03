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
from sequencing.machine import Machine

''' python modules '''
import logging

from os.path import join as pathjoin, isfile
from os.path import isdir

''' own modules '''


'''
Class describing a flowcell. It's has the main attributes from the database model
'''

class Flowcell(object):
    def __init__(self, machine, code):
        self._cmcbpath = ''
        self._zihpath = ''
        self._objectstore_id = ''
        self._objectstore_date = ''
        self._archivepath = ''
        self._archivedate = ''

        self._code = code # flowcell id
        self._name = '' # name of the directory
        self._dbid = '' # id in the database
        self._machine = machine # instance of machine class
        self._dbid = ''
        self._number = ''
        
        self._seqstatus = '' # 1 .. fresh, 2 .. on sequencer, 3 .. finished
        self._pipestatus =  '' # open or done

        self._issingle = True
        self._reverse_complement = False
        
        self._logger = logging.getLogger('support.flowcell')

    def show_log(self, level, message):
        if level == 'debug':
            self._logger.debug(message)
        elif level == 'info':
            self._logger.info(message)
        elif level == 'warning':
            self._logger.warning(message)
        elif level == 'error':
            self._logger.error(message)
        elif level == 'critical':
            self._logger.critical(message)

#     '''
#     init 4 variables which are retrievable from the database
#     @param code: string
#     @param seqstatus: integer
#     @param pipestatus: string
#     @param machine: Machine instance
#     '''
#     def do_sql_init(self, code, seqstatus, pipestatus, machine):
#         self._code = code
#         self._seqstatus = seqstatus
#         self._pipestatus = pipestatus
#         self._machine = machine

    '''
    method sets the path for the cmcb storage. if machine is an instance,
    takes the path from the machine
    @param pathstring: string
    '''
    def set_cmcbpath(self, pathstring):
        if isdir(pathstring):
            self._cmcbpath = pathstring
        else:
            self.show_log('warning', '{0} is not a valid path for the flowcell at the CMCB'.format(pathstring))

    '''
    method sets the path for the cmcb storage. if machine is an instance,
    takes the path from the machine
    @param flowcellname: string
    '''
    def set_cmcbpath_machine(self, flowcellname):
        if isinstance(self._machine, Machine):
            self._cmcbpath = pathjoin(self._machine.cmcbstorage, flowcellname)
        else:
            self.show_log('warning', 'machine instance does not exist. cannot create CMCB path for flowcell')

    '''
    method sets the path for the zih storage. if machine is an instance,
    takes the path from the machine
    @param pathstring: string
    '''
    def set_zihpath(self, pathstring):
        if isdir(pathstring):
            self._zihpath = pathstring
        else:
            self.show_log('warning', '{0} is not a valid path for the flowcell at the ZIH'.format(pathstring))

    '''
    method sets the path for the cmcb storage. if machine is an instance,
    takes the path from the machine
    @param flowcellname: string
    '''
    def set_zihpath_machine(self, flowcellname):
        if isinstance(self._machine, Machine):
            self._zihpath = pathjoin(self._machine.zihstorage, flowcellname)
        else:
            self.show_log('warning', 'machine instance does not exist. cannot create ZIH path for flowcell')
    
    def set_objectstore_id(self, oid):
        self._objectstore_id = oid
    
    def set_objectstore_date(self, date):
        self._objectstore_date = date
    
    def set_archivepath(self, pathstring):
        self._archivepath = pathstring
    
    def set_archivedate(self, date):
        self._archivedate = date
        
    def set_name(self, name):
        self._name = name
    
    def set_machine(self, machine):
        self._machine = machine
    
    def set_code(self, code):
        self._code = code
    
    def set_issingle(self, issingle):
        self._issingle = issingle

    def set_dbid(self, dbid):
        self._dbid = dbid

    def set_reversecomplement_machine(self):
        if isinstance(self._machine, Machine):
            self._reverse_complement = self._machine.reverse_complement
        else:
            self.show_log('warning', 'machine instance does not exist. cannot set reverse complement')
            self._reverse_complement = False

    def set_reversecomplement(self, reverse_complement):
        self._reverse_complement = reverse_complement
    
    '''
    set the pipelining status. input must match with db pipeline status options
    @param param: string
    '''
    def set_pipestatus(self, pipestatus):
        self._pipestatus = pipestatus

    '''
    set the sequencing status of the flowcell. input must match with db sequencing status options
    @param int: 
    '''
    def set_seqstatus(self, seqstatus):
        self._seqstatus = seqstatus
    
    def set_number(self, number):
        self._number = number
    
    def get_seqstatus(self):
        return self._seqstatus 
    
    def get_pipestatus(self):
        return self._pipestatus
    
    def get_cmcbpath(self):
        return self._cmcbpath
    
    def get_zihpath(self):
        return self._zihpath

    def get_objectstore_id(self):
        return self._objectstore_id

    def get_objectstore_date(self):
        return self._objectstore_date

    def get_archivepath(self):
        return self._archivepath

    def get_archivedate(self):
        return self._archivedate

    def get_name(self):
        return self._name

    def get_machine(self):
        return self._machine

    def get_code(self):
        return self._code
    
    def get_reverse_complement(self):
        return(self._reverse_complement)

    def get_issingle(self):
        return self._issingle

    def get_dbid(self):
        return self._dbid

    def get_number(self):
        return self._number

    seqstatus = property(get_seqstatus, set_seqstatus)
    pipestatus = property(get_pipestatus, set_pipestatus)
    code = property(get_code, set_code)
    machine = property(get_machine, set_machine)
    name = property(get_name, set_name)
    dbid = property(get_dbid, set_dbid)
    cmcbpath = property(get_cmcbpath)
    zihpath = property(get_zihpath)
    number = property(get_number, set_number)
     

class IlluminaFlowcell(Flowcell):
    def __init__(self, machine, code, name, dbid):
        Flowcell.__init__(self, machine, code)
        
        self._name = name
        self._dbid = dbid
        self._number = '_'.join(self._name.split('_')[1:3]) # .replace('_0', '_')
        
        self.__rtapath_cmcb = pathjoin(machine.cmcbstorage, self._name, 'RTAComplete.txt')
        self.__runinfopath_cmcb = pathjoin(machine.cmcbstorage, self._name, 'RunInfo.xml')
        
        self.__rtapath_zih = pathjoin(machine.zihstorage, self._name, 'RTAComplete.txt')
        self.__runinfopath_cmcb = pathjoin(machine.zihstorage, self._name, 'RunInfo.xml')
        
        self.set_reversecomplement_machine()
        self.set_zihpath_machine(self._name)
        self.set_cmcbpath_machine(self._name)

    def is_RTAcomplete_cmcb(self):
        if isfile(self.__rtapath_cmcb):
            return True
        else:
            return False






            