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

''' python modules ''' 
import logging

from os import listdir

''' own modules '''
from helper.helper_logger import MainLogger

from helper.database import Database
from helper.support_information import SupportInformation as SI

from sequencing.machine import Machine 
from sequencing.flowcell import IlluminaFlowcell

class ManageFlowcell(object):
    def __init__(self, dbinst):
        self.__statuslist = [1, 2, 3] # 1 .. fresh, 2 .. on sequencer, 3 .. finished
        self.__pipestatuslist = ['open', 'done']
        self.__dbinst = dbinst
    
        self.__flowcelllist = []
    
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
        
        return Machine(medict['NAME'], medict['CODE'], medict['DEFAULT_STORAGE'], '/projects/wherever', platform, medict['ID'])

    
    '''
    small function designed to find flowcells having a certain sequencing and pipestatus.
    if this is successful, add flowcell to list
    @param status: integer
    @param pipestatus: string
    '''
    def find_flowcell_add_list(self, status, pipestatus):
        entries = self.__dbinst.query_flowcell_with_status(status, pipestatus)
        
        if len(entries) == 0:
            self.show_log('info','No flowcells are in ({0}, {1}) mode. Nothing to do!'.format(status, pipestatus))
            return
        
        for fcdict in entries:
            minst = self.prepare_machine_inst(self.__dbinst.query_machines_with_machineid(fcdict['MACHINE_ID']))
            
            if minst.platform == 'pacbio':
                pass
            else:
                allfcs = listdir(minst.cmcbstorage) # retrieve all flowcells directories belonging to this flowcell
                flowcell = [i for i in allfcs if fcdict['CODE'] in i and 'archived' not in i]
                if len(flowcell) == 0:
                    self.show_log('warning','Cannot find path. Check if path or flowcell code is correct for {0} and machine {1}'.format(fcdict['CODE'], minst.name))
                    continue
                
                fcinst = IlluminaFlowcell(minst, fcdict['CODE'], flowcell[0], fcdict['ID'])
                fcinst.pipestatus, fcinst.seqstatus = pipestatus, status
                self.show_log('info', "add to flowcell list: '{0}' from machine '{1}' with status ({2}, {3})".format(fcdict['CODE'], minst.name, status, pipestatus))
                self.__flowcelllist.append(fcinst)

    def prepare_flowcell_pipelining(self, fcinst, seqstatus = 2, pipestatus = 'open'):
        if fcinst.seqstatus == seqstatus and fcinst.pipestatus == pipestatus:
            
            if fcinst.machine.platform == 'illumina':
                if not fcinst.is_RTAcomplete_cmcb():
                    self.show_log('info', "still sequencing: '{0}' from machine '{1}'".format(fcinst.code, fcinst.machine.name))
                    return
                
                fcinst.seqstatus = 3
#                 self.__dbinst.update_path_number_into_flowcells(fcinst.dbid, fcinst.cmcbpath, fcinst.zihpath, fcinst.number)
#                 self.__dbinst.update_sequencing_pipelinestatus_into_flowcells(fcinst.dbid, fcinst.seqstatus, fcinst.pipestatus)
            
            elif fcinst.machine.platform == 'pacbio':
                pass
            
            
            self.show_log('info', "ready pipelining: '{0}' from machine '{1}'".format(fcinst.code, fcinst.machine.name))
        
        
        
            
             
    
    def get_flowcelllist(self):
        return self.__flowcelllist
    
    
#     def get_status(self, index):
#         return self.__statuslist[index]
# 
#     def get_pipestatus(self, index):
#         return self.__pipestatuslist[index]
#         
#     def set_status(self, status):
#         self.__status = status
#     
#     def set_pipestatus(self, pipestatus):
#         self.__pipestatus = pipestatus
#     
#     status = property(get_status)
#     pipestatus = property(get_pipestatus)
    

    flowcelllist = property(get_flowcelllist)


if __name__ == '__main__':
    mainlog = MainLogger('support')
    dbinst = Database(SI.DB_HOST, SI.DB_USER, SI.DB_PW, SI.DB)
    dbinst.setConnection()
    
    inst = ManageFlowcell(dbinst)
    inst.find_flowcell_add_list(2, 'open')
    
    for counter, fcinst in enumerate(inst.flowcelllist):
        inst.prepare_flowcell_pipelining(fcinst, 2)
    
    dbinst.commitConnection()
    dbinst.closeConnection()
    mainlog.close()
    