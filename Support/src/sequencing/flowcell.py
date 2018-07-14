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

from collections import defaultdict

from operator import itemgetter

from os.path import join as pathjoin
from os.path import join as isfile
from os.path import isdir

from re import compile


''' own modules '''
from sequencing.machine import Machine
from helper.io_module import get_reverse_complement
from helper.io_module import read_file_get_list

from helper.support_information import SupportInformation as SI

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
        self._number = ''
        
        self._seqstatus = '' # 1 .. fresh, 2 .. on sequencer, 3 .. finished
        self._pipestatus =  '' # open or done

        self._issingle = True
        self._reverse_complement = False
        
        self._readlist = [] # number of sequenced reads
        self._indexlist = [] # length of each index
        self._seqorder = [] # order of sequencing I .. Index, R .. normal Reads
        
        self._pathdict = {}
        
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

    def get_pathdict(self):
        return self._pathdict

    def get_pathdict_with_location(self, location):
        return self._pathdict[location]

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
        return self._reverse_complement

    def get_issingle(self):
        return self._issingle

    def get_dbid(self):
        return self._dbid

    def get_number(self):
        return self._number

    def get_readlist(self):
        return self._readlist

    def get_indexlist(self):
        return self._indexlist

    def get_seqorder(self):
        return self._seqorder

    def set_readlist(self, value):
        self._readlist = value

    def set_indexlist(self, value):
        self._indexlist = value

    def set_seqorder(self, value):
        self._seqorder = value

    seqstatus = property(get_seqstatus, set_seqstatus)
    pipestatus = property(get_pipestatus, set_pipestatus)
    code = property(get_code, set_code)
    machine = property(get_machine, set_machine)
    name = property(get_name, set_name)
    dbid = property(get_dbid, set_dbid)
    cmcbpath = property(get_cmcbpath)
    zihpath = property(get_zihpath)
    number = property(get_number, set_number)
    readlist = property(get_readlist, set_readlist)
    indexlist = property(get_indexlist, set_indexlist)
    seqorder = property(get_seqorder, set_seqorder)
    issingle = property(get_issingle, set_issingle)
    pathdict = property(get_pathdict)
     

class IlluminaFlowcell(Flowcell):
    def __init__(self, machine, code, name, dbid):
        Flowcell.__init__(self, machine, code)
        
        self._name = name
        self._dbid = dbid
        self._number = '_'.join(self._name.split('_')[1:3]) # .replace('_0', '_')
        
        self.__rtapath_cmcb = pathjoin(machine.cmcbstorage, self._name, 'RTAComplete.txt')
        self.__runinfopath_cmcb = pathjoin(machine.cmcbstorage, self._name, 'RunInfo.xml')
        
        self.__rtapath_zih = pathjoin(machine.zihstorage, self._name, 'RTAComplete.txt')
        self.__runinfopath_zih = pathjoin(machine.zihstorage, self._name, 'RunInfo.xml')

        self.__basemaskdict = defaultdict(list)
        self.__lanedict = defaultdict(list)
        self.__samplesheetdict = {}

        self.__regnumber = compile('Number="[0-9]"')
        self.__regcycles = compile('NumCycles="[0-9]+"')
        self.__regindex = compile('IsIndexedRead="[Y|N]"')
    
        self.__csvcounter = 1

        self.set_reversecomplement_machine()
        self.set_zihpath_machine(self._name)
        self.set_cmcbpath_machine(self._name)
        
        self.build_path_dict()


    def build_path_dict(self):
        cmcb = {
            'rtapath' : self.__rtapath_cmcb,
            'runinfopath' : self.__runinfopath_cmcb,
            'machinepath' : self._cmcbpath
            }

        zih = {
            'rtapath' : self.__rtapath_zih,
            'runinfopath' : self.__runinfopath_zih,
            'machinepath' : self._zihpath
            }

        self._pathdict = {
            'cmcb' : cmcb,
            'zih' : zih
            }


    '''
    check if file RTAcomplete exists and return True/False.
    @param where: string
    @return: boolean
    '''
    def is_RTAcomplete(self, where = 'cmcb'):
        if where == 'cmcb': 
            if isfile(self._pathdict[where]['rtapath']): return True
        else:
            return False

    '''
    method reads in the RunInfo.xml to extract the length of the sequenced reads and how many barcodes were used.
    select the path with the where parameter for either cmcb or zih
    @param where: string
    '''
    def parse_runinfo_file(self, where = 'cmcb'):
        with open(self._pathdict[where]['runinfopath'], 'r') as filein:
            for line in filein:
                number = self.__regnumber.findall(line)
                cycles = self.__regcycles.findall(line)
                index = self.__regindex.findall(line)
                if len(number) != 0:
                    number, cycles, index = int(number[0].split('"')[1]), int(cycles[0].split('"')[1]), index[0].split('"')[1]
                    if index == 'Y':
                        self._indexlist.append(cycles)
                        self._seqorder.append('I')
                    else:
                        self._readlist.append(cycles)
                        self._seqorder.append('R')
        self.show_log('info', "flowcell status: '{0}' No. Reads: {3} Length: {1} - No. Barcodes: {4} Length: {2}".format(self._code, self._readlist, self._indexlist, len(self._readlist), len(self._indexlist)))
    
    '''
    method counts how many tracks per lane exist and returns either an empty
    list or a list of integers
    @return: list of integers
    '''
    def collect_lane_stats(self):
        lanes = []
        if len(self.__lanedict) == 0:
            self.show_log('warning', "flowcell status: '{0}' has no lanes and tracks".format(self._code))
            return lanes
        for i in self.__lanedict.values():
            lanes.append(len(i))
        self.show_log('info', "flowcell status: '{0}' has {1} lane(s) with {2} track(s) ({3})".format(self._code, len(lanes), sum(lanes), lanes))
        return lanes
        

    '''
    method queries database and retrieves all tracks belonging to the flowcell and having a certain status.
    next, for each track the libid, owner, barcodes and barcode name is retrieved and everything is stored
    in a dictionary where the key is the lane number and the value is a list of tuples defined as
    (client id, libstring (L{libid}_Track-{trackid}), samplename, bc name, bc1, bc2
    @param dbinst: database instance
    @param trackstatus: tuple
    '''
    def build_lanedict(self, dbinst, trackstatus = (1,2,3)):
        tracklist = dbinst.query_tracks_with_flowcellid(self._dbid)
        for trackdict in tracklist:
            if trackdict['TRACKSSTATUS_ID'] not in trackstatus: continue
            libid = trackdict['LIBRARY_ID']
            libdict = dbinst.query_libraries_with_libid(libid) # get the lib entry
            sampledict = dbinst.query_samples_with_sampleid(libdict['SAMPLE_ID']) # with lib get sample
            clientdict = dbinst.query_clients_with_clientid(sampledict['CLIENT_ID']) # with sample get client
            libstring = 'L{0}_Track-{1}'.format(libid, trackdict['ID'])
    
            if libdict['INDEX_ID'] is None:
                self.__lanedict[trackdict['COMPARTMENT']].append([clientdict['ID'], libstring, libid, sampledict['NAME'].replace(' ', '_'), 'NoBarcode', '', '']) # add to dictionary        
            else:
                bcdict = dbinst.query_indexes_with_indexid(libdict['INDEX_ID']) # with lib get barcode
                bc1 = '' if bcdict['SEQ'] is None else bcdict['SEQ']
                bc2 = '' if bcdict['SEQ2'] is None else bcdict['SEQ2']
                self.__lanedict[trackdict['COMPARTMENT']].append([clientdict['ID'], libstring, libid, sampledict['NAME'].replace(' ', '_'), bcdict['NAME'], bc1, bc2]) # add to dictionary
        
        self.collect_lane_stats()

    '''
    method calculates the min and max length of a list of barcodes (are a list as strings (e.g. ACGTACGT))
    and checks if there is an empty barcode
    @param templanelist: list of strings
    @return: int, int, boolean
    '''
    def get_min_max_empty_len(self, templanelist):
        templen = [len(i) for i in templanelist] # the length of the barcodes
        minvalue, maxvalue = min(templen), max(templen) # minimum and maximum length of the barcodes
        emptybc = True if sum([1 for i in templen if i == 0]) > 0 else False # is one barcode empty, then all are empty
        return minvalue, maxvalue, emptybc

    '''
    methods builds the basesmask string for the bcl2fastq
    there are several case for the indexes that are taken care of
    a) no barcodes are on the track but in the sequencing folder -> return n's
    b) barcode length is smaller as bc in sequencing folder -> n's are attached
    c) barcode length = bc length in sequencing folder
    @param fcindexlist: list
    @param bclenlist: list
    @return: list
    '''
    def build_basesMask_string(self, bclenlist):
#         temp = []
#         for c, fcindexlength in enumerate(self._indexlist):
#             bclen = bclenlist[c]
#             if bclen == 0: temp.append('n{0}'.format(fcindexlength)) # a)
#             elif fcindexlength > bclen: temp.append('I{0}n{1}'.format(bclen, fcindexlength - bclen)) # b)
#             else: temp.append('I{0}'.format(fcindexlength)) # c)
#         return temp
   
        rcount, bcount, temp = 0, 0, []
        for entry in self._seqorder:
            if entry == 'R':
                temp.append('Y{0}'.format(self._readlist[rcount]))
                rcount += 1
            else:
                fcindexlength = self._indexlist[bcount]
                bclen = bclenlist[bcount]
                if bclen == 0: temp.append('n{0}'.format(fcindexlength)) # a)
                elif fcindexlength > bclen: temp.append('I{0}n{1}'.format(bclen, fcindexlength - bclen)) # b)
                else: temp.append('I{0}'.format(fcindexlength)) # c)
                bcount += 1
        return ','.join(temp)
    

    '''
    method modifies the given barcodes per lane. there are several cases which are taken care of
    a) only one barcode was sequenced -> remove 2nd barcodes from all tracks 
    b) one of the first barcodes is empty -> all barcodes have to be empty -> return single track per lane with empty bc1, bc2
    c1) cut all 1st database barcodes to the shortest length of a database barcode
    c2) the 1st sequenced bc is smaller than 1st database barcode -> short database barcode to 1st sequenced barcode
    d1) same as c1) if 2nd sequenced bc is given
    d2) same as c2) if 2nd sequenced bc is given
    d3) built the reverse complement of 2nd barcode if the machine is a nextseq
    it returns the lanelist and a tuple with the barcode length of the indexes
    @param lanelist: list
    @param lane: integer
    @return: list, tuple(integer, integer)
    '''
    def modify_BC_per_lane(self, lanelist, lane):
        lanelist = sorted(lanelist, key = itemgetter(0, 1)) # sort by client and then by libstring
#         bc1list, bc2list = [i[5] for i in lanelist], [i[6] for i in lanelist] # lists with bc1 and bc2
        bc1list, bc2list = zip(*[(i[5], i[6]) for i in lanelist])
#         get min, max and check if several bcs are empty
        minbc1, maxbc1, emptybc1 = self.get_min_max_empty_len(bc1list)
        minbc2, maxbc2, emptybc2 = self.get_min_max_empty_len(bc2list)
#         a)
        if emptybc2 or self._issingle:
            for i in range(len(lanelist)):
                lanelist[i][6] = ''
            minbc2, maxbc2, emptybc2 = 0, 0, True
#         b)
        if emptybc1:
#             one of the first barcodes of the tracks on this lane is empty; all have to be empty; return single track
            self.show_log('info', 'pipeline status: {0} lane: {1} some bc1 are empty -> reduce whole lane to single track'.format(self._code, lane))
            lanelist[0][5], lanelist[0][6] = '', ''
            return (lanelist[0], ), (0, 0)
      
        for i in range(len(lanelist)):
            if minbc1 != maxbc1: lanelist[i][5] = lanelist[i][5][:minbc1] # c1)
            if minbc1 > self._indexlist[0]: lanelist[i][5] = lanelist[i][5][:self._indexlist[0]] # c2)
        
            if not self._issingle and not emptybc2:
                if minbc2 != maxbc2: lanelist[i][6] = lanelist[i][6][:minbc2] # d1)
                if minbc2 > self._indexlist[1]: lanelist[i][6] = lanelist[i][6][:self._indexlist[1]] # d2)
                if self._reverse_complement: lanelist[i][6] = get_reverse_complement(lanelist[i][6]) # d3)
        return lanelist, (minbc1, minbc2)

    '''
    small loop that goes through the available lanes
    '''
    def loopLanes_buildBC_buildBasesMask(self):
        for lane, lanelist in self.__lanedict.items():
            self.__lanedict[lane], barcodedblen = self.modify_BC_per_lane(lanelist, lane)
            bmaskstring = self.build_basesMask_string(barcodedblen)
            self.__basemaskdict[bmaskstring].append(lane)

    '''
    prepare the samplesheet for demultiplexing and create for each different basesmask a samplesheet
    '''
    def prepare_samplesheet(self, fcloc):
        ssheetlist = read_file_get_list(pathjoin(SI.STORAGEDICT[fcloc]['FILEFOLDER'], SI.SAMPLESHEET_NAME))
        
        for basemask, lanes in self.__basemaskdict.items():
            tempsheet = ssheetlist[::]
            for lane in sorted(lanes):
                tracks = self.__lanedict[lane]
#         Client,Sample_ID/Sample_Name,Barcode Name,BC1, BC2
#         ['mehmetc', 'L25268_Track-57024', 'ILL_MAR15_7-54', 'CTGTATGC', '']        
#         to
#         'LIBTRACK,LIBTRACK,CLIENT,LANE,BC1,BC2'
#         Sample_ID,Sample_Name,Sample_Project,Lane,index,index2
                for track in tracks:
                    trackstring = SI.SAMPLESHEETLINE.replace('LIBTRACK', track[1]).replace('CLIENT', track[0]).replace('BC1', track[5]).replace('BC2', track[6])
                    if self._reverse_complement:
                        trackstring = trackstring.replace('LANE', '')
                    else:
                        trackstring = trackstring.replace('LANE', str(lane))
                    tempsheet.append(trackstring)
                tempsheet = [i+'\n' for i in tempsheet]
            self.__samplesheetdict['{0}_{1}'.format(self._number, self.__csvcounter)] = (tempsheet, basemask)
            self.__csvcounter += 1


    def get_basemaskdict(self):
        return self.__basemaskdict
    
    def get_samplesheetdict(self):
        return self.__samplesheetdict

    basemaskdict = property(get_basemaskdict)
    samplesheetdict = property(get_samplesheetdict)
    

            