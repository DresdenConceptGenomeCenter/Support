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

import logging

import mysql.connector

from mysql.connector import errorcode

class Database(object):
    def __init__(self, host, user, pw, db):
        self.__host = host
        self.__user = user
        self.__pw = pw
        self.__db = db
        self.__conn = ''
        self.__cursor = ''
        self.__logger = logging.getLogger('support.database')

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

    def setConnection(self):
        try:
            self.__conn = mysql.connector.connect(host = self.__host, user = self.__user, password = self.__pw, database = self.__db)
            self.show_log('info', "database connection established")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                self.show_log('error', "Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                self.show_log('error', "Database does not exist")
            else:
                print(err)

 
    def closeConnection(self):
        self.show_log('info', "database connection shutdown")
        self.__conn.close()
    

    def commitConnection(self):
        self.__conn.commit()

    '''
    function updates the raw data paths and fc number of a flowcell entry in the flowcell table.
    it needs the ID as where_condition
    @param dbid: integer (row ID)
    @param cmcbpath: string
    @param zihpath: string
    @param number: string
    '''
    def update_path_number_into_flowcells(self, dbid, cmcbpath, zihpath, number):
        cursor = self.__conn.cursor()
#         updater = ('UPDATE Flowcells SET RAW_DATA_PATH = %s, NUMBER = %s, ZIH_DATA_PATH = %s WHERE ID = %s')
        updater = ('UPDATE Flowcells SET RAW_DATA_PATH = %s, NUMBER = %s WHERE ID = %s')
        
#         cursor.execute(updater, (cmcbpath, zihpath, number, dbid))
        cursor.execute(updater, (cmcbpath, number, dbid))
        cursor.close()

    '''
    function updates the sequencing and pipelining status.
    it needs the ID as where_condition
    @param dbid: integer (row ID)
    @param seqstatus: string
    @param pipestatus: integer
    '''
    def update_sequencing_pipelinestatus_into_flowcells(self, dbid, seqstatus, pipestatus):
        cursor = self.__conn.cursor()
        updater = ('UPDATE Flowcells SET FLOWCELLSSTATUS_ID = %s, PIPELINING_STATUS = %s WHERE ID = %s')
        cursor.execute(updater, (seqstatus, pipestatus, dbid))
        cursor.close()
 
    '''
    function queries the database table clients with the flowcell id and returns a dictionary
    @param clientid: integer
    @return: dictionary
    '''
    def query_clients_with_clientid(self, clientid):
        cursor = self.__conn.cursor(dictionary = True)
        query = 'SELECT * FROM Clients WHERE ID=%s'.format(clientid)
        cursor.execute(query, (clientid, ))
        resultset = cursor.fetchone()
        cursor.close()
        return resultset
  
    '''
    function queries the database table flowcells with the fcstatus and pipelinestatus.
    it returns a list of dictionaries
    @param fcstatus: integer
    @param pipelinestatus: string
    @return: list of dictionaries
    '''
    def query_flowcell_with_status(self, fcstatus, pipelinestatus):
        cursor = self.__conn.cursor(dictionary = True)
        query = ('SELECT * FROM Flowcells where PIPELINING_STATUS=%s and FLOWCELLSSTATUS_ID=%s')
        cursor.execute(query, (pipelinestatus, fcstatus))
        resultset = cursor.fetchall()
        cursor.close()
        return resultset

    '''
    function queries the database table indexes with the index id and returns a dictionary
    @param indexid: integer
    @return: dictionary
    '''
    def query_indexes_with_indexid(self, indexid):
        cursor = self.__conn.cursor(dictionary = True)
        query = ('SELECT * FROM Indexes WHERE ID=%s')
        cursor.execute(query, (indexid, ))
        resultset = cursor.fetchone()
        cursor.close()
        return resultset

    '''
    function queries the database table libraries with the library id and returns a dictionary
    @param libid: integer
    @return: dictionary
    '''
    def query_libraries_with_libid(self, libid):
        cursor = self.__conn.cursor(dictionary = True)
        query = ('SELECT * FROM Libraries WHERE ID=%s')
        cursor.execute(query, (libid, ))
        resultset = cursor.fetchone()
        cursor.close()
        return resultset

    '''
    function queries the machine table and return a list of dictionaries
    @return: list of dictionaries
    '''
    def query_machines(self):
        cursor = self.__conn.cursor(dictionary = True)
        query = ('SELECT * FROM Machines')
        cursor.execute(query)
        resultset = cursor.fetchall()
        cursor.close()
        return resultset

    '''
    function queries the machine table with machine id and returns a dictionary
    @param machineid: integer
    @return: dictionary
    '''
    def query_machines_with_machineid(self, machineid):
        cursor = self.__conn.cursor(dictionary = True)
        query = ('SELECT * FROM Machines WHERE ID=%s')
        cursor.execute(query, (machineid, ))
        resultset = cursor.fetchone()
        cursor.close()
        return resultset

    '''
    function queries the samples table with sample id and returns a dictionary
    @param sampleid: integer
    @return: dictionary
    '''
    def query_samples_with_sampleid(self, sampleid):
        cursor = self.__conn.cursor(dictionary = True)
        query = ('SELECT * FROM Samples WHERE ID=%s')
        cursor.execute(query, (sampleid, ))
        resultset = cursor.fetchone()
        cursor.close()
        return resultset

    '''
    function queries the tracks table with the flowcell id and returns a list of dictionaries of tracks
    @param fcid: integer
    @return: list of dictionaries 
    '''
    def query_tracks_with_flowcellid(self, fcid):
        cursor = self.__conn.cursor(dictionary = True)
        query = ('SELECT * FROM Tracks WHERE FLOWCELL_ID=%s')
        cursor.execute(query, (fcid, ))
        resultset = cursor.fetchall()
        cursor.close()
        return resultset


# @TODO flowcell insert (code, 
    
    
    