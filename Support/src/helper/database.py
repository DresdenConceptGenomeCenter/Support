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

    def rollbackConnection(self):
        self.__conn.rollback()

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
    function queries the database table flowcells with the raw data path.
    it returns a list of dictionaries
    @param raw_data_path: string
    @return: list of dictionaries
    '''
    def query_flowcell_with_raw_data_path(self, raw_data_path):
        cursor = self.__conn.cursor(dictionary = True)
        query = ('SELECT * FROM Flowcells where RAW_DATA_PATH=%s')
        cursor.execute(query, (raw_data_path, ))
        resultset = cursor.fetchall()
        cursor.close()
        return resultset

    '''
    function queries the machine table with machine id and returns a single dictionary
    @param machineid: integer
    @return: list of dictionaries
    '''
    def query_machines_with_machineid(self, machineid):
        cursor = self.__conn.cursor(dictionary = True)
        query = ('SELECT * FROM Machines WHERE ID=%s')
        cursor.execute(query, (machineid, ))
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
    function queries the library table and returns a list of libraries. Note that the tables
    LibrariesStatus, AccountingStatus, PriceProductTables, PriceDiscountLevels, PriceUserGroups,
    Platforms and LibrariesTypes are joined with libraries so that you can access their content via
    the table name and field. Parameters allow choose the fields to select and filter as well as
    additional operations such as GROUP_BY, etc.
    @param select_string: mysql string to select fields
    @param join_string: additional joins to be made
    @param where_string: mysql string to filter by fields
    @param where_values: values for the filtering
    @param suffix: additional mysql appended after the where statement
    @return: list of libraries
    '''
    def query_libraries(self,select_string=None,join_string = '', where_string='true',where_values=[],suffix_string=''):
        cursor = self.__conn.cursor(dictionary = True)
        
        # default fields to select
        if not select_string:
            select_string = """
            Libraries.ID AS 'Libraries.ID',Libraries.NAME AS 'Libraries.NAME',Libraries.SAMPLE_ID AS 'Libraries.SAMPLE_ID',Libraries.CONCENTRATION AS 'Libraries.CONCENTRATION',
            Libraries.VOLUME AS 'Libraries.VOLUME',Libraries.DATE AS 'Libraries.PREPARATION_DATE',Libraries.NOTES AS 'Libraries.NOTES',Libraries.INDEX_ID AS 'Libraries.INDEX_ID',
            Libraries.REQUESTED_SEQUENCING AS 'Libraries.REQUESTED_READS',Libraries.ACCOUNTING_DATE AS 'Libraries.ACCOUNTING_DATE',Libraries.CLIENT_ACCESS AS 'Libraries.CLIENT_ACCESS',
            Libraries.ACTIVITY AS 'Libraries.ACTIVITY',Libraries.ACTIVITY AS 'Libraries.ACTIVITY',LibrariesStatus.NAME AS 'LibrariesStatus.NAME',
            AccountingStatus.NAME AS 'AccountingStatus.NAME',PriceProductTables.NAME AS 'PriceProductTables.NAME',PriceDiscountLevels.NAME AS 'PriceDiscountLevels.NAME',
            PriceUserGroups.NAME AS 'PriceUserGroups.NAME',Platforms.NAME AS 'Platforms.NAME',LibrariesTypes.NAME AS 'LibrariesTypes.NAME'
            """
        
        # build up query
        query = """
        SELECT {0} FROM Libraries 
        LEFT OUTER JOIN LibrariesStatus ON Libraries.LIBRARIESSTATUS_ID = LibrariesStatus.ID
        LEFT OUTER JOIN AccountingStatus ON Libraries.ACCOUNTINGSTATUS_ID = AccountingStatus.ID
        LEFT OUTER JOIN PriceProductTables ON Libraries.PRICE_PRODUCT_TABLE_ID = PriceProductTables.ID
        LEFT OUTER JOIN PriceDiscountLevels ON Libraries.PRICE_DISCOUNT_LEVEL_ID = PriceDiscountLevels.ID
        LEFT OUTER JOIN PriceUserGroups ON Libraries.PRICE_USER_GROUP_ID = PriceUserGroups.ID
        LEFT OUTER JOIN Platforms ON Libraries.PLATFORMS_ID = Platforms.ID
        LEFT OUTER JOIN LibrariesTypes ON Libraries.LIBRARIESTYPES_ID = LibrariesTypes.ID 
        {1} 
        WHERE {2} 
        {3} 
        """.format(select_string,join_string,where_string,suffix_string)
                
        # execute and return
        cursor.execute(query,where_values)
        resultset = cursor.fetchall()
        cursor.close()
        return resultset

    '''
    function inserts a new flowcell into the database and returns 
    the id of the new database entry. Note that the correct ids for
    machine, flowcell_status and flowcell_type are handled automatically
    through joins. 
    @param code: string
    @param machine_code: string
    @param flowcell_type: string
    @param production_date: string
    @param sequencing_date: string
    @param is_barcoded: boolean
    @param notes: string
    @param number: string
    @param archiving_date: string
    @param archiving_status: integer
    @param compartment_count: integer
    @param compartment_capacity: float
    @param activity: integer
    @param raw_data_path: string
    @param pipelining_status: string
    @param status: string
    @param additional_information: string
    @return: integer
    '''
    def insert_flowcell(
                        self,
                        code,
                        machine_code,
                        flowcell_type = 'HiSeq SE',
                        production_date = None,
                        sequencing_date = None,
                        is_barcoded = False,
                        notes = None,
                        number = None,
                        archiving_date = None,
                        archiving_status = 0,
                        compartment_count = 1,
                        compartment_capacity = 1,
                        activity = 1,
                        raw_data_path = None,
                        pipelining_status = 'open',
                        flowcell_status = 'fresh',
                        additional_information = None):
        
        cursor = self.__conn.cursor()
        query = """
        INSERT INTO Flowcells (CODE,FLOWCELLSTYPE_ID,PRODUCTION_DAY,PRODUCTION_MACHINE,SEQUENCING_DAY,SEQUENCING_MACHINE,BARCODING,NOTES,
        FLOWCELLSSTATUS_ID,NUMBER,RAW_DATA_PATH,RAW_DATA_STATUS,ARCHIVING_DATE,ARCHIVING_STATUS,ACTIVITY,MACHINE_ID,COMPARTMENT_COUNT,COMPARTMENT_CAPACITY,
        PIPELINING_STATUS,ADDITIONAL_INFORMATION)
        (SELECT %s AS CODE,FlowcellsTypes.ID AS FC_TYPE_ID,%s AS PROD_DATE,Machines.NAME AS PROD_MACH,%s AS SEQ_DATE,Machines.NAME AS SEQ_MACH,%s AS IS_BC,%s AS NOTES,
        FlowcellsStatus.ID AS FC_Status_ID,%s AS FC_NUMBER,%s AS FC_RAW_PATH,%s AS RAW_STATUS,%s AS ARCH_DATE,%s AS ARCH_STATUS,%s AS ACTIVITY,
        Machines.ID AS Machine_ID,%s AS COMP_CNT,%s AS COMP_CAPACITY,%s AS PIPE_STATUS,%s AS ADDIT_INFO 
        FROM FlowcellsTypes LEFT JOIN Machines ON Machines.CODE = %s LEFT JOIN FlowcellsStatus ON FlowcellsStatus.NAME = %s WHERE FlowcellsTypes.NAME = %s)
        """
        cursor.execute(query, (code, production_date, sequencing_date, "yes" if is_barcoded else "no",notes,
                               number,raw_data_path,None,archiving_date,archiving_status,activity,
                               compartment_count,compartment_capacity,pipelining_status,additional_information,
                               machine_code,flowcell_status,flowcell_type))
        flowcell_id = cursor.lastrowid
        cursor.close()
        
        return flowcell_id

    '''
    function inserts a new track into the database and returns 
    the id of the new database entry. Note that the correct ids for operator,
    track_status, price_product_table,price_discount_level and
    price_user_group are handled automatically through joins. 
    @param flowcell_id: integer
    @param compartment: integer
    @param library_id: integer
    @param status: string
    @param control: string
    @param recipe: string
    @param molarity: float
    @param operator: string
    @param notes: string
    @param compartment_consumption: float
    @param accounting_status: string
    @param accounting_date: string
    @param price_product_table: string
    @param price_discount_level: string
    @param price_user_group: string
    @param activity: integer
    @param client_access: integer
    @return: integer
    '''
    def insert_track(
                    self,
                    flowcell_id,
                    compartment,
                    library_id,
                    operator,
                    price_product_table,
                    status = 'fresh',
                    control = 'N',
                    recipe = 'SE',
                    molarity = 0,
                    notes = None,
                    compartment_consumption = 1,
                    accounting_status = 'open',
                    accounting_date = None,
                    price_discount_level = 'Basic',
                    price_user_group = 'Extern',
                    activity = 1,
                    client_access = 1):
        cursor = self.__conn.cursor()
        query = """
        INSERT INTO Tracks (FLOWCELL_ID,COMPARTMENT,LIBRARY_ID,TRACKSSTATUS_ID,CONTROL,RECIPE,MOLARITY,OPERATOR_ID,NOTES,COMPARTMENT_CONSUMPTION,
        ACCOUNTINGSTATUS_ID,ACCOUNTING_DATE,PRICE_PRODUCT_TABLE_ID,PRICE_DISCOUNT_LEVEL_ID,PRICE_USER_GROUP_ID,ACTIVITY,CLIENT_ACCESS)
        (SELECT %s AS FC_ID,%s AS COMP,%s AS LIB_ID,TracksStatus.ID AS STATUS_ID,%s AS CTRL,%s AS REC,%s AS MOL,Operators.ID AS OPER_ID,%s AS NOTE,%s AS COMP_CONSUM,
        AccountingStatus.ID AS ACCOUNT_ID,%s AS ACCOUNT_DATE,PriceProductTables.ID AS PRICE_ID,PriceDiscountLevels.ID AS DISCOUNT_ID,PriceUserGroups.ID AS USER_ID,
        %s AS ACTIV,%s AS ACCESS 
        FROM TracksStatus LEFT JOIN AccountingStatus ON AccountingStatus.NAME = %s LEFT JOIN PriceProductTables ON 
        PriceProductTables.NAME = %s LEFT JOIN PriceDiscountLevels ON PriceDiscountLevels.NAME = %s LEFT JOIN PriceUserGroups ON PriceUserGroups.NAME = %s 
        LEFT JOIN Operators ON Operators.USERNAME = %s WHERE TracksStatus.NAME = %s)
        """
        cursor.execute(query, (flowcell_id, compartment, library_id,control,recipe,molarity,notes,compartment_consumption,accounting_date,activity,client_access,
                               accounting_status,price_product_table,price_discount_level,price_user_group,operator,status))
        track_id = cursor.lastrowid
        cursor.close()
        
        return track_id

    '''
    function attaches a product specified by a name to a flowcell.
    @param flowcell_id: the flowcell id
    @param product_name: the product name
    '''    
    def attach_product_to_flowcell_by_name(self,flowcell_id,product_name):
        cursor = self.__conn.cursor()
        query = 'INSERT INTO Flowcells_Products (FLOWCELL_ID,Product_ID)(SELECT %s AS FC_ID,Products.ID AS PROD_ID FROM Products WHERE NAME = %s)'
        cursor.execute(query, (flowcell_id, product_name))
        cursor.close()

    '''
    function attaches a product specified by a id to a flowcell.
    @param flowcell_id: the flowcell id
    @param product_id: the product id
    '''  
    def attach_product_to_flowcell_by_id(self,flowcell_id,product_id):
        cursor = self.__conn.cursor()
        query = 'INSERT INTO Flowcells_Products (FLOWCELL_ID,Product_ID) VALUES (%s,%s)'
        cursor.execute(query, (flowcell_id, product_id))
        cursor.close()
        
        '''
    function returns the current price product table.
    @return: the name of the price product table
    '''  
    def query_price_product_table(self):
        cursor = self.__conn.cursor(dictionary = True)
        query = 'SELECT NAME FROM PriceProductTables WHERE CURRENT_TABLE = 1;'
        cursor.execute(query)
        resultset = cursor.fetchone()
        cursor.close()
        return resultset['NAME']
        
    