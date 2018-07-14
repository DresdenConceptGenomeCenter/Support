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

class Machine(object):
    def __init__(self, name, code, cmcbstorage, zihstorage, platform, dbid):
        self.__name = name
        self.__code = code
        self.__cmcbstorage = cmcbstorage
        self.__zihstorage = zihstorage
        self.__platform = platform
        self.__dbid = dbid
        
        self.__logger = logging.getLogger('support.machine')

        if self.__name.find('Nextseq') or self.__name.find('Novaseq'):
            self.__reverse_complement = True
        else:
            self.__reverse_complement = False

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
    
    def get_rawstorage_path(self, location):
        if location == 'cmcb': return self.__cmcbstorage
        else: return self.__zihstorage

    def set_name(self, name):
        self.__name = name
    
    def set_code(self, code):
        self.__code = code
    
    def set_cmcbstorage(self, cmcbstorage):
        self.__cmcbstorage = cmcbstorage
    
    def set_zihstorage(self, zihstorage):
        self.__zihstorage = zihstorage
    
    def set_reverse_complement(self, rc):
        self.__reverse_complement = rc
    
    def get_name(self):
        return self.__name
    
    def get_code(self):
        return self.__code
    
    def get_cmcbstorage(self):
        return self.__cmcbstorage

    def get_zihstorage(self):
        return self.__zihstorage

    def get_reverse_complement(self):
        return self.__reverse_complement

    def get_platform(self):
        return self.__platform
    
    def get_dbid(self):
        return self.__dbid
 
    name = property(get_name, set_name)
    code = property(get_code, set_code)
    cmcbstorage = property(get_cmcbstorage, set_cmcbstorage)
    zihstorage = property(get_zihstorage, set_zihstorage)
    reverse_complement = property(get_reverse_complement, set_reverse_complement)
    platform = property(get_platform)
    dbid = property(get_dbid)
    
