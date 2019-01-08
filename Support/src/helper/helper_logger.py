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


class MainLogger(object):
    def __init__(self, logtitle, streamh = True, fileh = False, logfilename = '', clogstr = 'DEBUG', flogstr = 'DEBUG'):
        self.__logfilename = logfilename
        self.__ch = []
        self.__streamh = streamh
        self.__fh = []
        self.__fileh = fileh
        self.set_logger(logtitle, streamh, fileh, clogstr, flogstr)

    def set_logger(self, logtitle, streamh, fileh, chlogstring, fhlogstring):
        self.logger = logging.getLogger(logtitle)
        self.logger.setLevel('DEBUG')
        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt= '%m/%d/%Y %I:%M:%S %p')

        if fileh:
            fh = logging.FileHandler(self.__logfilename)
            fh.setLevel(fhlogstring)
            fh.setFormatter(self.formatter)
            self.logger.addHandler(fh)
            self.__fh.append(fh)

        if streamh:
            ch = logging.StreamHandler()
            ch.setLevel(chlogstring)
            ch.setFormatter(self.formatter)
            self.logger.addHandler(ch)
            self.__ch.append(ch)

    def add_filelogger(self, filename, fhlogstring):
        self.__logfilename = filename
        fh = logging.FileHandler(self.__logfilename)
        fh.setLevel(fhlogstring)
        fh.setFormatter(self.formatter)
        self.logger.addHandler(fh)
        self.__fh.append(fh)
        self.__fileh = True
        
    def add_streamlogger(self, clogstr):
        ch = logging.StreamHandler()
        ch.setLevel(clogstr)
        ch.setFormatter(self.formatter)
        self.logger.addHandler(ch)
        self.__ch.append(ch)
        self.__streamh = True
    
    @staticmethod
    def show_log(logger, level, message):
        if level == 'debug':
            logger.debug(message)
        elif level == 'info':
            logger.info(message)
        elif level == 'warning':
            logger.warning(message)
        elif level == 'error':
            logger.error(message)
        elif level == 'critical':
            logger.critical(message)

    def get_logger(self):
        return self.logger
    
    def close(self):
        for i in self.__fh:
            self.logger.removeHandler(i)
        for i in self.__ch:
            self.logger.removeHandler(i)

    def get_fileh(self):
        return self.__fileh

    fileh = property(get_fileh)
