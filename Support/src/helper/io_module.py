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
from pathlib import Path

from os import listdir

from os.path import abspath
from os.path import isfile
from os.path import isdir
from os.path import sep
from os.path import join as path_join
from os.path import exists

from os import environ

'''
  Method appends the separator of the os at the end of the string
  @param inputstring: string
  @return: string
'''
def add_separator(inputstring):
    if inputstring[-1] != sep:
        inputstring += sep
    return inputstring


'''
  Method changes the inputstring to an absolute path and appends
  the os separator if necessary
  @param dirname: string
  @return: string
'''
def get_absolute_path(name, add = False):
    if name == '~':
        name = environ['HOME']
    elif name[0] == '~':
        name = name.replace(name[0], environ['HOME'])
    else:
        name = abspath(name)

    if add: name = add_separator(name)
    
    return name

'''
  Method checks if the file exists
  @param filename: string
  @return: string
'''
def check_file(filename):
    if isfile(filename): return get_absolute_path(filename)
    return ''

'''
  Method checks if the directory exists
  @param dirname: string
  @return: string
'''
def check_directory(dirname):
    if isdir(dirname): return get_absolute_path(dirname)
    return ''

'''
  Method creates a directory. Returns True if succeeded or directory exists, otherwise False.
  @param dirname: string
  @return: boolean
'''
def create_directory(dirname):
    Path(get_absolute_path(dirname)).mkdir(mode = 0o770, parents = True, exist_ok = True)
    

'''
  Given a path, method lists all subdirectories.
  @param : string
  @return: list of string
'''
def list_subdirectories(dirname):
    dirname = get_absolute_path(dirname)
    subdirectories = []
    entries = listdir(dirname)
    for e in entries:
        sd = check_directory(path_join(dirname,e))
        if sd:
            subdirectories.append(sd)
    return subdirectories
            
'''
  Checks if a directory is archived (or to be archived).
  @param dirname: string
  @return: boolean
'''
def is_archived_directory(dirname):
    dirname = get_absolute_path(dirname)
    return '_archived' in dirname or exists(path_join(dirname,"toarchive.txt")) or exists(path_join(dirname,"topbarchive.txt")) or exists(path_join(dirname,"ARCHIVED.txt"))
