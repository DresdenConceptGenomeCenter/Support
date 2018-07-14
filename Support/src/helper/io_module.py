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

from gzip import open as gzipopen

from pathlib import Path

from os import environ
from os import listdir

from os.path import abspath
from os.path import isfile
from os.path import isdir
from os.path import sep
from os.path import join as pathjoin
from os.path import exists

'''
    Method produces the reverse complement of a nucleotide sequence
    @param inputstring: string
    @return: string 
'''
def get_reverse_complement(inputstring):
    intab = 'ACGTNacgtn'
    outtab = 'TGCANtgcan'
    transtab = str.maketrans(intab, outtab)
    return inputstring[::-1].translate(transtab)

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
  @param filename: string
  @return: string
'''
def check_directory(dirpath):
    if isdir(dirpath): return get_absolute_path(dirpath)
    return ''

'''
  Method creates a directory.
  @param dirname: string
  @return: boolean
'''
def create_directory(dirname):
    Path(get_absolute_path(dirname)).mkdir(mode = 0o770, parents = True, exist_ok = True)

'''
  Give a list of possible files, method checks if files exist
  @param filelist: possible list of files
  @return list (correct), list (incorrect) 
'''
def check_fileslist(filelist):
    correct, incorrect = [], []
    for name in filelist:
        name = get_absolute_path(name)
        if check_file(name) == '':
            incorrect.append(name)
        else:
            correct.append(name)
    return correct, incorrect

'''
  method adds files in a directory to a list, if they have certain suffixes
  @param directory: string
  @param ext: tuple of strings
  @return: list of filepaths
'''
def add_files_to_list(directory, ext):
    temp = listdir(directory)
    returnfiles = []
    if len(ext) == 0:
        for name in temp:
            filename = pathjoin(directory, name)
            if check_file(filename) != '': returnfiles.append(filename)
        return returnfiles
    for name in temp:
        for suffix in ext:
            if name.endswith(suffix):
                filename = pathjoin(directory, name)
            if check_file(filename) != '':
                returnfiles.append(filename)
                break
    return returnfiles

'''
  Method goes recursively through a directory and add files 
  in a directory to a list, if they have certain suffixes.
  @param directory: string
  @param returnfiles: list
  @param ext: tuple
  @return: list of files
'''
def add_files_to_list_recursive(directory, returnfiles, ext):
    returnfiles.extend(add_files_to_list(directory, ext))
    tempdirectories = ['{0}{1}'.format(directory, i) for i in listdir(add_separator(directory)) if isdir('{0}{1}'.format(directory, i))]
    for dirname in tempdirectories:
        returnfiles = add_files_to_list_recursive(add_separator(dirname), returnfiles, ext)
    return returnfiles

'''
  Method goes recursively through a directory up to a certain depth and add files 
  in a directory to a list, if they have certain suffixes.
  @param directory: string
  @param returnfiles: list
  @param ext: tuple
  @param count: int  
  @param depth: int
  @return: list of files
'''
def add_files_to_list_recursive_depth(directory, returnfiles, ext, count, depth = 2):
    returnfiles.extend(add_files_to_list(directory, ext))
    if count == depth: return returnfiles
    tempdirectories = ['{0}{1}'.format(directory, i) for i in listdir(add_separator(directory)) if isdir('{0}{1}'.format(directory, i))]
    for dirname in tempdirectories:
        returnfiles = add_files_to_list_recursive_depth(add_separator(dirname), returnfiles, ext, count + 1, depth)
    return returnfiles

'''
  Method add directories in a directory to a list
  @param directory: string
'''
def add_directories_to_list(directory):
    temp = listdir(directory)
    returndirs = []
    for name in temp:
        name = pathjoin(directory, name)
        if check_directory(name) != '': returndirs.append(name)
    return returndirs

'''
  Method goes recursively through a directory and add directories
  to a list. Once depth is reached it stops.
  @param directory: string
  @param returndirs: list
  @param count: int  
  @param depth: int
'''
def add_directories_to_list_recursive(directory, returndirs, count, depth = 2):
    returndirs.extend(add_directories_to_list(directory))
    if depth == count: return returndirs
    tempdirectories = [pathjoin(directory, i) for i in listdir(add_separator(directory)) if isdir(pathjoin(directory, i))]
    for dirname in tempdirectories:
        returndirs = add_directories_to_list_recursive(add_separator(dirname), returndirs, count + 1, depth)
    return returndirs


'''
STANDARD READ WRITE FUNCTIONS
'''
def get_fileobject(filename, attr):
    if filename.endswith('gz'):
        return gzipopen(filename, '{0}b'.format(attr))
    return open(filename, attr)
 
def write_list(writelist, filename, attr = 'w'):
    with get_fileobject(filename, attr) as fileout:
        fileout.writelines(writelist)
 
def write_string(writestring, filename, attr):
    with get_fileobject(filename, attr) as fileout:
        fileout.write(writestring)
 
def read_file_get_string(filename, attr='r'):
    with get_fileobject(filename, attr) as filein:
        temp = filein.readlines()
    return ''.join(temp)
 
def read_file_get_list(filename, attr='r'):
    with get_fileobject(filename, attr) as filein:
        temp = filein.readlines()
    temp = [i.rstrip('\n') for i in temp]
    return temp

def read_file_get_list_with_sep(filename, sep, attr='r'):
    temp = []
    with get_fileobject(filename, attr) as filein:
        for line in filein:
            temp.append(line.rstrip('\n').split(sep))
    return temp

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
        subdirname = check_directory(pathjoin(dirname,e))
        if subdirname != '' : subdirectories.append(subdirname)
    return subdirectories
            
'''
  Checks if a directory is archived (or to be archived).
  @param dirname: string
  @return: boolean
'''
def is_archived_directory(dirname):
    dirname = get_absolute_path(dirname)
    return '_archived' in dirname or exists(pathjoin(dirname,"toarchive.txt")) or exists(pathjoin(dirname,"topbarchive.txt")) or exists(pathjoin(dirname,"ARCHIVED.txt"))

