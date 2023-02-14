#!/usr/bin/python

import time

def read_file_tables():

    document = open('input/tabelas-index-1.txt')
    tables = document.readlines()

    document.close

    return tables

def read_file_create_index():

    document = open('input/create-all-indices.txt')
    index = document.readlines()

    document.close

    return index

def write_file_create_index(index):
    
    output = open('output/create-1-shego.txt', 'a')
    output.write(index)

    output.close()

def search_array(count):

    tables = read_file_tables()

    indexing = read_file_create_index()

    i = 0

    while i < len(indexing):
        
        table = (tables[count]).strip()
        index = indexing[i]
        
        if index.find(str(table)) > 1:
                print(index)
                write_file_create_index(index)
        
        i = i + 1

if __name__ == '__main__':

    count = 0
    limit = len(read_file_tables())

    while count < limit:
        time.sleep(1)
        search_array(count)

        count = count + 1

    print(limit)

