#!/usr/bin/python

import psycopg2
import time
import sys 
import sqlalchemy

sys.setrecursionlimit(16385)

from sqlalchemy.engine import result
from sqlalchemy import create_engine, MetaData, Table, Column, Numeric, Integer, VARCHAR, update, delete
from config.config import config
from resource.date import time_info

params = config()

def connect():
    return psycopg2.connect(**params)

engine = sqlalchemy.create_engine('postgresql://', creator=connect)

def writeError(erro, nameErro, result, error, tables, count):

    timestamp = time_info()

    print(erro)
    print(result)
    
    output = open('output/' + nameErro +'log', 'a')
    output.write(timestamp + erro  + error + result)
    output.close()
    
    createIndex(tables, count + 1)

def readArchive():

    document = open('input/create-3.0-shego-1.txt','r')
    lines = document.readlines()
    document.close()

    return lines

def createIndex(tables, count):

        try:

            sizeTables = len(tables)

            while count <= sizeTables:

                query = ""
                
                time.sleep(2)
                print(str(count) + " - " + (tables[count]))
                query = engine.execute(tables[count]).fetchall()
                print(query)
                
                if (str(query).find("CREATE INDEX")) > 1:

                    nameMessage = 'Indice criado.'
                    message = "SUCESSO - " + tables[count] + nameMessage + "\n\n"
                    error = ""
                    
                    writeError(message, nameMessage, error, tables, count)

                return (count, query)

        except (Exception, psycopg2.DatabaseError) as error:
                       
            error = str(error)

            if error.find("already exists") > 1:

                nameErro = 'Indice Existente.'
                erro = "ERROR - " + tables[count] + nameErro + "\n\n"
                
                writeError(erro, nameErro, query, error, tables, count)

            elif error.find("does not exist") > 1:

                nameErro = 'Tabela nao existe.'
                erro = "ERROR - " + tables[count] + nameErro + "\n\n"
                
                writeError(erro, nameErro, query, error, tables, count)
            
            elif error.find("is duplicated.") > 1:
                
                nameErro = 'Indice Duplicado.'
                erro = "ERROR - " + tables[count] + nameErro + "\n\n"
                
                writeError(erro, nameErro, query, error, tables, count)
            
            else:
                nameErro = "other.log:"
                erro = error + "\n"
                writeError(erro, nameErro, query, error, tables, count)

        finally:
            if engine is not None:
                print('Conexão finalizada.')

if __name__ == '__main__':

    tables = readArchive()
    
    createIndex(tables, 0)

    

