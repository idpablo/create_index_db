#!/usr/bin/python

from doctest import OutputChecker
import psycopg2
import time
from sqlalchemy.engine import result
import sqlalchemy
from sqlalchemy import create_engine, MetaData,\
Table, Column, Numeric, Integer, VARCHAR, update, delete

from config.config import config

params = config()

def connect():
    return psycopg2.connect(**params)

engine = sqlalchemy.create_engine('postgresql://', creator=connect)

def writeError(erro, nameErro, tables, count):

    print(erro)
    
    output = open('output/' + nameErro +'.log', 'a')
    output.write(erro)

    output.close()
    
    createIndex(tables, count + 1)


def readArchive():

    document = open('input/create-1-shego-4.txt','r')
    lines = document.readlines()

    return lines

def createIndex(tables, count):

        try:

            sizeTables = len(tables)

            while count <= sizeTables:
                
                time.sleep(2)
                print(str(count) + " - " + (tables[count]))
                query = engine.execute(tables[count]).fetchall()
                
                if (str(query).find("CREATE INDEX")) > 1:

                    nameMessage = 'Indice criado.'
                    message = "SUCESSO - " + tables[count] + nameMessage + "\n\n"
                    
                    writeError(message, nameMessage, tables, count)

                return count 

        except (Exception, psycopg2.DatabaseError) as error:
            
                       
            error = str(error)

            if error.find("already exists") > 1:

                nameErro = 'Indice Existente.'
                erro = "ERROR - " + tables[count] + nameErro + "\n\n"
                
                writeError(erro, nameErro, tables, count)

            elif error.find("does not exist") > 1:

                nameErro = 'Tabela nao existe.'
                erro = "ERROR - " + tables[count] + nameErro + "\n\n"
                
                writeError(erro, nameErro, tables, count)
            
            elif error.find("is duplicated.") > 1:
                
                nameErro = 'Indice Duplicado.'
                erro = "ERROR - " + tables[count] + nameErro + "\n\n"
                
                writeError(erro, nameErro, tables, count)
            
            else:
                nameErro = "other.log:"
                erro = error + "\n"
                writeError(erro, nameErro, tables, count)

        finally:
            if engine is not None:
                print('Conexão finalizada.')


if __name__ == '__main__':

    tables = readArchive()
    
    createIndex(tables, 489)

    

