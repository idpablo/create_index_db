#!/usr/bin/python

import datetime

def time_info():
    date = datetime.datetime.now()
    
    timestamp = date.strftime("%d-%m-%Y %H:%M:%S - ")

    return timestamp

time_info()