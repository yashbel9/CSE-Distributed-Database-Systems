#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    #Implement RangeQuery Here.
    if ratingMinValue < 0 or ratingMaxValue>5:
	print "Invalid input"
	exit()
    c = openconnection.cursor()
    c.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    allt = c.fetchall()
    f = open('RangeQueryOut.txt', 'w')
    data = []
    for table in allt:
        if 'rangeratingspart' in table[0] or 'roundrobinratingspart' in table[0]:
	    if table[0]=='rangeratingspart0':
                c.execute("SELECT * FROM "+table[0]+" WHERE rating >= "+str(ratingMinValue)+" AND rating <= " + str(ratingMaxValue)+";")
                values = c.fetchall()
	        if len(values) != 0:
                    for value in values:
                        f.write(str(table[0]) + ',' + str(value[0]) + ',' + str(value[1]) + ',' + str(value[2]) +'\n')
	    else:
		c.execute("SELECT * FROM "+table[0]+" WHERE rating > "+str(ratingMinValue)+" AND rating <= " + str(ratingMaxValue)+";")
                values = c.fetchall()
	        if len(values) != 0:
                    for value in values:
                        f.write(str(table[0]) + ',' + str(value[0]) + ',' + str(value[1]) + ',' + str(value[2]) +'\n')
    f.close() 

def PointQuery(ratingsTableName, ratingValue, openconnection):
    #Implement PointQuery Here.
    if ratingValue>5 or ratingValue<0:
	print "Invalid input"
	exit()
    c = openconnection.cursor()
    c.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    allt = c.fetchall()
    f = open('PointQueryOut.txt', 'w')
    data = []
    for table in allt:
	if 'rangeratingspart' in table[0] or 'roundrobinratingspart' in table[0]:
            c.execute("SELECT * FROM "+table[0]+" WHERE rating = "+str(ratingValue)+";")
	    #print table[0]
            values = c.fetchall()
	    if len(values) != 0:
                for value in values:
                    f.write(str(table[0]) + ',' + str(value[0]) + ',' + str(value[1]) + ',' + str(value[2]) +'\n')
    f.close() 
