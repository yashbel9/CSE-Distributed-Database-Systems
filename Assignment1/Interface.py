#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    c = openconnection.cursor()
    c.execute("DROP TABLE IF EXISTS ratings")
    c.execute("CREATE TABLE if not exists " + ratingstablename +
              "(userid INT NOT NULL,temp VARCHAR, movieid INT NOT NULL,temp1 VARCHAR, rating REAL,temp2 VARCHAR, Timestamp REAL)")
    '''with open(ratingsfilepath) as f:
	for data in f:
		line = split.("::")'''
    f = open(ratingsfilepath, 'r')
    c.copy_from(f, ratingstablename, ':', columns = ('userID','temp','movieID','temp1','rating','temp2','Timestamp'))      
    c.execute("ALTER TABLE "+ratingstablename+ " DROP COLUMN temp")  
    c.execute("ALTER TABLE "+ratingstablename+ " DROP COLUMN temp1")
    c.execute("ALTER TABLE "+ratingstablename+ " DROP COLUMN temp2")
    c.execute("SELECT * from "+ratingstablename) 
    '''rows = c.fetchall()
    print "\nRows: \n"
    for row in rows:
        print "   ", row
    openconnection.commit()'''



def rangepartition(ratingstablename, numberofpartitions, openconnection):
    c = openconnection.cursor()
    gap = float(5 / numberofpartitions)
    start = float(0)
    partition_number = 0
    end = float(start + gap)
    while(partition_number < numberofpartitions):
        if partition_number == 0:
            c.execute("DROP TABLE range_part" + str(partition_number))
            c.execute("CREATE TABLE range_part"+str(partition_number)+
              " AS SELECT * FROM "+ratingstablename+" WHERE rating>="+str(start)+
             " AND rating<="+str(end)+";")
            partition_number = partition_number + 1
            start = float(start + gap)
            end = float(start + gap)
            openconnection.commit()
        else:
            c.execute("DROP TABLE range_part"+str(partition_number))
            c.execute("CREATE TABLE range_part" + str(partition_number) +
                      " AS SELECT * FROM " + ratingstablename + " WHERE rating>" + str(start) +
                      " AND rating<=" + str(end) + ";")
            partition_number = partition_number + 1
            start = float(start + gap)
            end = float(start + gap)
            openconnection.commit()
    pass


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    
    c = openconnection.cursor()
    for i in range(0, numberofpartitions):
	c.execute("DROP TABLE IF EXISTS rrobin_part"+str(i))
        c.execute(" CREATE TABLE rrobin_part" + str(i) + " (userid, movieid, rating) AS SELECT temp.userid, temp.movieid, temp.rating"
                    " FROM ( select row_number() over() as rn, userid, movieid, rating "
                    " FROM " + ratingstablename + " ) as temp "
                    " WHERE (temp.rn - 1) % "+ str(numberofpartitions) + " = "+ str(i) + " ;")
	'''c.execute("Select * from rrobin_part"+str(i))
	rows = c.fetchall()
        print "\nRows:\n"
        for row in rows:
            print "   ", row'''
    openconnection.commit()
    pass
    


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    c = openconnection.cursor()
    c.execute("SELECT table_name FROM information_schema.tables WHERE table_name like'%rrobin_part%'; ")
    allt = c.fetchall()
    c.execute("SELECT count(*) FROM "+allt[0][0])
    start = c.fetchone()[0]
    temp = start
    for table_number in range(1, len(allt)):
        c.execute("SELECT count(*) FROM "+allt[table_number][0])
        ntable = c.fetchone()[0]
        if ntable < temp:
            c.execute("INSERT INTO " + allt[table_number][0] + " (userid, movieid, rating)values ("
            +str(userid)+" , "+str(itemid)+" , "+str(rating)+")")
            break
        temp = ntable
    if table_number+1==len(allt) and temp==start:
        c.execute("INSERT INTO "+allt[0][0]+" (userid, movieid, rating) values ("
        +str(userid)+" , "+str(itemid)+" , "+str(rating)+")")
    openconnection.commit()
    pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    c = openconnection.cursor()
    c.execute("SELECT table_name FROM information_schema.tables WHERE table_name like '%range_part%'; ")
    allt = c.fetchall()
    gap = float(5) / len(allt)
    start = float(0)
    end = float(gap)
    table_number = 0
    while table_number<len(allt):
	if table_number==0:
            if rating >= start and rating <= end:
                c.execute("INSERT INTO "+allt[table_number][0]+"(userid, movieid, rating) values ( " 
                +str(userid)+" , "+str(itemid)+" , "+str(rating)+")")
       	        break
	else:
	    if rating > start and rating <= end:
                c.execute("INSERT INTO "+allt[table_number][0]+"(userid, movieid, rating) values( " 
                +str(userid)+" , "+str(itemid)+" , "+str(rating)+")")
                break
        start = float(start + gap)
        end = float(start + gap)
        table_number += 1
    c.execute("INSERT INTO "+ratingstablename+" (userid, movieid, rating) values ( " 
    +str(userid)+" , "+str(itemid)+" , "+str(rating)+")")
    openconnection.commit()
    pass

def deletepartitionsandexit(openconnection):
    pass

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()
