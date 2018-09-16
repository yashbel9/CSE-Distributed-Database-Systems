#!/usr/bin/python2.7
#
# Interface for the assignement
#
DATABASE_NAME = 'dds_assgn1'

# TODO: Change these as per your code
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'test_data.dat'
ACTUAL_ROWS_IN_INPUT_FILE = 20  # Number of lines in the input file

import psycopg2
import datetime
import time
import Interface as MyAssignment

# SETUP Functions
def createdb(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection()
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named "{0}" already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# ##############

# Utilities
def handleerror(message):
    print('\nE: {0} {1}'.format(getformattedtime(time.time()), message))


def getformattedtime(srctime):
    return datetime.datetime.fromtimestamp(srctime).strftime('%Y-%m-%d %H:%M:%S')


def formattedprint(message, newlineafter=False):
    if newlineafter:
        print("T: {0} {1}\n".format(getformattedtime(time.time()), message))
    else:
        print("T: {0} {1}".format(getformattedtime(time.time()), message))


# ##############

# Decorators
def timeme(func):
    def timeme_and_call(*args, **kwargs):
        tic = time.time()
        res = func(*args, **kwargs)
        toc = time.time()
        formattedprint('Took %2.5fs for "%r()"' % (toc - tic, func.__name__))
        return res

    return timeme_and_call


class LogMe(object):
    def __init__(self, *args, **kwargs):
        self.message = args[0]
        pass

    def __call__(self, func):
        def wrapped_func(*args, **kwargs):
            formattedprint(self.message)
            res = func(*args, **kwargs)
            return res

        return wrapped_func


def testme(func):
    def testme_and_call(*args, **kwargs):
        try:
            res = func(*args, **kwargs)
            formattedprint('Test passed!', True)
        except Exception as e:
            formattedprint('Test failed :( Error: {0}'.format(e), True)
            return False
        return res

    return testme_and_call


# ##########


# Helpers for Tester functions
def checkpartitioncount(cursor, expectedpartitions, prefix):
    cursor.execute(
        "SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '{0}%';".format(
            prefix))
    count = int(cursor.fetchone()[0])
    if count != expectedpartitions:  raise Exception(
        'Range partitioning not done properly. Excepted {0} table(s) but found {1} table(s)'.format(
            expectedpartitions,
            count))


def totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex):
    selects = []
    for i in range(partitionstartindex, n + partitionstartindex):
        selects.append('SELECT * FROM {0}{1}'.format(rangepartitiontableprefix, i))
    cur.execute('SELECT COUNT(*) FROM ({0}) AS T'.format(' UNION ALL '.join(selects)))
    count = int(cur.fetchone()[0])
    return count


def testrangeandrobinpartitioning(n, openconnection, rangepartitiontableprefix, partitionstartindex):
    with openconnection.cursor() as cur:
        if not isinstance(n, int) or n < 0:
            # Test 1: Check the number of tables created, if 'n' is invalid
            checkpartitioncount(cur, 0, rangepartitiontableprefix)
        else:
            # Test 2: Check the number of tables created, if all args are correct
            checkpartitioncount(cur, n, rangepartitiontableprefix)

            # Test 3: Test Completeness by SQL UNION ALL Magic
            count = totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex)
            if count < ACTUAL_ROWS_IN_INPUT_FILE: raise Exception(
                "Completeness property of Range Partitioning failed. Excpected {0} rows after merging all tables, but found {1} rows".format(
                    ACTUAL_ROWS_IN_INPUT_FILE, count))

            # Test 4: Test Disjointness by SQL UNION Magic
            count = totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex)
            if count > ACTUAL_ROWS_IN_INPUT_FILE: raise Exception(
                "Dijointness property of Range Partitioning failed. Excpected {0} rows after merging all tables, but found {1} rows".format(
                    ACTUAL_ROWS_IN_INPUT_FILE, count))

            # Test 5: Test Reconstruction by SQL UNION Magic
            count = totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex)
            if count != ACTUAL_ROWS_IN_INPUT_FILE: raise Exception(
                "Rescontruction property of Range Partitioning failed. Excpected {0} rows after merging all tables, but found {1} rows".format(
                    ACTUAL_ROWS_IN_INPUT_FILE, count))


def testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
    with openconnection.cursor() as cur:
        cur.execute(
            'SELECT COUNT(*) FROM {0} WHERE {4} = {1} AND {5} = {2} AND {6} = {3}'.format(expectedtablename, userid,
                                                                                          itemid, rating,
                                                                                          USER_ID_COLNAME,
                                                                                          MOVIE_ID_COLNAME,
                                                                                          RATING_COLNAME))
        count = int(cur.fetchone()[0])
        if count != 1:  return False
        return True


# ##########


# Testers
@LogMe('Testing LoadingRating()')
@testme
@timeme
def testloadratings(ratingstablename, filepath, openconnection, rowsininpfile):
    """
    Tests the load ratings function
    :param ratingstablename: Argument for function to be tested
    :param filepath: Argument for function to be tested
    :param openconnection: Argument for function to be tested
    :param rowsininpfile: Number of rows in the input file provided for assertion
    :return:Raises exception if any test fails
    """
    MyAssignment.loadratings(ratingstablename, filepath, openconnection)
    # Test 1: Count the number of rows inserted
    with openconnection.cursor() as cur:
        cur.execute('SELECT COUNT(*) from {0}'.format(RATINGS_TABLE))
        count = int(cur.fetchone()[0])
        if count != rowsininpfile:
            raise Exception(
                'Expected {0} rows, but {1} rows in \'{2}\' table'.format(rowsininpfile, count, RATINGS_TABLE))


@LogMe('Testing RangePartition()')
@testme
@timeme
def testrangepartition(ratingstablename, n, openconnection, rangepartitiontableprefix, partitionstartindex):
    """
    Tests the range partition function for Completness, Disjointness and Reconstruction
    :param ratingstablename: Argument for function to be tested
    :param n: Argument for function to be tested
    :param openconnection: Argument for function to be tested
    :param rangepartitiontableprefix: This function assumes that you tables are named in an order. Eg: range_part0, range_part1...
    :param partitionstartindex: Indicates how the table names are indexed. Do they start as rangepart1, 2 ... or rangepart0, 1, 2...
    :return:Raises exception if any test fails
    """

    try:
        MyAssignment.rangepartition(ratingstablename, n, openconnection)
    except Exception:
        # ignore any exceptions raised by function
        pass
    testrangeandrobinpartitioning(n, openconnection, rangepartitiontableprefix, partitionstartindex)


@LogMe('Testing RoundRobinPartition()')
@testme
@timeme
def testroundrobinpartition(ratingstablename, numberofpartitions, openconnection, robinpartitiontableprefix,
                            partitionstartindex):
    """
    Tests the round robin partitioning for Completness, Disjointness and Reconstruction
    :param ratingstablename: Argument for function to be tested
    :param numberofpartitions: Argument for function to be tested
    :param openconnection: Argument for function to be tested
    :param robinpartitiontableprefix: This function assumes that you tables are named in an order. Eg: robinpart1, robinpart2...
    :param partitionstartindex: Indicates how the table names are indexed. Do they start as robinpart1, 2 ... or robinpart0, 1, 2...
    :return:Raises exception if any test fails
    """
    try:
        MyAssignment.roundrobinpartition(ratingstablename, numberofpartitions, openconnection)
    except Exception:
        # ignore any exceptions raised by function
        pass
    testrangeandrobinpartitioning(numberofpartitions, openconnection, robinpartitiontableprefix, partitionstartindex)


@LogMe('Testing RoundRobinInsert()')
@testme
@timeme
def testroundrobininsert(ratingstablename, userid, itemid, rating, openconnection, expectedtablename):
    """
    Tests the roundrobin insert function by checking whether the tuple is inserted in he Expected table you provide
    :param ratingstablename: Argument for function to be tested
    :param userid: Argument for function to be tested
    :param itemid: Argument for function to be tested
    :param rating: Argument for function to be tested
    :param openconnection: Argument for function to be tested
    :param expectedtablename: The expected table to which the record has to be saved
    :return:Raises exception if any test fails
    """
    try:
        MyAssignment.roundrobininsert(ratingstablename, userid, itemid, rating, openconnection)
    except Exception:
        # ignore any exceptions raised by function
        pass
    if not testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
        raise Exception(
            'Round robin insert failed! Couldnt find ({0}, {1}, {2}) tuple in {3} table'.format(userid, itemid, rating,
                                                                                                expectedtablename))


@LogMe('Testing RangeInsert()')
@testme
@timeme
def testrangeinsert(ratingstablename, userid, itemid, rating, openconnection, expectedtablename):
    """
    Tests the range insert function by checking whether the tuple is inserted in he Expected table you provide
    :param ratingstablename: Argument for function to be tested
    :param userid: Argument for function to be tested
    :param itemid: Argument for function to be tested
    :param rating: Argument for function to be tested
    :param openconnection: Argument for function to be tested
    :param expectedtablename: The expected table to which the record has to be saved
    :return:Raises exception if any test fails
    """
    try:
        MyAssignment.rangeinsert(ratingstablename, userid, itemid, rating, openconnection)
    except Exception:
        # ignore any exceptions raised by function
        pass
    if not testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
        raise Exception(
            'Range insert failed! Couldnt find ({0}, {1}, {2}) tuple in {3} table'.format(userid, itemid, rating,
                                                                                          expectedtablename))


@LogMe('Deleting all testing tables using your own function')
def testdelete(openconnection):
    # Not testing this piece!!!
    MyAssignment.deletepartitionsandexit(openconnection)


if __name__ == '__main__':
    try:
        createdb(DATABASE_NAME)

        with getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            testloadratings(RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE)

            testrangepartition(RATINGS_TABLE, 5, conn, RANGE_TABLE_PREFIX, 0)

            testroundrobinpartition(RATINGS_TABLE, 5, conn, RROBIN_TABLE_PREFIX, 0)

            testroundrobininsert(RATINGS_TABLE, 100, 1, 3, conn, RROBIN_TABLE_PREFIX + '0')
            # testroundrobininsert(RATINGS_TABLE, 100, 1, 3, conn, RROBIN_TABLE_PREFIX + '1')
            # testroundrobininsert(RATINGS_TABLE, 100, 1, 3, conn, RROBIN_TABLE_PREFIX + '2')

            # ALERT:: Use only one at a time i.e. uncomment only one line at a time and run the script
            testrangeinsert(RATINGS_TABLE, 100, 2, 3, conn, RANGE_TABLE_PREFIX + '2')
            # testrangeinsert(RATINGS_TABLE, 100, 2, 0, conn, RANGE_TABLE_PREFIX + '0')

            choice = raw_input('Press enter to Delete all tables? ')
            if choice == '':
                testdelete(conn)

    except Exception as detail:
        handleerror(detail)
        print detail
