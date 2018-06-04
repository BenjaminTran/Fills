import pytz
import math
import datetime
import dateutil.parser
import sys
import json
import cx_Oracle
import dbms
# sys.path.append('..')
import connect
import fillStats
from dateutil.relativedelta import *

#
# Functions to check if updated to db is needed and execute if so
#

# Dictionary to convert SummaryField names to attributes returned from DB YEARLY_LUMINOSITY table
FillRequest2DBtableDict = {
        'peak_lumi'           : 'peaklumi',
        'bunches_colliding'   : 'maxbunches',
        'delivered_lumi'      : 'maxlumifill',
        'peak_pileup'         : 'maxpileup',
        'fastest_turnaround'  : 'fastest_beam_turnaround_hours',
        'efficiency_lumi'     : 'bestefficiencyfill_percent',
        'recorded_lumi'       : 'maxlumirecordedfill',
        'longest_stable_beam' : 'longestfill_hours',
        'maxlumiday' : 'maxlumiday',
        'maxlumiweek' : 'maxlumiweek',
        'maxlumimonth' : 'maxlumimonth',
        'longestday_hours' : 'longestday_hours',
        'longestweek_hours' : 'longestweek_hours',
        'longestmonth_hours' : 'longestmonth_hours',
        'maxlumirecordedday'  : 'maxlumirecordedday',
        'maxlumirecordedweek' : 'maxlumirecordedweek',
        'maxlumirecordedmonth': 'maxlumirecordedmonth',
        'maxlumiday_eff'      : 'maxlumirecordedday_eff',
        'maxlumiweek_eff'     : 'maxlumirecordedweek_eff',
        'maxlumimonth_eff'    : 'maxlumirecordedmonth_eff'}

# def updateDBvalue(cursor, value, fillNum, date, rowID, tableName, columnName):
def updateDBvalue(cursor, fillDict, rowID, tableName, columnName):
    """
    update DB values. rowID is a dictionary containing information needed to identify
    the row (year, weeknumber, daynumber, collision type, etc.)
    """
    if(columnName == 'longestday_hours'):
        columnName = 'longestday'
    elif(columnName == 'longestweek_hours'):
        columnName = 'longestweek'
    elif(columnName == 'longestmonth_hours'):
        columnName = 'longestmonth'

    # Does not have datetime fields and has two fill fields
    if(columnName == 'fastest_beam_turnaround_hours'):
        updateColumns = [columnName, columnName + '_fill1',  columnName + '_fill2']
        prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + "=:1, " + updateColumns[1] + "=:2, " + updateColumns[2] + "=:3 WHERE year =:4 AND runtime_type_id =:5"
        cursor.execute(prompt, (fillDict['value'], fillDict['prev_fill_number'], fillDict['fill_number'], rowID['year'], rowID['runtime_type_id']))
    elif(columnName == 'longestfill_hours'): # Does not have fill field or datetime field
        prompt = "UPDATE " + tableName + " SET " + columnName + "= :1 WHERE year = :2  AND runtime_type_id = :3"
        cursor.execute(prompt,(fillDict['value'],rowID['year'],rowID['runtime_type_id']))
    elif(columnName == 'peaklumi' or columnName == 'maxbunches'): # has both fill and datetime fields
        updateColumns = [columnName, columnName + '_fill', columnName + '_time']
        prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + " = :1, " + updateColumns[1] + " = :2, " + updateColumns[2] + " = :3 WHERE year = :4 AND runtime_type_id = :5"
        cursor.execute(prompt, (fillDict['value'], fillDict['fill_number'], fillDict['start_stable_beam'], rowID['year'], rowID['runtime_type_id']))
    elif(columnName == 'maxlumiday' or columnName == 'maxlumirecordedday'): # time interval values
        updateColumns = [columnName, columnName + '_day']
        prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + "=:1," + updateColumns[1] + "=:2" + " WHERE year =:3 AND runtime_type_id =:4 "
        cursor.execute(prompt, (fillDict['value'], fillDict['Num'], rowID['year'], rowID['runtime_type_id']))
    elif(columnName == 'maxlumiweek' or columnName == 'maxlumirecordedweek'): # time interval values
        updateColumns = [columnName, columnName + '_week']
        prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + "=:1, " + updateColumns[1] + "=:2" + " WHERE year =:3 AND runtime_type_id =:4"
        cursor.execute(prompt, (fillDict['value'], fillDict['Num'], rowID['year'], rowID['runtime_type_id']))
    elif(columnName == 'maxlumimonth' or columnName == 'maxlumirecordedmonth'): # time interval values
        updateColumns = [columnName, columnName + '_month']
        prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + "=:1," + updateColumns[1] + "=:2" + " WHERE year =:3 AND runtime_type_id =:4 "
        cursor.execute(prompt, (fillDict['value'], fillDict['Num'], rowID['year'], rowID['runtime_type_id']))
    elif(columnName == 'longestday' or columnName == 'longestweek' or columnName == 'longestmonth'): # day interval values
        updateColumns = [columnName, columnName + '_hours']
        prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + "=:1, " + updateColumns[1] + "=:2" + " WHERE year =:3 AND runtime_type_id =:4 "
        cursor.execute(prompt, (fillDict['Num'], fillDict['value'], rowID['year'], rowID['runtime_type_id']))
    elif(columnName == 'maxlumirecordedday_eff' or columnName == 'maxlumirecordedweek_eff' or columnName == 'maxlumirecordedmonth_eff'):
        prompt = "UPDATE " + tableName + " SET " + columnName + "=:1 WHERE year =:2 AND runtime_type_id =:3"
        prompt_print = prompt
        cursor.execute(prompt, (fillDict['value'],rowID['year'],rowID['runtime_type_id']))
    elif(columnName == 'maxlumirecordedfill'):
        updateColumns = [columnName, columnName + '_fill', 'maxlumirecordedfill_eff']
        prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + "=:1, " + updateColumns[1] + "=:2, " + updateColumns[2] + "=:3 WHERE year =:3 AND runtime_type_id =:4"
        cursor.execute(prompt, (fillDict['value'], fillDict['fill_number'], fillDict['efficiency_lumi'], rowID['year'], rowID['runtime_type_id']))
    else: # Does not have datetime field
        updateColumns = [columnName, columnName + '_fill']
        prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + "=:1, " + updateColumns[1] + "=:2 WHERE year =:3 AND runtime_type_id =:4"
        cursor.execute(prompt, (fillDict['value'], fillDict['fill_number'], rowID['year'], rowID['runtime_type_id']))

def insertDBvalue(cursor, fillDict, rowID, tableName):
    """
    insert DB values. rowID is a dictionary containing information needed to identify
    the row (year, weeknumber, daynumber, collision type, etc.)
    """
    values = {}
    for valueDict in fillDict:
        values[valueDict['field']] = valueDict['value']
    prompt = ""
    if(tableName == "CMS_RUNTIME_LOGGER.DAILY_LUMINOSITY" or tableName == "CMS_RUNTIME_LOGGER.DAILY_LUMINOSITY_77"):
        prompt = "INSERT INTO " + tableName + " (year, day, runtime_type_id, peaklumi, recorded, delivered, lastupdate, duration) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(prompt, (rowID['year'],rowID['intervalID'],rowID['runtime_type_id'],valueDict['peak_lumi'],valueDict['recorded_lumi'],valueDict['delivered_lumi'],datetime.datetime.utcnow(),valueDict['longest_stable_beam']))
    elif(tableName == "CMS_RUNTIME_LOGGER.WEEKLY_LUMINOSITY" or tableName == "CMS_RUNTIME_LOGGER.WEEKLY_LUMINOSITY_WT"):
        prompt = "INSERT INTO " + tableName + " (year, week, runtime_type_id, peaklumi, recorded, delivered, lastupdate) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(prompt, (rowID['year'],rowID['intervalID'],rowID['runtime_type_id'],valueDict['peak_lumi'],valueDict['recorded_lumi'],valueDict['delivered_lumi'],datetime.datetime.utcnow()))

def str2seconds(duration):
    """
    convert string hh:mm:ss to seconds of type int
    """
    timeParts = duration.split(':')
    seconds = int(timeParts[0])*3600 + int(timeParts[1])*60 + int(timeParts[2])
    return int(seconds)

def isEquivalent(item1, item2, tolerance):
    """
    compare two items up to a tolerance
    """
    if(item1 == None or item2 == None):
        return
    item1_tmp = item1;
    item2_tmp = item2;

    if(isinstance(item1, str)):
        item1_tmp = str2seconds(item1)
    if(isinstance(item2,str)):
        item2_tmp = str2seconds(item2)
    if(isinstance(item1_tmp, int) and isinstance(item2_tmp, int)):
        # tolerance of 3 seconds to account for
        # rounded values in DB
        return abs(item1_tmp - item2_tmp) < 3
    return abs(item1_tmp - item2_tmp) < tolerance*max(item1_tmp,item2_tmp)


def checkFillEnd(fillData = None, interval = 5):
    """
    check if the current fill has ended, interval in minutes
    """
    currentTime = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
    latestEndTime = currentTime + datetime.timedelta(0,60)
    mostRecentFillNum = 0
    isEndTimeEmpty = False

    for dict1 in fillData['data']:
        dict2 = dict1['attributes']
        if(dict2['fill_number'] > mostRecentFillNum):
            mostRecentFillNum = dict2['fill_number']
            if(dict2['end_time'] == None):
                print "end_time is NoneType for most recent fill"
                #reset latest end time
                latestEndTime = currentTime + datetime.timedelta(0,60)
                continue
            else:
                latestEndTime = dateutil.parser.parse(dict2['end_time'])

    print "Most recent Fill Number is " + str(mostRecentFillNum)
    if(currentTime - latestEndTime > datetime.timedelta(minutes = interval)):
        return True
    else:
        print "Most recent Fill has not ended"
        return False

def getRuntimeTypeID(collType):
    """
    get numerical value for runtime type id from string
    """
    runtime_type_id = 0
    if(collType == "PROTONS"):
        runtime_type_id = 1
    elif(collType == "PROTONS_PB"):
        runtime_type_id = 21
    elif(collType == "PB"):
        runtime_type_id = 2
    else:
        print "Enter a valid collision name. No update was performed."

    return runtime_type_id

def getTableData(cursor, tableName, year, collType, intervalType, intervalID):
    """
    get data from specified table. If intervalID = 0 then get from YEARLY_LUMINOSITY
    """
    runtime_type_id = getRuntimeTypeID(collType)
    if(intervalType == 'yearly'):
        cursor.execute('select * from ' + tableName + ' where year = ' + str(year) + ' and runtime_type_id = ' + str(runtime_type_id))
    elif(intervalType == 'weekly'):
        cursor.execute('select * from ' + tableName + ' where year = ' + str(year) + ' and runtime_type_id = ' + str(runtime_type_id) + ' and week = ' + str(intervalID))
    elif(intervalType == 'daily'):
        cursor.execute('select * from ' + tableName + ' where year = ' + str(year) + ' and runtime_type_id = ' + str(runtime_type_id) + ' and day = ' + str(intervalID))
    # elif(intervalType == 'daily77'):
        # cursor.execute('select * from ' + tableName + ' where year = ' + str(year) + ' and runtime_type_id = ' + str(runtime_type_id) + ' and day = ' + str(intervalID))
    else:
        print "invalid intervalType."
        return

    Summary = cursor.fetchone()
    if(Summary == None):
        print "no Summary"
        return
    print "="*80
    print Summary
    print "="*80
    return Summary

def checkDBvalues(fillSummary,summary):
    """
    Check Database values with calculated values to see if update is needed.
    Returns list of tuples containing database parameter names that need to
    be updated and the dictionary containing the data.
    """
    updateList = []
    if(summary == None):
        print "Summary is empty, no values to check"
        return
    # cursor = db.cursor()
    for newDataDict in fillSummary:
        attributeName = FillRequest2DBtableDict[newDataDict['field']]
        DBvalue = summary[attributeName]
        fillValue = newDataDict['value']
        if(DBvalue is None or fillValue is None):
            updateList.append((newDataDict,attributeName))
            # updateDBvalue(cursor, newDataDict, year, runtime_type_id, table_row, attributeName)
            continue
        if('_hours' in attributeName):
            #int() to ensure the values are integer seconds
            DBvalue = int(summary[attributeName]*3600)
            if(fillValue == ''):
                fillValue = 0
            else:
                fillValue = int(newDataDict['value']*3600)
        if(isEquivalent(fillValue, DBvalue, 1e-5)):
            print attributeName + ' does not need updating; calcVal: ' + str(fillValue) + ' dbVal: '  + str(DBvalue)
        else:
            print attributeName + ' Change ' + str(DBvalue) + ' to ' + str(fillValue)
            updateList.append((newDataDict,attributeName))
            # updateDBvalue(cursor, newDataDict, year, runtime_type_id, table_row, attributeName)
    return updateList


def checkAndUpdateDBvalues(fillSummary, tableName, year, collType, intervalType, intervalID = ''):
    """
    check the values in the database to see if update is needed
    """
    runtime_type_id = getRuntimeTypeID(collType)
    con = connect.cms_wbm_r_online()
    db = dbms.connect.Connection(con, cx_Oracle)
    cursor = db.cursor()
    summary = getTableData(cursor, tableName, year, collType, intervalType, intervalID)
    # Now compare database values with new values
    updateList = checkDBvalues(fillSummary,summary)
    if(updateList is None):
        print "updateList is empty"
        return
    # Update database
    rowID = {'year' : year, 'runtime_type_id' : runtime_type_id}
    for dataTuple in updateList:
        updateDBvalue(cursor, dataTuple[0], rowID, tableName, dataTuple[1])
    # con.commit()
    db.close()

def checkAndInsertDBvalues(fillSummary, tableName, year, collType, intervalType, intervalID = 0):
    """
    insert into if needed. intervalID is the week number / day number
    """
    runtime_type_id = getRuntimeTypeID(collType)
    con = connect.cms_wbm_r_online()
    db = dbms.connect.Connection(con, cx_Oracle)
    cursor = db.cursor()
    rowID = {'year' : year, 'runtime_type_id' : runtime_type_id, 'intervalID' : intervalID}
    summary = getTableData(cursor, tableName, year, collType, intervalType, intervalID)
    if(summary == None):
        updateDB.insertDBvalue(cursor,fillSummary,rowID,tableName)

    # con.commit()
    db.close()

def UpdateFill(fillData, SummaryFields, collType, args,interval,intervalID,tableName):
    FillStatistics = fillStats.FillStats(args, fillData, SummaryFields, collType)
    FillSummary = FillStatistics.getDayIntervalSummary(args.year)
    IntervalEff = FillStatistics.getIntervalEfficiency(args.year,interval,FillSummary[1]['Num'])

    checkAndUpdateDBvalues(FillSummary,tableName, args.year, collType,intervalID)
    checkAndUpdateDBvalues(IntervalEff,tableName, args.year, collType,intervalID)

def InsertIntoTable(begin, end, timeID, fillData, SummaryFields, collType, args,increment,tableName,sum77 = False):
    """
    Designed for Daily Daily77 Weekly and Weekly_WT tables
    """
    FillStatistics = fillStats.FillStats(args, fillData, SummaryFields, collType)
    FillSummary = FillStatistics.getTableSummary(begin,end,increment,None,sum77)

    #checkAndInsertDBvalues

    print FillSummary

def UpdateYearFill(fillData, SummaryFields, collType, args):
    if(checkFillEnd(fillData)):
        print "Most recent fill has ended"

        FillStatistics = fillStats.FillStats(args, fillData, SummaryFields, collType)
        FillSummary = FillStatistics.getFillSummary()
        # print FillSummary

        checkAndUpdateDBvalues(FillSummary, 'CMS_RUNTIME_LOGGER.YEARLY_LUMINOSITY', args.year, collType, 'yearly')

