import pytz
import datetime
import dateutil.parser
import sys
import json
import cx_Oracle
import dbms
# sys.path.append('..')
import connect
import fillStats

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
        'longest_stable_beam' : 'longestfill_hours'}

# def updateDBvalue(cursor, value, fillNum, date, rowID, tableName, columnName):
def updateDBvalue(cursor, fillDict, rowID, collType, tableName, columnName):
    prompt_print = ''
    if(columnName == 'longestday_hours'):
        columnName = 'longestday'
    elif(columnName == 'longestweek_hours'):
        columnName = 'longestweek'
    elif(columnName == 'longestmonth_hours'):
        columnName = 'longestmonth'


    # Does not have datetime fields and has two fill fields
    if(columnName == 'fastest_beam_turnaround_hours'):
        updateColumns = [columnName, columnName + '_fill1',  columnName + '_fill2']
        prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + " = " + str(fillDict['value']) + " " + updateColumns[1] + " = " + str(fillDict['prev_fill_number']) + " " + updateColumns[2] + " = " + str(fillDict['fill_number']) + " WHERE year = " + str(rowID) + " AND runtime_type_id = " + str(collType)
        prompt_print = prompt
        # prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + "=%s " + updateColumns[1] + "=%s " + updateColumns[2] + "=%s WHERE year =%s AND runtime_type_id =%s"
        #cursor.execute(prompt, (fillDict['value'], fillDict['prev_fill_number'], fillDict['fill_number'], rowID, collType))

    elif(columnName == 'longestfill_hours'): # Does not have fill field or datetime field
        prompt = "UPDATE " + tableName + " SET " + columnName + "=" + str(fillDict['value']) + " WHERE year = " + str(rowID) + " AND runtime_type_id = " + str(collType)
        prompt_print = prompt
        #cursor.execute(prompt)

    elif(columnName == 'peaklumi' or columnName == 'maxpileup' or columnName == 'maxbunches'): # has both fill and datetime fields
        updateColumns = [columnName, columnName + '_fill', columnName + '_time']
        prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + " = " + str(fillDict['value']) + " " + updateColumns[1] + " = " + str(fillDict['fill_number']) + " " + updateColumns[2] + " = " + str(fillDict['start_stable_beam']) + " WHERE year = " + str(rowID) + " AND runtime_type_id = " + str(collType)
        prompt_print = prompt
        # prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + "=%s " + updateColumns[1] + "=%s " + updateColumns[2] + "=%s WHERE year =%s AND runtime_type_id =%s"
        #cursor.execute(prompt, (fillDict['value'], fillDict['fill_number'], fillDict['start_stable_beam'], rowID, collType))

    elif(columnName == 'maxlumiday' or columnName == 'maxlumirecordedday'): # time interval values
        updateColumns = [columnName, columnName + '_day']
        prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + "=" + str(fillDict['value']) + updateColumns[1] + "=" + str(fillDict['Num']) + " WHERE year = " + str(rowID) + " AND runtime_type_id = " + str(collType)
        prompt_print = prompt
        #prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + "=%s" + updateColumns[1] + "=%s" + " WHERE year =%s AND runtime_type_id =%s "
        # cursor.execute(prompt, (fillDict['value'], fillDict['Num'], rowID, collType))

    elif(columnName == 'maxlumiweek' or columnName == 'maxlumirecordedweek'): # time interval values
        updateColumns = [columnName, columnName + '_week']
        prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + "=" + str(fillDict['value']) + updateColumns[1] + "=" + str(fillDict['Num']) + " WHERE year = " + str(rowID) + " AND runtime_type_id = " + str(collType)
        prompt_print = prompt
        #prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + "=%s" + updateColumns[1] + "=%s" + " WHERE year =%s AND runtime_type_id =%s "
        # cursor.execute(prompt, (fillDict['value'], fillDict['Num'], rowID, collType))

    elif(columnName == 'maxlumimonth' or columnName == 'maxlumirecordedmonth'): # time interval values
        updateColumns = [columnName, columnName + '_month']
        prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + "=" + str(fillDict['value']) + updateColumns[1] + "=" + str(fillDict['Num']) + " WHERE year = " + str(rowID) + " AND runtime_type_id = " + str(collType)
        prompt_print = prompt
        #prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + "=%s" + updateColumns[1] + "=%s" + " WHERE year =%s AND runtime_type_id =%s "
        # cursor.execute(prompt, (fillDict['value'], fillDict['Num'], rowID, collType))

    elif(columnName == 'longestday' or columnName == 'longestweek' or columnName == 'longestmonth'): # day interval values
        updateColumns = [columnName, columnName + '_hours']
        prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + "=" + str(fillDict['Num']) + " " + updateColumns[1] + "=" + str(fillDict['value']) + " WHERE year = " + str(rowID) + " AND runtime_type_id = " + str(collType)
        prompt_print = prompt
        #prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + "=%s" + updateColumns[1] + "=%s" + " WHERE year =%s AND runtime_type_id =%s "
        # cursor.execute(prompt, (fillDict['Num'], fillDict['value'], rowID, collType))

    else: # Does not have datetime field
        updateColumns = [columnName, columnName + '_fill']
        prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + " = " + str(fillDict['value']) + " " + updateColumns[1] + " = " + str(fillDict['fill_number']) + " WHERE year = " + str(rowID) + " AND runtime_type_id = " + str(collType)
        prompt_print = prompt
        # prompt = "UPDATE " + tableName + " SET " + updateColumns[0] + "=%s " + updateColumns[1] + "=%s WHERE year =%s AND runtime_type_id =%s"
        #cursor.execute(prompt, (fillDict['value'], fillDict['fill_number'], rowID, collType))

    print prompt_print

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

def checkAndUpdateDBvalues(fillSummary, year, collType):
    """
    check the values in the database to see if update is needed
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
        return

    oracle2java = {'FLOAT' : 'Double', 'NUMBER' : 'Integer', 'VARCHAR2': 'String', 'TIMESTAMP(9)' : 'String'}

    # con = connect.cms_wbm_r_offline()
    con = connect.cms_wbm_r_online()

    db = dbms.connect.Connection(con, cx_Oracle)

    table_row = 'CMS_RUNTIME_LOGGER.YEARLY_LUMINOSITY'

    cursor = db.cursor()
    cursor.execute('select * from ' + table_row + ' where year = ' + str(year) + ' and runtime_type_id = ' + str(runtime_type_id))
    yearSummary = cursor.fetchone()
    if(yearSummary == None):
        print "no yearSummary"
        return
    print "="*80
    print yearSummary
    print "="*80

    # Now compare database values with new values

    for newDataDict in fillSummary:
        attributeName = newDataDict['field']
        if('day' not in attributeName and 'week' not in attributeName and 'month' not in attributeName):
            attributeName = FillRequest2DBtableDict[newDataDict['field']]
        # print attributeName
        DBvalue = yearSummary[attributeName]
        fillValue = newDataDict['value']
        if(DBvalue is None or fillValue is None):
            continue
        # if(attributeName == 'longestfill_hours' or attributeName == 'fastest_beam_turnaround_hours' or attributeName == 'longestday_hours'):
        if('_hours' in attributeName):
            #int() to ensure the values are integer seconds
            DBvalue = int(yearSummary[attributeName]*3600)
            if(fillValue == ''):
                fillValue = 0
            else:
                fillValue = int(newDataDict['value']*3600)
        if(isEquivalent(fillValue, DBvalue, 1e-5)):
            print attributeName + ' does not need updating'
        else:
            updateDBvalue(cursor, newDataDict, year, runtime_type_id, table_row, attributeName)

    #con.commit()
    db.close()

def UpdateDayFill(fillData, SummaryFields, collType, year):
    FillStatistics = fillStats.FillStats(fillData, SummaryFields, collType)
    FillSummary = FillStatistics.getDayIntervalSummary(year)

    checkAndUpdateDBvalues(FillSummary, year, collType)


def UpdateFill(fillData, SummaryFields, collType, year):
    if(checkFillEnd(fillData)):
        print "Most recent fill has ended"

        FillStatistics = fillStats.FillStats(fillData, SummaryFields, collType)
        FillSummary = FillStatistics.getFillSummary()
        # print FillSummary

        checkAndUpdateDBvalues(FillSummary, year, collType)

