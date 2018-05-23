import fillStats
import datetime
from dateutil.relativedelta import *
import math
import fillInterface
import updateDB
import connect
import dbms
import cx_Oracle

args = fillInterface.argParser()
# Assuming this function will be called 5 minutes into the week following the week
# to be calculated
current = datetime.datetime.now()
firstDay = datetime.datetime(args.year,1,1) + relativedelta(weekday=MO(1))
print firstDay
weekNumber = int(math.ceil((current - firstDay).days/7.0))
print weekNumber
firstDay += relativedelta(weeks=weekNumber-1, days=-1)
print firstDay
filters = [ [ 'start_time', 'GT', firstDay.isoformat() + 'Z'], [ 'start_time', 'LT', current.isoformat() + 'Z'], ['start_stable_beam', 'NEQ', 'null'] ]
fields = ['fill_type_runtime','fill_number','peak_lumi','efficiency_lumi','start_stable_beam','start_time','end_time','delivered_lumi','recorded_lumi']

fillData = fillInterface.fillRequest(args,filters,fields)
firstDay += relativedelta(days=1)

SummaryFields = ['delivered_lumi','recorded_lumi','longest_stable_beam']

if fillData:
    updateDB.UpdateTable(firstDay,current,fillData, SummaryFields,"PROTONS",args)
else:
    print 'bad'
