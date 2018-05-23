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
current = datetime.datetime.now()
lastWed = current.date() + relativedelta(weekday=WE(-1)) # -2 assuming function will be called 00:05:00 on wednesday
firstDay = datetime.datetime(lastWed.year,lastWed.month,lastWed.day,0,0,0,0) - datetime.timedelta(days=1)
firstDate = datetime.datetime(current.date().year,1,1,0,0,0,0)

print firstDay
weekNumber = int(math.ceil((current - firstDate).days/7.0))
print weekNumber
filters = [ [ 'start_time', 'GT', firstDay.isoformat() + 'Z'], [ 'start_time', 'LT', current.isoformat() + 'Z'], ['start_stable_beam', 'NEQ', 'null'] ]
fields = ['fill_type_runtime','fill_number','peak_lumi','efficiency_lumi','start_stable_beam','start_time','end_time','delivered_lumi','recorded_lumi']
firstDay += datetime.timedelta(days=1)

fillData = fillInterface.fillRequest(args,filters,fields)

SummaryFields = ['delivered_lumi','recorded_lumi','longest_stable_beam']

if fillData:
    updateDB.UpdateTable(firstDay,current,fillData, SummaryFields,"PROTONS",args,'week')
else:
    print 'bad'
