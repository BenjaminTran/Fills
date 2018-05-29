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
today = datetime.datetime.today()
yesterday = datetime.datetime.today() - relativedelta(days=1)
time_low = datetime.datetime(today.year, today.month, today.day, 0,0,0,0) + relativedelta(weekday=MO(-1))
time_high = datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59, 0)
firstDay = datetime.datetime(today.year,1,1) + relativedelta(weekday=MO(1))
weekNumber = int(math.ceil((today - firstDay).days/7.0))
print weekNumber
filters = [ ['start_stable_beam', 'NEQ', 'null'] ]
fields = ['fill_type_runtime','fill_number','peak_lumi','efficiency_lumi','start_stable_beam','start_time','end_time','delivered_lumi','recorded_lumi']
min_fill = fillInterface.checkPreviousFill(time_low,time_low + datetime.timedelta(minutes=1),args.server)
if min_fill != 0:
    filters.append(['fill_number', 'GE', min_fill])
else:
    filters.append(['start_time', 'GT', time_low.isoformat() + 'Z'],['start_time', 'LT', time_high.isoformat() + 'Z'])

fillData = fillInterface.fillRequest(args,filters,fields)

SummaryFields = ['delivered_lumi','recorded_lumi','longest_stable_beam']

if fillData:
    updateDB.UpdateTable(time_low,time_high,fillData, SummaryFields,"PROTONS",args,'week')
else:
    print 'bad'
