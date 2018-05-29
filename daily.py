import fillStats
import datetime
from dateutil.relativedelta import *
import math
import fillInterface
import updateDB
import connect
import dbms
import cx_Oracle
import pytz
import dateutil

args = fillInterface.argParser()
today = datetime.datetime.today()
time_low = datetime.datetime(today.year, today.month, today.day, 0,0,0,0)
time_high = datetime.datetime(today.year, today.month, today.day, 23, 59, 59, 0)
begin = datetime.datetime(today.year, 1, 1, 0,0,0,0)
end = datetime.datetime(today.year, 12, 31, 23, 59, 59, 0)
print begin
dayNumber = datetime.datetime.today().timetuple().tm_yday
print dayNumber
filters = [ ['start_stable_beam', 'NEQ', 'null'] ]
fields = ['fill_type_runtime','fill_number','peak_lumi','efficiency_lumi','start_stable_beam','start_time','end_time','delivered_lumi','recorded_lumi','duration']
min_fill = fillInterface.checkPreviousFill(time_low,time_low + datetime.timedelta(minutes=1),args.server)
if min_fill != 0:
    filters.append(['fill_number', 'GE', min_fill])
else:
    filters.append(['start_time', 'GT', time_low.isoformat() + 'Z'],['start_time', 'LT', end.isoformat() + 'Z'])


fillData = fillInterface.fillRequest(args,filters,fields)

print fillData


SummaryFields = ['delivered_lumi','recorded_lumi','longest_stable_beam']

updateDB.UpdateTable(time_low,time_high,fillData, SummaryFields,"PROTONS",args,'day')
