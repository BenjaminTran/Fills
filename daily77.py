import fillStats
import datetime
from dateutil.relativedelta import *
import fillInterface
import updateDB
import connect
import dbms
import cx_Oracle

args = fillInterface.argParser()
today = datetime.datetime.today()
yesterday = today - datetime.timedelta(days=1)
time_low = fillStats.local2UTC(datetime.datetime(yesterday.year, yesterday.month, yesterday.day,7,0,0,0))
print time_low
dayNumber = datetime.datetime.today().timetuple().tm_yday
print dayNumber
time_high = fillStats.local2UTC(datetime.datetime(today.year, today.month, today.day, 7, 0, 0, 0))

filters = [ ['start_stable_beam', 'NEQ', 'null'] ]
fields = ['fill_type_runtime','fill_number','peak_lumi','efficiency_lumi','start_stable_beam','start_time','end_time','delivered_lumi','recorded_lumi','duration']
min_fill = fillInterface.checkPreviousFill(time_low,time_low + datetime.timedelta(minutes=1),args.server)
if min_fill != 0:
    filters.append(['fill_number', 'GE', min_fill])
else:
    filters.append(['start_time', 'GT', time_low.isoformat() + 'Z'],['start_time', 'LT', time_high.isoformat() + 'Z'])

fillData = fillInterface.fillRequest(args,filters,fields)

SummaryFields = ['delivered_lumi','recorded_lumi','longest_stable_beam']

if fillData:
    updateDB.UpdateTable(time_low,time_high,fillData, SummaryFields,"PROTONS",args,'day',True)
else:
    print 'bad'
