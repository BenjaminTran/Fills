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
today = datetime.datetime.today()
begin = datetime.datetime(today.year, today.month, today.day)
print begin
dayNumber = datetime.datetime.today().timetuple().tm_yday
print dayNumber
end = datetime.datetime(today.year, today.month, today.day, 23, 59, 59, 0)
filters = [ [ 'start_time', 'GT', begin.isoformat() + 'Z'], [ 'start_time', 'LT', end.isoformat() + 'Z'], ['start_stable_beam', 'NEQ', 'null'] ]
fields = ['fill_type_runtime','fill_number','peak_lumi','efficiency_lumi','start_stable_beam','start_time','end_time','delivered_lumi','recorded_lumi']

fillData = fillInterface.fillRequest(args,filters,fields)

SummaryFields = ['delivered_lumi','recorded_lumi','longest_stable_beam']

if fillData:
    updateDB.UpdateTable(begin,end,fillData, SummaryFields,"PROTONS",args,'day')
else:
    print 'bad'
