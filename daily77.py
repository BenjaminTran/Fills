import fillStats
import datetime
from dateutil.relativedelta import *
import fillInterface
import updateDB
import connect
import dbms
import cx_Oracle

args = fillInterface.argParser()
# Assuming this function will be called 5 minutes into the week following the week
# to be calculated
today = datetime.datetime.today()
yesterday = today - datetime.timedelta(days=1)
begin = datetime.datetime(yesterday.year, yesterday.month, yesterday.day,7,0,0,0)
print begin
dayNumber = datetime.datetime.today().timetuple().tm_yday
print dayNumber
end = datetime.datetime(today.year, today.month, today.day, 7, 0, 0, 0)
filters = [ [ 'start_time', 'GT', begin.isoformat() + 'Z'], [ 'start_time', 'LT', end.isoformat() + 'Z'], ['start_stable_beam', 'NEQ', 'null'] ]
fields = ['fill_type_runtime','fill_number','peak_lumi','efficiency_lumi','start_stable_beam','start_time','end_time','delivered_lumi','recorded_lumi']

fillData = fillInterface.fillRequest(args,filters,fields)

SummaryFields = ['delivered_lumi','recorded_lumi','longest_stable_beam']

if fillData:
    updateDB.UpdateTable(begin,end,fillData, SummaryFields,"PROTONS",args,'day')
else:
    print 'bad'
