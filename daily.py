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
# begin = datetime.datetime(today.year, today.month, today.day)
begin = datetime.datetime(today.year, 1, 1, 0,0,0,0)
print begin
dayNumber = datetime.datetime.today().timetuple().tm_yday
print dayNumber
# end = datetime.datetime(today.year, today.month, today.day, 23, 59, 59, 0)
end = datetime.datetime(today.year, 12, 31, 23, 59, 59, 0)
filters = [ [ 'start_time', 'GT', begin.isoformat() + 'Z'], [ 'start_time', 'LT', end.isoformat() + 'Z'], ['start_stable_beam', 'NEQ', 'null'] ]
# filters = [ [ 'start_time', 'GT', begin.isoformat() + 'Z'], [ 'start_time', 'LT', end.isoformat() + 'Z'], ['start_stable_beam', 'NEQ', 'null'] ]
fields = ['fill_type_runtime','fill_number','peak_lumi','efficiency_lumi','start_stable_beam','start_time','end_time','delivered_lumi','recorded_lumi','duration']

fillData = fillInterface.fillRequest(args,filters,fields)
# fillData_list = fillInterface.fillRequestList(args,filters,fields)

time_low = datetime.datetime(today.year, today.month, today.day, 0,0,0,0,pytz.UTC)
time_high = datetime.datetime(today.year, today.month, today.day, 23, 59, 59, 0,pytz.UTC)
# filteredFill_list = []
fillData_dict = {}
fillData_list = fillData['data']
for dict1 in fillData_list[:]:
    dict2 = dict1['attributes']
    start_time = dateutil.parser.parse(dict2['start_time'])
    if dict2['end_time'] is not None:
        end_time = dateutil.parser.parse(dict2['end_time'])
    if((start_time < time_low or start_time > time_high) and end_time < time_low and dict2['end_time'] is not None):
        fillData_list.remove(dict1)
        # filteredFill_list.append(dict2)
fillData_dict['data'] = fillData_list

# print filteredFill_list
print "======="
print fillData_list

SummaryFields = ['delivered_lumi','recorded_lumi','longest_stable_beam']

updateDB.UpdateTable(time_low,time_high,fillData_dict, SummaryFields,"PROTONS",args,'day')
