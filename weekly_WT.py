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
yesterday = datetime.datetime.today() - relativedelta(days=1)
lastWed = yesterday.date() + relativedelta(weekday=WE(-1)) # -2 assuming function will be called 00:05:00 on wednesday of the week following
thisTue = lastWed + relativedelta(weekday=TU(1))
# firstDay = datetime.datetime(lastWed.year,lastWed.month,lastWed.day,0,0,0,0) - datetime.timedelta(days=1)
time_low = datetime.datetime(lastWed.year, lastWed.month, lastWed.day, 0,0,0,0)
time_high = datetime.datetime(thisTue.year,thisTue.month,thisTue.day,23,59,59,0)
firstDate = datetime.datetime(today.date().year,1,1,0,0,0,0)

weekNumber = int(math.ceil((today - firstDate).days/7.0))
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
    updateDB.InsertIntoTable(time_low,time_high,fillData, SummaryFields,"PROTONS",args,'CMS_RUNTIME_LOGGER.WEEKLY_LUMINOSITY_WT','week')
else:
    print 'bad'
