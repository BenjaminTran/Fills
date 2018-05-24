#!/usr/bin/ python

import fillStats
import updateDB
import fillInterface
import datetime

args = fillInterface.argParser()
begin = datetime.datetime( args.year, 1, 1 )
end = datetime.datetime( args.year, 12, 31 )
filters = [ [ 'start_time', 'GT', begin.isoformat() + 'Z'], [ 'start_time', 'LT', end.isoformat() + 'Z'], ['start_stable_beam', 'NEQ', 'null'] ]
fields = ['fill_type_runtime','fill_number','peak_lumi','peak_pileup','efficiency_lumi','bunches_colliding','start_stable_beam','start_time','end_time','delivered_lumi','recorded_lumi']

fillData = fillInterface.fillRequest(args,filters,fields)

if fillData:
    # The entries in SummaryFields are the keys to the values in the dictionary returned
    # in the API for maxlumi<interval>, longest<interval>, and maxlumirecorded<interval>
    SummaryFields = ['delivered_lumi', 'longest_stable_beam', 'recorded_lumi']
    interval = 'day'
    intervalID = 'yearly'
    tableName = 'CMS_RUNTIME_LOGGER.YEARLY_LUMINOSITY'

    if(args.collType == "ALL"):
        print "="*10 + "PROTONS PROTONS PROTONS" + "="*10
        updateDB.UpdateFill(fillData, SummaryFields, "PROTONS", args,interval,intervalID,tableName)
        print "="*10 + "PROTONS_PB PROTONS_PB PROTONS_PB" + "="*10
        updateDB.UpdateFill(fillData, SummaryFields, "PROTONS_PB", args,interval,intervalID,tableName)
        print "="*10 + "PB PB PB" + "="*30
        updateDB.UpdateFill(fillData, SummaryFields, "PB", args,interval,intervalID,tableName)
    else:
        updateDB.UpdateFill(fillData, SummaryFields, args.collType, args,interval,intervalID,tableName)
