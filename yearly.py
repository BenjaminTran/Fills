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
    SummaryFields = ['bunches_colliding','delivered_lumi','peak_pileup','efficiency_lumi','recorded_lumi','peak_lumi','longest_stable_beam','fastest_turnaround']
    # print fillData
    # print

    # Acceptable collType names = ALL | PROTONS | PROTONS_PB | PB
    if(args.collType == "ALL"):
        print "="*10 + "PROTONS PROTONS PROTONS" + "="*10
        updateDB.UpdateYearFill(fillData, SummaryFields, "PROTONS", args)
        print "="*10 + "PROTONS_PB PROTONS_PB PROTONS_PB" + "="*10
        updateDB.UpdateYearFill(fillData, SummaryFields, "PROTONS_PB", args)
        print "="*10 + "PB PB PB" + "="*30
        updateDB.UpdateYearFill(fillData, SummaryFields, "PB", args)
    else:
        updateDB.UpdateYearFill(fillData, SummaryFields, args.collType, args)
