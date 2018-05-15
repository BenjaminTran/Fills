#!/usr/bin/ python

import fillStats
import updateDB
import fillInterface

args = fillInterface.argParser()
fillData = fillInterface.fillRequest(args)

if fillData:
    # The entries in SummaryFields are the keys to the values in the dictionary returned
    # in the API for maxlumi<interval>, longest<interval>, and maxlumirecorded<interval>
    SummaryFields = ['delivered_lumi', 'longest_stable_beam', 'recorded_lumi']

    if(args.collType == "ALL"):
        print "="*10 + "PROTONS PROTONS PROTONS" + "="*10
        updateDB.UpdateWeekFill(fillData, SummaryFields, "PROTONS", args)
        print "="*10 + "PROTONS_PB PROTONS_PB PROTONS_PB" + "="*10
        updateDB.UpdateWeekFill(fillData, SummaryFields, "PROTONS_PB", args)
        print "="*10 + "PB PB PB" + "="*30
        updateDB.UpdateWeekFill(fillData, SummaryFields, "PB", args)
    else:
        updateDB.UpdateWeekFill(fillData, SummaryFields, args.collType, args)

