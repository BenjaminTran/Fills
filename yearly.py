#!/usr/bin/ python


import fillStats
import updateDB
import fillInterface

args = fillInterface.argParser()

fillData = fillInterface.fillRequest(args)

if fillData:
    SummaryFields = ['bunches_colliding','delivered_lumi','peak_pileup','efficiency_lumi','recorded_lumi','peak_lumi','longest_stable_beam','fastest_turnaround']
    # print fillData
    # print

    # Acceptable collType names = ALL | PROTONS | PROTONS_PB | PB
    if(args.collType == "ALL"):
        print "="*10 + "PROTONS PROTONS PROTONS" + "="*10
        updateDB.UpdateFill(fillData, SummaryFields, "PROTONS", args.year)
        print "="*10 + "PROTONS_PB PROTONS_PB PROTONS_PB" + "="*10
        updateDB.UpdateFill(fillData, SummaryFields, "PROTONS_PB", args.year)
        print "="*10 + "PB PB PB" + "="*30
        updateDB.UpdateFill(fillData, SummaryFields, "PB", args.year)
    else:
        updateDB.UpdateFill(fillData, SummaryFields, args.collType, args.year)
