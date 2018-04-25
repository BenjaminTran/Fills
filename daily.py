#!/usr/bin/ python

import fillStats
import updateDB
import fillInterface

args = fillInterface.argParser()
fillData = fillInterface.fillRequest(args)

if fillData:
    SummaryFields = ['delivered_lumi', 'longest_stable_beam', 'recorded_lumi']

    if(args.collType == "PROTONS"):
        updateDB.UpdateDayFill(fillData, SummaryFields, args.collType, args.year)
