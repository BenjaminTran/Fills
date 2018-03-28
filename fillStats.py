#!/usr/bin/ python

import fills
import dateutil.parser
from pytz import timezone

class FillStats:
    """
    Functions for calculating fill statistics
    """

    def __init__(self, fillData = None, Fields = None):
    """
    Construct fill summary object
    fillData: response from OMS API
    Fields: Selected fields for statistics calculation
    """
    if not fillData:
        print "Please supply the result from OMS API."
    else:
        self.fillData = fillData
    if not Fields:
        print "Please supply a list of fields for statistics calculation"
    else:
        self.Fields = Fields

    @staticmethod
    def formatTimeDelta(timeDelta):
        d = {"D" : timeDelta.days}
        d["H"], rem = divmod(timeDelta.seconds, 3600)
        d["M"], d["S"] = divmod(rem, 60)
        d["H"] += d["D"]*24
        return str(d["H"]) + ":" + str(d["M"]) + ":" + str(d["S"])

    @staticmethod
    def formatDate(date):
        newdate = date.strftime('%Y.%m.%d %H:%M:%S')
        return newdate

    @staticmethod
    def getMaxValue(fillData, field = None):
        """
        get the maximum value of a specified field
        """
        maxField = [0,0,0]
        result = {}
        if field:
            for dict1 in fillData['data']:
                dict2 = dict1['attributes']
                fillNum = dict2['fill_number']
                value = dict2[field]
                time_unf = dateutil.parser.parse(dict2['start_time'])
                time = formatDate(time_unf)
                tmp = [fillNum,value,time]
                if maxField[1] < tmp[1]:
                    maxField = tmp
            result['fill_number'] = maxField[0]
            result[field] = maxField[1]
            result['start_time'] = maxField[2]
            print result
            return result
        return {}

    @staticmethod
    def getLongestStableBeam(fillData, field):
        """
        get the Longest time duration of stable beams and fastest turnaround
        """
        longest = [0,datetime.timedelta(),0]
        result = {}

        for dict1 in fillData['data']:
            dict2 = dict1['attributes']
            fillNum = dict2['fill_number']
            time_stable_beam = dateutil.parser.parse(dict2['start_stable_beam'])
            time_end_fill = dateutil.parser.parse(dict2['end_time'])
            duration = time_end_fill - time_stable_beam
            tmp = [fillNum, duration, time_stable_beam]
            if longest[1] < tmp[1]:
                longest = tmp

        Duration = formatTimeDelta(longest[1])
        result['fill_number'] = longest[0]
        result[field] = Duration
        result['start_stable_beam'] = formatDate(longest[2])
        print result
        return result

    @staticmethod
    def getFastestTurnaround(fillData, field):
        """
        get the fastest turnaround time
        """

        # fillNumber, fillNumber for previous fill, turnaround time, start time, start time for previous fill
        fastest = [0,0,datetime.timedelta(hours=99),0,0]
        result = {}

        fillNumStable = {}
        fillNumEnd = {}

        for dict1 in fillData['data']:
            dict2 = dict1['attributes']
            fillNum = dict2['fill_number']
            time_stable_beam = dateutil.parser.parse(dict2['start_stable_beam'])
            time_end_fill = dateutil.parser.parse(dict2['end_time'])
            time_start_fill = dateutil.parser.parse(dict2['start_time'])
            fillNumStable[fillNum] = time_stable_beam
            fillNumEnd[fillNum] = time_end_fill

        ordered_fillNumStable = collections.OrderedDict(sorted(fillNumStable.items()))
        ordered_fillNumEnd = collections.OrderedDict(sorted(fillNumEnd.items()))

        for i, (fillNumber, times) in enumerate(ordered_fillNumStable.iteritems()):
            if fillNumber == ordered_fillNumStable.keys()[0]:
                continue
            prev_fillNumber = ordered_fillNumStable.keys()[i-1]
            time_previous_end_fill = ordered_fillNumEnd[prev_fillNumber]
            turnaround = times - time_previous_end_fill
            tmp = [fillNumber, prev_fillNumber, turnaround, times, time_previous_end_fill]
            if fastest[2] > tmp[2]:
                fastest = tmp
        result['fill_number'] = fastest[0]
        result['prev_fill_number'] = fastest[1]
        result[field] = formatTimeDelta(fastest[2])
        result['start_stable_beam'] = formatDate(fastest[3])
        result['end_time'] = formatDate(fastest[4])
        print result
        return result

    def getFillSummary(fillData, Fields = None):
        """
        get the fill statistics as a dictionary
        """
        FillSummary = {}
        if Fields:
            for field in Fields:
                if field == 'longest_stable_beam':
                    FillSummary[field] = api.getLongestStableBeam(fillData, field)
                elif field == 'fastest_turnaround':
                    FillSummary[field] = api.getFastestTurnaround(fillData, field)
                else:
                    FillSummary[field] = api.getMaxValue(fillData, field)
            return Fields
        return {}

