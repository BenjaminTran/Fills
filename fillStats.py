import datetime
import copy
import collections
import dateutil.parser
from pytz import timezone

class FillStats:
    """
    Functions for calculating fill statistics
    """

    def __init__(self, fillData = None, Fields = None, collType = 'ALL'):
        """
        Construct fill summary object
        fillData: response from OMS API
        Fields: Selected fields for statistics calculation
        """
        if not fillData:
            print "Please supply the result from OMS API."
        else:
            fillData_copy = copy.deepcopy(fillData)
            self.fillData = self.filterCollResponse(fillData_copy,collType)
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
                time = FillStats.formatDate(time_unf)
                tmp = [fillNum,value,time_unf]
                if maxField[1] < tmp[1]:
                    maxField = tmp
            result['field'] = field
            result['fill_number'] = maxField[0]
            result['value'] = maxField[1]
            result['start_time'] = maxField[2]
            # print result
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
            if dict2['delivered_lumi'] == 0:
                continue
            time_stable_beam = dateutil.parser.parse(dict2['start_stable_beam'])
            time_end_fill = dateutil.parser.parse(dict2['end_time'])
            duration = time_end_fill - time_stable_beam
            tmp = [fillNum, duration, time_stable_beam]
            if longest[1] < tmp[1]:
                longest = tmp

        # Duration = FillStats.formatTimeDelta(longest[1])
        Duration = longest[1].total_seconds()/3600
        result['field'] = field
        result['fill_number'] = longest[0]
        result['value'] = Duration
        if type(longest[2]) is not int:
            # result['start_stable_beam'] = FillStats.formatDate(longest[2])
            result['start_time'] = longest[2]
        else:
            result['start_time'] = 0
        # print result
        return result

    @staticmethod
    def getFastestTurnaround(fillData, field):
        """
        get the fastest turnaround time
        """

        # fillNumber, fillNumber for previous fill, turnaround time, start time, start time for previous fill
        fastest = [0,0,datetime.timedelta(hours=999),0,0]
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
        result['field'] = field
        result['fill_number'] = fastest[0]
        result['prev_fill_number'] = fastest[1]
        if fastest[2] < datetime.timedelta(hours=500):
            # result['value'] = FillStats.formatTimeDelta(fastest[2])
            result['value'] = fastest[2].total_seconds()/3600
        else:
            result['value'] = ''
        if type(fastest[3]) is not int:
            # result['start_stable_beam'] = FillStats.formatDate(fastest[3])
            result['start_time'] = fastest[3]
        else:
            result['start_time'] = 0
        if type(fastest[4]) is not int:
            # result['end_time'] = FillStats.formatDate(fastest[4])
            result['end_time'] = fastest[4]
        else:
            result['end_time'] = 0
        # print result
        return result

    @staticmethod
    def filterCollResponse(fillData, collision_type):
        if collision_type == 'ALL': # Deprecated but here if needed in the future
            return fillData
        else:
            fill_list = fillData['data']
            for dict1 in fill_list[:]:
                dict2 = dict1['attributes']
                if(dict2['fill_type_runtime'] != collision_type):
                    fillData['data'].remove(dict1)
            return fillData

    def getFillSummary(self):
        """
        get the fill statistics as a dictionary
        """
        FillSummary = []
        if self.Fields:
            for field in self.Fields:
                if field == 'longest_stable_beam':
                    FillSummary.append(self.getLongestStableBeam(self.fillData, field))
                elif field == 'fastest_turnaround':
                    FillSummary.append(self.getFastestTurnaround(self.fillData, field))
                else:
                    FillSummary.append(self.getMaxValue(self.fillData, field))
        return FillSummary

