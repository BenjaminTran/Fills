import pytz
import datetime
import copy
import collections
import dateutil.parser
from dateutil.relativedelta import *
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
                time_unf = dateutil.parser.parse(dict2['start_stable_beam'])
                time = FillStats.formatDate(time_unf)
                tmp = [fillNum,value,time_unf]
                if maxField[1] < tmp[1]:
                    maxField = tmp
            result['field'] = field
            result['fill_number'] = maxField[0]
            result['value'] = maxField[1]
            result['start_stable_beam'] = maxField[2]
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
            result['start_stable_beam'] = longest[2]
        else:
            result['start_stable_beam'] = 0
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
        fill_list = fillData['data']
        for dict1 in fill_list[:]:
            dict2 = dict1['attributes']
            if(dict2['fill_type_runtime'] != collision_type):
                fillData['data'].remove(dict1)
        return fillData


    def sumDay(self, date, field):
        """
        Sum a field for a given day.
        """
        result = 0
        dateUTC = date.replace(tzinfo=pytz.UTC)
        for dict1 in self.fillData['data']:
            dict2 = dict1['attributes']
            end_time = dateutil.parser.parse(dict2['end_time']).replace(tzinfo=pytz.UTC)
            begin_time = dateutil.parser.parse(dict2['start_stable_beam']).replace(tzinfo=pytz.UTC)
            # if fill has started and ended the day before and after,
            # then add 24 hours
            if((begin_time.date() - dateUTC.date()).days == -1 and (end_time.date() - dateUTC.date()).days == 1):
                    result += 24
                    return result
            # Fills ending or starting on the day
            if(end_time.date() == dateUTC.date() or begin_time.date() == dateUTC.date()):
                if('longest' in field and dict2['delivered_lumi'] > 0):
                    beginning_day = datetime.datetime(dateUTC.year,dateUTC.month,dateUTC.day,0,0,0,0,pytz.UTC)
                    ending_day = datetime.datetime(dateUTC.year,dateUTC.month,dateUTC.day,23,59,59,0,pytz.UTC)
                    # Fill started the day before
                    if((begin_time.date() - dateUTC.date()).days < 0):
                        result += (end_time - beginning_day).total_seconds()/3600
                        print str((end_time - beginning_day).total_seconds()/3600) + " " + str(dict2['fill_number'])

                    # Fill ended the next day
                    elif((end_time.date() - dateUTC.date()).days > 0):
                        result += (ending_day - begin_time).total_seconds()/3600

                        print str((ending_day - begin_time).total_seconds()/3600) + " " + str(dict2['fill_number'])
                    # Fill started and ended the same day
                    elif(end_time.date() == dateUTC.date() and begin_time.date() == dateUTC.date()):
                        result += (end_time - begin_time).total_seconds()/3600
                        print str((end_time - begin_time).total_seconds()/3600) + " " + str(dict2['fill_number'])
                elif('recorded' in field):
                    result += dict2['recorded_lumi']
                else:
                    result += dict2['delivered_lumi']

        return result

    def sumByInterval(self, begin, end, increment, field, year):
        """
        Calculate the given statistic by an increment(string or int) over a time period.
        """
        maxValue = 0
        maxNumber = 0
        Number = 1
        # begin = datetime.datetime(year, 1, 1, 0, 0, 0, 0, pytz.UTC)
        begin_UTC = begin.replace(tzinfo=pytz.UTC)
        incrementalCheck = begin_UTC
        timeIncrement = datetime.timedelta(days=0)
        if(isinstance(increment,int)):
            timeIncrement = datetime.timedelta(days=increment)
        elif(isinstance(increment,basestring)):
            if(increment == 'day'):
                timeIncrement = datetime.timedelta(days=1)
            elif(increment == 'week'):
                timeIncrement = datetime.timedelta(weeks=1)
            elif(increment == 'month'):
                timeIncrement = relativedelta(days=1, day=31)

        while(end.replace(tzinfo=pytz.UTC) - incrementalCheck).days > -1:
            print (end.replace(tzinfo=pytz.UTC) - incrementalCheck).days
            print "IC: " + str(incrementalCheck)
            summedValue = 0
            while(incrementalCheck + timeIncrement > begin_UTC):
                print begin_UTC
                # for dict1 in self.fillData['data']:
                    # dict2 = dict1['attributes']
                    # if((dateutil.parser.parse(dict2['end_time']).replace(tzinfo=pytz.UTC) - begin).days == 0):
                summedValue += self.sumDay(begin_UTC,field)
                print summedValue
                begin_UTC += datetime.timedelta(days=1)
            if(summedValue > maxValue):
                maxValue = summedValue
                maxNumber = Number
            # print "Incrementing number"
            Number += 1
            incrementalCheck += timeIncrement

        result = {'field' : field, 'Num' : maxNumber, 'value' : maxValue}

        return result

    def getDayIntervalSummary(self, year):
        """
        Calculate statistics for interval values
        """
        result_list = []
        # result_list.append(self.sumByInterval(datetime.datetime(year,12,31,23,59,59,0), 'week', 'maxlumiweek', year))
        # result_list.append(self.sumByInterval(datetime.datetime.today(), 'day', 'maxlumirecordedday', year))
        # result_list.append(self.sumByInterval(datetime.datetime(year,1,1,0,0,0,0), datetime.datetime(year,12,31,23,59,59,0), 'day', 'longestday_hours', year))
        # result_list.append(self.sumByInterval(datetime.datetime(year,1,1,0,0,0,0) + relativedelta(weekday=MO(1)), datetime.datetime(year,12,31,23,59,59,0), 'week', 'longestweek_hours', year))
        result_list.append(self.sumByInterval(datetime.datetime(year,1,1,0,0,0,0), datetime.datetime(year,12,31,23,59,59,0), 'month', 'longestmonth_hours', year))

        return result_list

    def getFillSummary(self, otherFills = None):
        """
        get the fill statistics as a list of dictionaries. Must have the same structure as the API response
        """
        FillSummary = []
        FillData = self.fillData

        if otherFills:
            FillData = otherFills

        if self.Fields:
            for field in self.Fields:
                if field == 'longest_stable_beam':
                    FillSummary.append(self.getLongestStableBeam(FillData, field))
                elif field == 'fastest_turnaround':
                    FillSummary.append(self.getFastestTurnaround(FillData, field))
                else:
                    FillSummary.append(self.getMaxValue(FillData, field))
        return FillSummary

