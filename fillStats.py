import pytz
import datetime
import copy
import collections
import dateutil.parser
import fillInterface
from dateutil.relativedelta import *
from pytz import timezone

class FillStats:
    """
    Functions for calculating fill statistics
    """

    # def __init__(self, fillData = None, Fields = None, collType = 'ALL'):
    def __init__(self, args, fillData = None, Fields = None, collType = 'ALL'):
        """
        Construct fill summary object
        fillData: response from OMS API
        Fields: Selected fields for statistics calculation
        """
        self.args = args
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
        # last entry is only used for efficiency for recorded_lumi
        maxField = [0,0,0,0]
        result = {}
        if field:
            for dict1 in fillData['data']:
                dict2 = dict1['attributes']
                fillNum = dict2['fill_number']
                value = dict2[field]
                time_unf = dateutil.parser.parse(dict2['start_stable_beam'])
                # time = FillStats.formatDate(time_unf)
                tmp = [fillNum,value,time_unf,dict2['efficiency_lumi']]
                if maxField[1] < tmp[1]:
                    maxField = tmp
            result['field'] = field
            result['fill_number'] = maxField[0]
            result['value'] = maxField[1]
            result['start_stable_beam'] = maxField[2]
            result['efficiency_lumi'] = maxField[3]
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
        fastest = [0,0,datetime.timedelta(hours=99999),0,0]
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
        result['value'] = fastest[2].total_seconds()/3600
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

    @staticmethod
    def lumiParser(lumiData, field):
        """
        Given a lumiRequest grab the first lumisection and return delivered/recorded lumi
        """
        lumi_delivered = 0
        lumi_recorded = 0
        largestLS = 0
        fillNum = 0
        if lumiData and len(lumiData['data']) > 0:
            if('recorded' in field):
                lumi_recorded = lumiData['data'][0]['attributes']['recorded_lumi']
                if lumi_recorded:
                    return lumi_recorded
                else:
                    return 0
            else:
                lumi_delivered = lumiData['data'][0]['attributes']['delivered_lumi']
                if lumi_delivered:
                    return lumi_delivered
                else:
                    return 0
        else:
            return 0


    def sumDay(self, date, field):
        """
        Sum a field for a given day.
        """
        result = 0
        result_recorded = 0
        dateUTC = date.replace(tzinfo=pytz.UTC)
        for dict1 in self.fillData['data']:
            dict2 = dict1['attributes']
            if(dict2['end_time'] == None):
                continue
            end_time = dateutil.parser.parse(dict2['end_time'])#.replace(tzinfo=pytz.UTC)
            begin_time = dateutil.parser.parse(dict2['start_stable_beam'])#.replace(tzinfo=pytz.UTC)
            beginning_day = datetime.datetime(dateUTC.year,dateUTC.month,dateUTC.day,0,0,0,0,pytz.UTC)
            ending_day = datetime.datetime(dateUTC.year,dateUTC.month,dateUTC.day,23,59,59,0,pytz.UTC)
            DiffDay_begin = (begin_time.date() - dateUTC.date()).days
            DiffDay_end = (end_time.date() - dateUTC.date()).days
            sameDay = True
            if(end_time.date() == dateUTC.date() and begin_time.date() == dateUTC.date()):
                sameDay = True
            else:
                sameDay = False

            # if fill has started and ended the day before and after
            # if(DiffDay_begin == -1 and DiffDay_end == 1):
            if(DiffDay_begin < 0 and DiffDay_end > 0):
                if('longest' in field and dict2['delivered_lumi'] > 0):
                    result += 24
                    return result
                else:
                    lumiData_end = fillInterface.lumiRequest(ending_day - datetime.timedelta(seconds=60),ending_day,self.args.server)
                    lumiData_beg = fillInterface.lumiRequest(beginning_day,beginning_day + datetime.timedelta(seconds=60),self.args.server)
                    if lumiData_end and lumiData_beg:
                        result = self.lumiParser(lumiData_end,field) - self.lumiParser(lumiData_beg,field)
                        print "FILL " + str(dict2['fill_number']) +  " WHOLE DAY " + str(result)
                        return result

            # Fills ending or starting on the day
            if(end_time.date() == dateUTC.date() or begin_time.date() == dateUTC.date()):
                if('longest' in field and dict2['delivered_lumi'] > 0):
                    # Fill started the day before
                    if(DiffDay_begin < 0):
                        result += (end_time - beginning_day).total_seconds()/3600
                        print str((end_time - beginning_day).total_seconds()/3600) + " " + str(dict2['fill_number'])

                    # Fill ended the next day
                    elif(DiffDay_end > 0):
                        result += (ending_day - begin_time).total_seconds()/3600

                        print str((ending_day - begin_time).total_seconds()/3600) + " " + str(dict2['fill_number'])
                    # Fill started and ended the same day
                    elif(sameDay):
                        result += (end_time - begin_time).total_seconds()/3600
                        print str((end_time - begin_time).total_seconds()/3600) + " " + str(dict2['fill_number'])
                else:
                    recorded_lumi = dict2['recorded_lumi']
                    delivered_lumi = dict2['delivered_lumi']
                    # Fill started the day before
                    if(DiffDay_begin < 0):
                        lumiData = fillInterface.lumiRequest(beginning_day,beginning_day + datetime.timedelta(minutes=1),self.args.server)
                        if('maxlumirecorded' in field):
                            result += dict2['recorded_lumi'] - self.lumiParser(lumiData,field)
                            print "(before) Fill Number: " + str(dict2['fill_number']) + " Date: " + str(end_time) + str(dict2['recorded_lumi'] - self.lumiParser(lumiData,field))
                        else:
                            result += dict2['delivered_lumi'] - self.lumiParser(lumiData,field)
                            print "(before) Fill Number: " + str(dict2['fill_number']) + " Date: " + str(end_time) + str(dict2['delivered_lumi'] - self.lumiParser(lumiData,field))

                    # Fill ended the next day
                    elif(DiffDay_end > 0):
                        # lumiData = fillInterface.lumiRequest(ending_day + datetime.timedelta(seconds=1),ending_day + datetime.timedelta(seconds=40))
                        lumiData = fillInterface.lumiRequest(ending_day,ending_day + datetime.timedelta(seconds=60),self.args.server)
                        result += self.lumiParser(lumiData,field)
                        print "(next) Fill Number: " + str(dict2['fill_number']) + " " + str(self.lumiParser(lumiData,field))

                    # Fill started and ended the same day
                    elif(sameDay):
                        if('maxlumirecorded' in field):
                            result += dict2['recorded_lumi']
                            print "Fill Number: " + str(dict2['fill_number']) + " " + str(dict2['recorded_lumi'])
                        else:
                            result += dict2['delivered_lumi']
                            print "(same) Fill Number: " + str(dict2['fill_number']) + " " + str(dict2['delivered_lumi'])
                    print "Day result: " + str(result)

        return result


    def sumByInterval(self, begin, end, increment, field):
        """
        Calculate the given statistic by an increment(string or int) over a time period.
        """
        maxValue = 0
        maxNumber = 0
        Number = 1
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

        while(end.date() - incrementalCheck.date()).days > -1:
            print Number
            print "IC: " + str(incrementalCheck)
            summedValue = 0
            while(incrementalCheck + timeIncrement > begin_UTC):
                summedValue += self.sumDay(begin_UTC,field)
                print summedValue
                begin_UTC += datetime.timedelta(days=1)
            if(summedValue > maxValue):
                maxValue = summedValue
                maxNumber = Number
            Number += 1
            incrementalCheck += timeIncrement

        result = {'field' : field, 'Num' : maxNumber, 'value' : maxValue}

        return result

    def getIntervalEfficiency(self, year, increment, dateID):
        """
        Get efficiency for intervaled maximum recorded lumi
        """
        result = 0
        begin_date = datetime.datetime(year,1,1,0,0,0,0)
        end_date = datetime.datetime(year,12,31,23,59,59,0)
        timeIncrement = datetime.timedelta(days=0)
        if(increment == 'day'):
            begin_date += datetime.timedelta(days=(dateID)-1)
            timeIncrement = datetime.timedelta(days=1)
        elif(increment == 'week'):
            begin_date += relativedelta(weekday=MO(1))
            begin_date += relativedelta(weeks=dateID-1)
            timeIncrement = datetime.timedelta(weeks=1)
        elif(increment == 'month'):
            begin_date += relativedelta(months=dateID-1)
            timeIncrement = relativedelta(days=1, day=31)

        incrementalCheck = begin_date

        rec_lumi = 0
        del_lumi = 0
        while(incrementalCheck + timeIncrement > begin_date):
            rec_lumi += self.sumDay(begin_date,'maxlumirecorded')
            del_lumi += self.sumDay(begin_date,'maxlumi')
            begin_date += datetime.timedelta(days=1)

        eff = rec_lumi/del_lumi

        result = [{'field' : 'maxlumi' + increment + '_eff', 'value' : eff}]

        return result

    def getDayIntervalSummary(self, year):
        """
        Calculate statistics for day interval values
        """
        begin_date = datetime.datetime(year,1,1,0,0,0,0)
        end_date = datetime.datetime(year,12,31,23,59,59,0)
        result_list = []
        result_list.append(self.sumByInterval(begin_date, end_date, 'day', 'maxlumiday'))
        result_list.append(self.sumByInterval(begin_date, end_date, 'day', 'maxlumirecordedday'))
        result_list.append(self.sumByInterval(begin_date, end_date, 'day', 'longestday_hours'))

        return result_list

    def getWeekIntervalSummary(self,year):
        """
        Calculate statistics for week interval values
        """
        begin_date = datetime.datetime(year,1,1,0,0,0,0) + relativedelta(weekday=MO(1))
        end_date = datetime.datetime(year,12,31,23,59,59,0)
        result_list = []
        result_list.append(self.sumByInterval(begin_date, end_date, 'week', 'maxlumiweek'))
        result_list.append(self.sumByInterval(begin_date, end_date, 'week', 'maxlumirecordedweek'))
        result_list.append(self.sumByInterval(begin_date, end_date, 'week', 'longestweek_hours'))

        return result_list

    def getMonthIntervalSummary(self,year):
        """
        Calculate statistics for month interval values
        """
        begin_date = datetime.datetime(year,1,1,0,0,0,0)
        end_date = datetime.datetime(year,12,31,23,59,59,0)
        result_list = []
        result_list.append(self.sumByInterval(begin_date, end_date, 'month', 'maxlumimonth'))
        result_list.append(self.sumByInterval(begin_date, end_date, 'month', 'maxlumirecordedmonth'))
        result_list.append(self.sumByInterval(begin_date, end_date, 'month', 'longestmonth_hours'))

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

