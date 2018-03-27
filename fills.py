#!/usr/bin/ python

import datetime
import collections
import argparse
import sys
import urllib
import urllib2
import json
import dateutil.parser
from pytz import timezone



OMS_API_SERVER = 'http://cmsomsapi.cern.ch:8080/api/v1'

def formatTimeDelta(timeDelta):
    d = {"D" : timeDelta.days}
    d["H"], rem = divmod(timeDelta.seconds, 3600)
    d["M"], d["S"] = divmod(rem, 60)
    d["H"] += d["D"]*24
    return str(d["H"]) + ":" + str(d["M"]) + ":" + str(d["S"])


class OmsApi:
    """
    OMS API object
    """

    def __init__(self, url = None, debug = False):
        """
        Construct API object.
        url: URL to OMS API server
        debug: should debug messages be printed out? Verbose!
        """
        if not url:
            self.url = OMS_API_SERVER
        else:
            self.url = url
        self.debug = debug

    def dprint(self, *args):
        """
        Print debug information
        """
        if self.debug: 
            print "OmsApi:",
            for arg in args:
                print arg, 
            print
            
    @staticmethod
    def defaultServer():
        return OMS_API_SERVER

    @staticmethod
    def buildFilters( filter_list ):
        """
        filter_list: list of filters
        each filter: 3 items: column, comparator, value
        returned: dictionary to be converted by urlencode and appended to url
        """
        if not filter_list:
            return {}
        filters = {}
        for filter in filter_list:
            name = 'filter[' + filter[0] + '][' + filter[1] + ']'
            filters[name] = filter[2]
        return filters
    
    @staticmethod
    def rows( response ):
        """
        extract data rows from OMS object
        """
        rows = []
        data = response['data']
        if isinstance( data, list ):
            for row in data:
                rows.append(row['attributes'])
        else:
            rows.append( data )
        return rows
    
    def getRows(self, resource, filters, fields = None ):
        """
        get data rows from OMS API server according to filters and selected fields
        """
        response = self.getOmsObject( resource, filters, fields )
        return self.rows( response )

    def getOmsObject(self, resource, filters = None, fields = None ):
        """
        get OMS API object from server accroding to filters and selected fields
        """
        params = self.buildFilters(filters)
        if fields:
            params['fields[' + resource + ']'] = ','.join( fields )

        if type(params) != dict: 
            params = {}
        all_params = dict( params )
        all_params['page[limit]'] = 100000
        all_params['include'] = 'dataonly,meta'

        #
        # Constructing request path
        #
        url_values = urllib.urlencode( all_params )
        callurl = self.url + '/' + resource + '?' + url_values

        #
        # Do the query and respond
        #
        self.dprint( callurl )

        request = urllib2.Request( callurl )
        response = urllib2.urlopen(request)
        data = json.load( response )
        return data

    @staticmethod
    def getMaxValue(fillData, field = None):
        """
        get the maximum value of a specified field
        """
        maxField = [0,0,0]
        if field:
            for dict1 in fillData['data']:
                dict2 = dict1['attributes']
                fillNum = dict2['fill_number']
                value = dict2[field]
                time_unf = dateutil.parser.parse(dict2['start_time'])
                time = time_unf.strftime('%y/%m/%d %H:%M:%S')
                tmp = [fillNum,value,time]
                if maxField[1] < tmp[1]:
                    maxField = tmp
            print maxField
            return maxField
        return {}

    @staticmethod
    def getLongestStableBeam(fillData):
        """
        get the Longest time duration of stable beams and fastest turnaround
        """
        result = [0,datetime.timedelta(),0]

        for dict1 in fillData['data']:
            dict2 = dict1['attributes']
            fillNum = dict2['fill_number']
            time_stable_beam = dateutil.parser.parse(dict2['start_stable_beam'])
            time_start_fill = dateutil.parser.parse(dict2['start_time'])
            time_end_fill = dateutil.parser.parse(dict2['end_time'])
            duration = time_end_fill - time_stable_beam
            tmp = [fillNum, duration, time_start_fill]
            if result[1] < tmp[1]:
                result = tmp

        Duration = formatTimeDelta(result[1])
        result[1] = Duration
        result[2] = result[2].strftime('%Y/%m/%d %H:%M:%S')
        print result
        return result

    @staticmethod
    def getFastestTurnaround(fillData):
        """
        get the fastest turnaround time
        """

        # fillNumber, fillNumber for previous fill, turnaround time, start time, start time for previous fill
        result = [0,0,datetime.timedelta(hours=99),0,0]

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
            if result[2] > tmp[2]:
                result = tmp
        result[2] = formatTimeDelta(result[2])
        result[3] = result[3].strftime('%Y/%m/%d %H:%M:%S')
        result[4] = result[4].strftime('%Y/%m/%d %H:%M:%S')
        print result

    @staticmethod
    def getFillSummary(fillData, Fields = None):
        """
        get the fill statistics as a dictionary
        """
        FillSummary = {}
        if Fields:
            for field in Fields:
                FillSummary[field] = self.getMaxValue(fillData, field)
            fastestAndLongest = self.getLongestStableBeam(fillData)

        return {}


parser = argparse.ArgumentParser( description='python example script to get fill info from OMSAPI', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
parser.add_argument( '-y', '--year', type = int, help='get info for all fills of this year')
parser.add_argument( "-s", "--server", help = "server URL, default=" + OmsApi.defaultServer(), default=None )

args = parser.parse_args()


api = OmsApi( args.server, debug = True )
if args.year:
    Jan1 = datetime.datetime( args.year, 1, 1 )
    Dec31 = datetime.datetime( args.year, 12, 31 )
    fills = api.getOmsObject( 'fills', 
              filters = [ [ 'start_time', 'GT', Jan1.isoformat() + 'Z'], [ 'start_time', 'LT', Dec31.isoformat() + 'Z'], ['start_stable_beam', 'NEQ', 'null'] ],
              fields = ['fill_number','peak_lumi','peak_pileup','efficiency_lumi','bunches_target','start_stable_beam','start_time','end_time','to_ready_time','delivered_lumi,recorded_lumi']   )
    # print fills
    # print
    #api.getMaxValue(fills,'bunches_target')
    # api.getMaxValue(fills,'delivered_lumi')
    # api.getMaxValue(fills,'peak_pileup')
    # api.getMaxValue(fills,'efficiency_lumi')
    # api.getMaxValue(fills,'recorded_lumi')
    # api.getMaxValue(fills,'peak_lumi')
    # api.getLongestStableBeam(fills)
    api.getFastestTurnaround(fills)
