import pytz
import argparse
import datetime
import fills

def argParser():
    """
    Ask for input variables
    """
    parser = argparse.ArgumentParser( description='python script to calculate fill statistics info from OMSAPI', formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument( '-y', '--year', type = int, help='get info for all fills of this year')
    parser.add_argument( '-c', '--collType', type = str, help='filter collision type: ALL, PROTONS, PROTONS_PB, PB, default=ALL', default='ALL')
    parser.add_argument( "-s", "--server", help = "server URL, default=" + fills.OmsApi.defaultServer(), default=None )

    args = parser.parse_args()
    return args


def fillRequest(args, filters, fields):
    api = fills.OmsApi( args.server, debug = True )
    # if args.year:
    fillData = api.getOmsObject( 'fills',
              filters,
              fields)

    print fillData
    return fillData

def fillRequestList(args, filters, fields):
    api = fills.OmsApi(args.server, debug = True)
    fillData = api.getRows('fills', filters, fields)

    return fillData

def lumiRequest(beginTime,endTime,server):
    api = fills.OmsApi(server, debug = True)
    beginTime_tzNone = beginTime.replace(tzinfo=None)
    endTime_tzNone = endTime.replace(tzinfo=None)
    lumiData = api.getOmsObject( 'lumisections',
            filters = [ ['start_time', 'GE', beginTime_tzNone.isoformat() + 'Z'], ['end_time','LE',endTime_tzNone.isoformat() + 'Z']])

    return lumiData

def checkPreviousFill(beginTime,endTime,server):
    """
    if fill is ongoing into the day of interest specified in beginTime,
    return fill number for that fill
    """
    fillNumber = 0
    api = fills.OmsApi(server, debug = True)
    beginTime_tzNone = beginTime.replace(tzinfo=None)
    endTime_tzNone = endTime.replace(tzinfo=None)
    lumiData = api.getRows( 'lumisections',
            filters = [ ['start_time', 'GE', beginTime_tzNone.isoformat() + 'Z'], ['end_time','LE',endTime_tzNone.isoformat() + 'Z']],
            fields = ['fill_number'])
    if lumiData:
        fillNumber = lumiData[0]['fill_number']
        print lumiData


    return fillNumber
