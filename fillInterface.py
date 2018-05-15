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


def fillRequest(args):
    api = fills.OmsApi( args.server, debug = True )
    if args.year:
        Jan1 = datetime.datetime( args.year, 1, 1 )
        Dec31 = datetime.datetime( args.year, 12, 31 )
        fillData = api.getOmsObject( 'fills',
                  filters = [ [ 'start_time', 'GT', Jan1.isoformat() + 'Z'], [ 'start_time', 'LT', Dec31.isoformat() + 'Z'], ['start_stable_beam', 'NEQ', 'null'] ],
                  fields = ['fill_type_runtime','fill_number','peak_lumi','peak_pileup','efficiency_lumi','bunches_target','bunches_colliding','start_stable_beam','start_time','end_time','to_ready_time','delivered_lumi','recorded_lumi']   )

    return fillData

def lumiRequest(beginTime,endTime,server):
    api = fills.OmsApi(server, debug = True)
    beginTime_tzNone = beginTime.replace(tzinfo=None)
    endTime_tzNone = endTime.replace(tzinfo=None)
    lumiData = api.getOmsObject( 'lumisections',
            filters = [ ['start_time', 'GE', beginTime_tzNone.isoformat() + 'Z'], ['end_time','LE',endTime_tzNone.isoformat() + 'Z']])

    return lumiData


