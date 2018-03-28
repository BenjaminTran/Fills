#!/usr/bin/ python


from fillStats import FillStats
from fills import OmsApi
import argparse
import datetime

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
              fields = ['fill_type_runtime','fill_number','peak_lumi','peak_pileup','efficiency_lumi','bunches_target','bunches_colliding','start_stable_beam','start_time','end_time','to_ready_time','delivered_lumi','recorded_lumi']   )
    print fills
    # print

SummaryFields = ['bunches_colliding','delivered_lumi','peak_pileup','efficiency_lumi','recorded_lumi','peak_lumi','longest_stable_beam','fastest_turnaround']

FillStatistics = FillStats(fills, SummaryFields)
Summary = FillStatistics.getFillSummary()
