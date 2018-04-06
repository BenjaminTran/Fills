import collections
import datetime
import dateutil.parser

class Updatedb:
    """
    Functions to check if updated to db is needed and execute if so
    """

    @staticmethod
    def checkFillEnd(fillData = None, interval = 5):
        """
        check if the current fill has ended, interval in minutes
        """
        currentTime = datetime.datetime.now(tzinfo=None)
        mostRecentFillNum = 0
        for dict1 in fillData['data']:
            dict2 = dict1['attributes']
            if(dict2['fill_number'] > mostRecentFillNum):
                mostRecentFillNum = dict2['fill_number']

        for dict1 in fillData['data']:
            dict2 = dict1['attributes']
            if(dict2['fill_number'] == mostRecentFillNum):
                if(currentTime - dateutil.parser.parse(dict2['end_time']) > timedelta(minutes = interval)):
                    return True
