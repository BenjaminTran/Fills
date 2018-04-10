import pytz
import collections
import datetime
import dateutil.parser
import sys
import json
import cx_Oracle
import dbms
# sys.path.append('..')
import connect

class Updatedb:
    """
    Functions to check if updated to db is needed and execute if so
    """

    @staticmethod
    def checkFillEnd(fillData = None, interval = 5):
        """
        check if the current fill has ended, interval in minutes
        """
        currentTime = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
        mostRecentFillNum = 0
        for dict1 in fillData['data']:
            dict2 = dict1['attributes']
            if(dict2['fill_number'] > mostRecentFillNum):
                mostRecentFillNum = dict2['fill_number']

        for dict1 in fillData['data']:
            dict2 = dict1['attributes']
            if(dict2['fill_number'] == mostRecentFillNum):
                if(currentTime - dateutil.parser.parse(dict2['end_time']) > datetime.timedelta(minutes = interval)):
                    return True

    @staticmethod
    def checkDBvalues():
        """
        check the values in the database to see if update is needed
        """
        oracle2java = {'FLOAT' : 'Double', 'NUMBER' : 'Integer', 'VARCHAR2': 'String', 'TIMESTAMP(9)' : 'String'}

        con = connect.cms_wbm_r_offline()

        db = dbms.connect.Connection(con, cx_Oracle)

        table = 'CMS_RUNTIME_LOGGER.YEARLY_LUMINOSITY'

        columns = dbms.probe.ProbeOracle(db).getColumns(table,'CMS_RUNTIME_LOGGER')
        oms = { 'RESOURCE_NAME' : table, 'javaClass' : '', 'javaPackage' : '', 'path' : '', 'jsonApiResource': '', 'attributes' : []}
        for column in columns:
            attribute = {}
            oms['attributes'].append({'db' : {'column' : column['name'].lower(), 'type' : column['data_type']},
                'api' : {'attribute' : column['name'].lower(), 'type' : oracle2java[column['data_type']]}})

            with open(table + '.json', 'w') as outfile:
                json.dump(oms, outfile, indent = 2)
