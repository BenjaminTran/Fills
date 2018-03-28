import sys
import urllib
import urllib2
import json



OMS_API_SERVER = 'http://cmsomsapi.cern.ch:8080/api/v1'



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
