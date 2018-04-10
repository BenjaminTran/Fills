import cx_Oracle
from os.path import expanduser
import os.path

def readUserPassword( file ):
    with open( file, 'r') as f:
        userPw = f.read()
    if userPw:
        user,password = userPw.split( '/' )
        return user,password.strip()
    else:
        return 'unknown','unknown'

def cms_wbm_r_offline( file = None ):
    if not file:
        file = os.path.join( expanduser('~'), 'test', 'descriptor', 'cms_omds_adg.config' )
    user, password = readUserPassword(file)
    return cx_Oracle.connect( user, password, '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=cmsonr1-adg1-s.cern.ch)(PORT=10121))(ENABLE=BROKEN)(CONNECT_DATA=(SERVICE_NAME=cms_omds_adg.cern.ch)))')


def cms_wbm_r_online( file = None ):
    if not file:
        file = os.path.join( expanduser('~'), 'test', 'descriptor', 'cms_omds_lb.config' )
    user, password = readUserPassword(file)
    return cx_Oracle.connect( user, password, '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=cmsonr1-s.cern.ch)(PORT=10121))(ENABLE=BROKEN)(CONNECT_DATA=(SERVICE_NAME=cms_omds_lb.cern.ch)))')

def cms_wbm_w_online( file = None ):
    if not file:
        file = os.path.join( expanduser('~'), 'test', 'descriptor', 'datasummary.config' )
    user, password = readUserPassword(file)
#    print user
#    print password
    return cx_Oracle.connect( user, password, '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=cmsonr1-s.cern.ch)(PORT=10121))(ENABLE=BROKEN)(CONNECT_DATA=(SERVICE_NAME=cms_omds_lb.cern.ch)))')






