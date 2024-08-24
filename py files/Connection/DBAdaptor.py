import sys
import platform
import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
from contextlib import closing
from collections import defaultdict
import logging
from TCRD.Create import CreateMethodsMixin
from TCRD.Read import ReadMethodsMixin 
from TCRD.Update import UpdateMethodsMixin
from TCRD.Delete import DeleteMethodsMixin

class DBAdaptor(CreateMethodsMixin,ReadMethodsMixin,UpdateMethodsMixin,DeleteMethodsMixin):
    # Default config
    _DBHost='localhost';
    _DBPort=3306 ;
    _DBName='mkdev'
    _DBUser='root'

    _LogFile='/tmp/mkdevpy3_DBA.log'
    _LogLevel=logging.WARNING

    def __init__(self,init):
        # building connection 
        if 'dbhost' in init:
            dbhost=init['dbhost']
        else:
            dbhost=self._DBHost
        if 'dbport' in init:
            dbport=init['dbport']
        else:
            dbport=self._DBPort
        if 'dbname' in init:
            dbname=init['dbname']
        else :
            dbname=self._DBName
        if 'dbuser' in init:
            dbuser=init['dbuser']
        else :
            dbuser=self._DBUser
        
        pwfile=init['pwfile']
        dbauth=self._get_auth(pwfile)
        
        if 'logger_name' in init:
            ln =init['logger_name']+'.auxliary.DBAdaptor'
            self._logger=logging.getLogger(ln)
        else:
            if 'logfile' in init:
                lfn=init['loglevel']
            else:
                lfn=self._LogFile
            if 'loglevel' in init:
                ll=init['loglevel']
            else:
                ll=self._LogLevel
            self._logger=logging.getLogger(__name__)
            self._logger.propagate=False #turn off console logging
            fh=logging.FileHandler(lfn)
            self._logger.setLevel(ll)
            fmtr=logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            fh.setFormatter(fmtr)
            self._logger.addHandler(fh)
        self._logger.debug('Instatialing new mkdev DBAdator')
        self._connect(host=dbhost,port=dbport,db=dbname,user=dbuser,passwd=dbauth)

        # self._cache_info_types()
        # self._cache_xref_types()
        # self._cache_expression_types()
        # self._cache_phenotype_types()
        # self._cache_gene_attribute_types()


    def _get_auth(self,pw_file):
        #function will try to get the password file pwfile
        f=open(pw_file,'r')
        pw=f.readline().strip()
        return pw
    
    def _connect(self,host,port,db,user,passwd):
        # function will connect to a database using the information provided by the user

        try:
            self._conn=mysql.connector.connect(host=host,port=port,db=db,user=user,passwd=passwd,charset='utf8')
        except:
            print(f"dbhost:{host},port:{port},user:{user},db:{db},password:{passwd}")
            self._logger.error("Error connecting to MySQL database server")
        self._logger.debug(f"Successful connection to database {db}:{self._conn}")
    def get_dbinfo(self):
        self._logger.debug('get_dbinfo() entry')
        sql='SELECT * FROM dbinfo'
        self._logger.debug('creating cursor')
        try:
            cur=self._conn.cursor(dictionary=True)
        except Error as e:
            self._logger.error(f"error creating cursor: {e}")
        self._logger.debug(f"ececuting query:'{sql}'")
        try:
            cur.execute(sql)
        except Error as e:
            self._logger.error(f"error ececuting query:{e}")
        self._logger.debug("fetching data")
        try:
            row=cur.fetchone()
        except Error as e:
            self._logger.error(f"Error in fetching data:{e}")
        results=cur.fetchall()
        cur.close()
        return row  

    def warning(*objs):
        print("TCRD DBAdaptor WARNING: ", *objs, file=sys.stderr)       
    
