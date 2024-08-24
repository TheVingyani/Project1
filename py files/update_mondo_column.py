#!/usr/bin/env python

"""

Usage:
    load-eRAM.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>] [--mondoid=<str>]
    load-eRAM.py -? | --help

Options:
  -m --mondoid MONDO   : mondo id
  -h --dbhost DBHOST   : MySQL database host name [default: localhost]
  -n --dbname DBNAME   : MySQL database name [default: tcrdev]
  -u --dbuser DBUSER   : MySQL login user name [default: root]
  -p --pwfile PWFILE   : MySQL password File path [default: ./tcrd_pass]
  -l --logfile LOGF    : set log file name
  -v --loglevel LOGL   : set logging level [default: 30]
                         50: CRITICAL
                         40: ERROR
                         30: WARNING
                         20: INFO
                         10: DEBUG
                          0: NOTSET
  -q --quiet           : set output verbosity to minimal level
  -d --debug           : turn on debugging output
  -? --help            : print this message and exit 
"""

import os,sys,time
from docopt import docopt
from TCRD.DBAdaptor import DBAdaptor
import logging
import csv
import slm_util_functions as slmf
import obo
import pandas as pd
from collections import defaultdict


__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"






def load(args, dba, logger, logfile):
    diseases = dba.get_disease_name()
    #print(diseases)
    line_ct = len(diseases)
    ct =0
    sc_ct = 0
    db_ct_err = 0
    # ds = dba.get_disease_not_null()
    # for d in diseases:
    #     ct +=1
    #     if d in ds:
    #         continue
    #     slmf.update_progress(ct/line_ct)
    #     mondo_id = dba.get_mondo(d)
        
    #     if mondo_id:
    #         id=mondo_id[0]['mondoid']
    #         rv = dba.update_disease_mondo(d,id)
    #         if rv:
    #             sc_ct +=1
    #         else:
    #             db_ct_err +=1
    
    mondo_dids = dba.get_mondo_xref()
    disease_dids = dba.get_disease_dids()
    line_ct =len(mondo_dids)
    ct =0
    for row in mondo_dids:
        slmf.update_progress(ct/line_ct)

        did = row['db']+":"+row["value"]
        if did not in disease_dids:
            continue
        mondoid = row['mondoid']
        rv = dba.update_disease_mondo_did(did,mondoid)
        
        




    


    

    

            
                    
            
      
  
  


if __name__ == '__main__':
    print("\n{} (v{}) [{}]:\n".format(PROGRAM, __version__, time.strftime("%c")))
    start_time = time.time()

    args = docopt(__doc__, version=__version__)
    if args['--debug']:
        print(f"\n[*DEBUG*] ARGS:\nargs\n")

    if args['--logfile']:
        logfile=args['--logfile']
    else:
        logfile=LOGFILE

    loglevel=int(args['--loglevel'])
    logger =logging.getLogger(__name__)
    logger.setLevel(loglevel)

    if not args['--debug']:
        logger.propagate=False
    fh=logging.FileHandler(logfile)
    fmtr=logging.Formatter('%(asctime)s-%(name)s-%(levelname)s-%(message)s',datefmt='%Y-%m-%d %H:%M:%S')
    fh.setFormatter(fmtr)
    logger.addHandler(fh)

    dba_params={'dbname':args['--dbname'],'dbhost':args['--dbhost'],'pwfile':args['--pwfile'],'logger_name':__name__}

    dba=DBAdaptor(dba_params)
    dbi=dba.get_dbinfo()
    logger.info(f"Connected to database {args['--dbname']} Schema_ver:{dbi['schema_ver']},data ver:{dbi['data_ver']}")
    if not args['--quiet']:
        print(f"Connected to database {args['--dbname']} Schema_ver:{dbi['schema_ver']},data ver:{dbi['data_ver']}")


    load(args , dba, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    