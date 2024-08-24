#!/usr/bin/env python

"""

Usage:
    load-RDO.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-RDO.py -? | --help

Options:
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
import json
from ast import literal_eval
import obo

__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"

def load(args, dba, logger, logfile):
    data = dba.read_json_drugbank()
    total = 0
    db_err_ct = 0
    ct=0
    line_ct =len(data)
    for row in data:
        
        drugbank_id = row['drugbank_id']
        res = json.loads(row['jsondata'])

        if 'drug-interactions' in res and res['drug-interactions'] is not None: 
            if 'drug-interaction' in res['drug-interactions'] and res['drug-interactions']['drug-interaction'] is not None:
                x=res['drug-interactions']['drug-interaction']
                if isinstance(x, list):
                    for i in x:
                        #print(drugbank_id)
                        name=i['name']
                        description=i['description']
                        related_drugbank_id=i['drugbank-id']
                        #print(name,description,related_drugbank_id)

                        rv = dba.ins_drug_drug({'drugbank_id':drugbank_id,'related_drug_name':name,'related_drug_description':description,'related_drugbank_id':related_drugbank_id})
                        if rv:
                            total +=1
                        else:
                            db_err_ct +=1
                        
                else:
                    #print(drugbank_id)
                    name=x['name']
                    description=x['description']
                    related_drugbank_id=x['drugbank-id']
                    #print(name,description,related_drugbank_id)

                    rv = dba.ins_drug_drug({'drugbank_id':drugbank_id,'related_drug_name':name,'related_drug_description':description,'related_drugbank_id':related_drugbank_id})
                    if rv:
                        total +=1
                    else:
                        db_err_ct +=1

        ct +=1
        slmf.update_progress(ct/line_ct)




    print(f"inserted {total} into rdo")
    
    


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







