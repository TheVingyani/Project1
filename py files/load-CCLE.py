#!/usr/bin/env python

"""

Usage:
    load-CCLE.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-CCLE.py -? | --help

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
import obo
import pandas as pd
import decimal



__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"

CCLE_TSVGZ_FILE = '../data/CCLE/CCLE.tsv'



def load(args, dba, dataset_id, logger, logfile):
    line_ct = slmf.wcl(CCLE_TSVGZ_FILE)
    ct = 0
    pmark = {}
    exp_ct = 0
    dba_err_ct = 0
    with open(CCLE_TSVGZ_FILE,'r') as f:
        csvreader = csv.reader(f,delimiter='\t')
        headers=csvreader.__next__()
        for row in csvreader:
            
            #print(row)
            ct += 1
            slmf.update_progress(ct/line_ct)
            # "protein_id"    "22RV1_PROSTATE (ACH-000956)"        "expression"
            pid = row[0]
            split_row = row[1].split("_")
            cell_id = split_row[0]
            tissue = split_row[1]
            val = row[2]
            expression_data = {
                    'protein_id': pid,
                    'etype': 'CCLE',
                    'tissue': tissue,
                    'cell_id': cell_id,
                    'number_value': val
                        }
                
            rv = dba.ins_expression(expression_data)
            if not rv:
                dba_err_ct += 1
                continue
            exp_ct += 1
            pmark[pid] = True

            
            

    

            
                    
            
      
  
  


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

    # Dataset
    # dataset_id = dba.ins_dataset( {'name': 'CCLE', 'source': 'File %s from https://portals.broadinstitute.org/ccle', 'app': PROGRAM, 'app_version': __version__, 'url': 'https://portals.broadinstitute.org/ccle'} )
    # assert dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # # Provenance
    # rv = dba.ins_provenance({'dataset_id': dataset_id, 'table_name': 'expression', 'where_clause': "etype = 'CCLE'"})
    # assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)
    
    dataset_id = 74
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    