#!/usr/bin/env python

"""

Usage:
    load-LocSigDB.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-LocSigDB.py -? | --help

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
from collections import defaultdict


__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"
DOWNLOAD_DIR = '../data/LocSigDB/'
BASE_URL = 'http://genome.unmc.edu/LocSigDB/doc/'
FILENAME = 'LocSigDB.csv'



def load(args, dba, dataset_id, logger, logfile):
    fn = DOWNLOAD_DIR + FILENAME
    line_ct = slmf.wcl(fn)
    with open(fn, 'r') as f:
        ct = 0
        up2pid = {}
        notfnd = set()
        ls_ct = 0
        skip_ct = 0
        pmark = set()
        dba_err_ct = 0
        for line in f:
            ct += 1
            slmf.update_progress(ct/line_ct)
            data = line.split(',')
            if 'Homo sapiens' not in data[5]:
                skip_ct += 1
                continue
            fnd = False
            for up in data[4].split(';'):
                if up in up2pid:
                # we've already found it
                    pid = up2pid[up]
                elif up in notfnd:
                # we've already not found it
                    continue
                else:
                    targets = dba.find_protein_ids({'uniprot': up})
                if not targets:
                    notfnd.add(up)
                    continue
                pid = targets[0]
                up2pid[up] = pid
                rv = dba.ins_locsig( {'protein_id': pid, 'location': data[2],
                                    'signal': data[0], 'pmids': data[3]} )
                if not rv:
                    dba_err_ct += 1
                    continue
                ls_ct += 1
                pmark.add(pid)     
    

            
                    
            
      
  
  


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
    dataset_id = dba.ins_dataset( {'name': 'LocSigDB', 'source': 'File %s from %s'%(FILENAME, BASE_URL), 'app': PROGRAM, 'app_version': __version__, 'url': 'http://genome.unmc.edu/LocSigDB/'} )
    assert dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # Provenance
    rv = dba.ins_provenance({'dataset_id': dataset_id, 'table_name': 'locsig'})
    assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)
    
    #dataset_id = 60
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    