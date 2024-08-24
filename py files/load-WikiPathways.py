#!/usr/bin/env python

"""

Usage:
    load-WikiPathways.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-WikiPathways.py -? | --help

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

DOWNLOAD_DIR = '../data/WikiPathways/'
BASE_URL = 'https://wikipathways-data.wmcloud.org/'
PATHWAYS_FILE = 'gmt-Homo_sapiens.gmt'



def load(args, dba, dataset_id, logger, logfile):
    fn = DOWNLOAD_DIR + PATHWAYS_FILE
    line_ct = slmf.wcl(fn)
    with open(fn, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        # Example line:
        # Apoptosis Modulation and Signaling%WikiPathways_20160516%WP1772%Homo sapiens    http://www.wikipathways.org/instance/WP1772_r85184       843     3725    842 ...
        ct = 0
        gid2pids = defaultdict(list)
        pmark = set()
        notfnd = set()
        pw_ct = 0
        dba_err_ct = 0
        for row in tsvreader:
            ct += 1
            slmf.update_progress(ct/line_ct)
            name = row[0].split('%')[0]
            wpid = row[1].split('/')[-1]
            geneids = row[2:]
            for gid in geneids:
                if gid in gid2pids:
                    pids = gid2pids[gid]
                elif gid in notfnd:
                    continue
                else:
                    targets = dba.find_protein_ids({'geneid': gid})
                if not targets:
                    notfnd.add(gid)
                    continue
                pids = targets
                gid2pids[gid] = pids # save this mapping so we only lookup each target once
                for pid in pids:
                    rv = dba.ins_pathway({'protein_id': pid, 'pwtype': 'WikiPathways', 'name': name,
                                            'id_in_source': wpid, 'url': row[1]})
                    if rv:
                        pw_ct += 1
                        pmark.add(pid)
                    else:
                        dba_err_ct += 1

                 
        

            
                    
            
      
  
  


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
    # dataset_id = dba.ins_dataset( {'name': 'WikiPathways', 'source': 'File %s'%BASE_URL+PATHWAYS_FILE, 'app': PROGRAM, 'app_version': __version__, 'url': 'http://www.wikipathways.org/'} )
    # assert dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # # Provenance
    # rv = dba.ins_provenance({'dataset_id': dataset_id, 'table_name': 'pathway', 'where_clause': "pwtype = 'WikiPathways'"})
    # assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)
    
    dataset_id = 81
    
    #print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    