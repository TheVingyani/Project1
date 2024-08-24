#!/usr/bin/env python

"""

Usage:
    load-GeneRIF_Years.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-GeneRIF_Years.py -? | --help

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
import pickle
import re


__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"
PICKLE_FILE = '../data/TCRDv8_PubMed2Date.p'



def load(args, dba, dataset_id, logger, logfile):
    pubmed2date = pickle.load(open(PICKLE_FILE, 'rb'))
    generifs = dba.get_generifs()
    logger.info("Processing {} GeneRIFs".format(len(generifs)))
    yrre = re.compile(r'^(\d{4})')
    ct = 0
    yr_ct = 0
    skip_ct = 0
    net_err_ct = 0
    dba_err_ct = 0
    line_ct = len(generifs)
    #print(pubmed2date)
    for generif in generifs:
        ct += 1
        slmf.update_progress(ct/line_ct)
        logger.debug("Processing GeneRIF: {}".format(generif))
        # GeneRIFs with multiple refs often have duplicates, so fix that
        if "|" in generif['pubmed_ids']:
            pmids = list(set(generif['pubmed_ids'].split("|")))
        else:
            pmids = [generif['pubmed_ids']]
        years = list()
        for pmid in pmids:
            if pmid in pubmed2date:
                m = yrre.match(pubmed2date[pmid])
                if m:
                    years.append(m.groups(1)[0])
                else:
                    years.append('')
            else:
                years.append('')


            if any(years): # if so, so do the updates
                rv = dba.do_update({'table': 'generif', 'id': generif['id'],
                                'col': 'years', 'val':"|".join(years)})
                if rv:
                    yr_ct += 1
                else:
                    dba_err_ct += 1
            else: # if not, skip
                skip_ct += 1
         
    

            
                    
            
      
  
  


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
    # dataset_id = dba.ins_dataset( {'name': 'GeneRIF Years', 'source': 'PubMed records via NCBI E-Utils', 'app': PROGRAM, 'app_version': __version__, 'url': 'https://www.ncbi.nlm.nih.gov/pubmed'} )
    # assert dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # # Provenance
    # rv = dba.ins_provenance({'dataset_id': dataset_id, 'table_name': 'generif', 'column_name': 'years'})
    # assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)
        
    dataset_id = 106
    
    #print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    