#!/usr/bin/env python

"""

Usage:
    load-CSPA.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-CSPA.py -? | --help

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

INFILE ='../data/CSPA/S1_File.csv'



def load(args, dba, dataset_id, logger, logfile):
    line_ct = slmf.wcl(INFILE)
    ct = 0
    k2pids = defaultdict(list)
    notfnd = set()
    skip_ct = 0
    dba_err_ct = 0
    pmark = {}
    exp_ct = 0
    with open(INFILE, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        header = csvreader.__next__()
        for row in csvreader:
            ct += 1
            if row[2] != '1 - high confidence':
                skip_ct += 1
                continue
            uniprot = row[1]
            geneid = row[4]
            k = "%s|%s"%(uniprot,geneid)
            if k in k2pids:
                # we've already found it
                pids = k2pids[k]
            elif k in notfnd:
                # we've already not found it
                continue
            else:
                # look it up
                targets = dba.find_protein_ids({'uniprot': uniprot}, False)
                if not targets:
                    targets = dba.find_protein_ids({'geneid': geneid}, False)
                if not targets:
                    notfnd.add(k)
                    continue
                pids = []
                for t in targets:
                    pids.append(t)
            for pid in pids:
                cell_lines = [c for c in header[6:-1]] # there's a blank field at the end of the header line
                for (i,cl) in enumerate(cell_lines):
                    val_idx = i + 6 # add six because row has other values at beginning
                    if not row[val_idx]:
                        continue
                    rv = dba.ins_expression( {'protein_id': pid, 'etype': 'Cell Surface Protein Atlas',
                                                'tissue': 'Cell Line '+cl, 'boolean_value': True} )
                    if not rv:
                        dba_err_ct += 1
                        continue
                    exp_ct += 1
                pmark[pid] = True

            
    print(" \n Inserted {} new expression rows for {} proteins.".format(exp_ct, len(pmark)))    
        

            
                    
            
      
  
  


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
    dataset_id = dba.ins_dataset( {'name': 'Cell Surface Protein Atlas', 'source': 'Worksheet B in S1_File.xlsx from http://wlab.ethz.ch/cspa/#downloads, converted to CSV', 'app': PROGRAM, 'app_version': __version__, 'url': 'http://wlab.ethz.ch/cspa'} )
    assert dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # Provenance
    rv = dba.ins_provenance({'dataset_id': dataset_id, 'table_name': 'expression', 'where_clause': "etype = 'Cell Surface Protein Atlas'", 'comment': 'Only high confidence values are loaded.'})
    assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)
    
    dataset_id = 69

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    