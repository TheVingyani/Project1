#!/usr/bin/env python

"""

Usage:
    load-JensenLab-COMPARTMENTS.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-JensenLab-COMPARTMENTS.py -? | --help

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


__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"


FILE_K = '../data/JensenLab/human_compartment_knowledge_full.tsv'
FILE_E = '../data/JensenLab/human_compartment_experiments_full.tsv'
FILE_T = '../data/JensenLab/human_compartment_textmining_full.tsv'
FILE_P = '../data/JensenLab/human_compartment_predictions_full.tsv'

BASE_URL = 'http://download.jensenlab.org/'

def load(args, dba, dataset_id, logger, logfile):
    # Knowledge channel
    fn = FILE_K
    line_ct = slmf.wcl(fn)
    with open(fn, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        ct = 0
        pmark = {}
        comp_ct = 0
        skip_ct = 0
        notfnd = set()
        dba_err_ct = 0
        pmap ={}
        for row in tsvreader:
            ct += 1
            slmf.update_progress(ct/line_ct)
            if int(row[6]) < 3: # skip rows with conf < 3
                skip_ct += 1
                continue
            ensp = row[0]
            sym = row[1]
            k = "%s|%s"%(ensp,sym)
            if k in pmap:
                # we've already found it
                pids = pmap[k]
            elif k in notfnd:
                # we've already not found it
                continue
            else:
                # look it up
                pids = find_pids(dba, ensp, sym, pmap)
            if not pids:
                notfnd.add(k)
                continue
            for pid in pids:
                pmark[pid] = True
                rv = dba.ins_compartment( {'protein_id': pid, 'ctype': 'JensenLab Knowledge',
                                        'go_id': row[2], 'go_term': row[3],
                                        'evidence': "%s %s"%(row[4], row[5]), 
                                        'conf': row[6]} )
                if not rv:
                    dba_err_ct += 1
                    continue
                comp_ct += 1
            
    print(" \n Inserted {} new compartment rows for {} proteins".format(comp_ct, len(pmark)))

    # Experiment channel
    fn =  FILE_E
    line_ct = slmf.wcl(fn)
    with open(fn, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        ct = 0
        pmark = {}
        comp_ct = 0
        skip_ct = 0
        notfnd = set()
        dba_err_ct = 0
        for row in tsvreader:
            ct += 1
            slmf.update_progress(ct/line_ct)

            if float(row[6]) < 3: # skip rows with conf < 3
                skip_ct += 1
                continue
            ensp = row[0]
            sym = row[1]
            k = "%s|%s"%(ensp,sym)
            if k in pmap:
                # we've already found it
                pids = pmap[k]
            elif k in notfnd:
                # we've already not found it
                continue
            else:
                # look it up
                pids = find_pids(dba, ensp, sym, pmap)
            if not pids:
                notfnd.add(k)
                continue
            for pid in pids:
                rv = dba.ins_compartment( {'protein_id': pid, 'ctype': 'JensenLab Experiment',
                                        'go_id': row[2], 'go_term': row[3],
                                        'evidence': "%s %s"%(row[4], row[5]), 
                                        'conf': row[6]} )
                if not rv:
                    dba_err_ct += 1
                    continue
                comp_ct += 1
                pmark[pid] = True

            
    print(" \n Inserted {} new compartment rows for {} proteins".format(comp_ct, len(pmark)))


    # Text Mining channel
    fn =  FILE_T
    line_ct = slmf.wcl(fn)
    with open(fn, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        ct = 0
        pmark = {}
        comp_ct = 0
        skip_ct = 0
        notfnd = set()
        dba_err_ct = 0
        for row in tsvreader:
            #print(row)
            ct += 1
            slmf.update_progress(ct/line_ct)

            if float(row[4]) < 3.0: # skip rows with zscore < 3
                skip_ct += 1
                continue
            ensp = row[0]
            sym = row[1]
            k = "%s|%s"%(ensp,sym)
            if k in pmap:
                # we've already found it
                pids = pmap[k]
            elif k in notfnd:
                # we've already not found it
                continue
            else:
                # look it up
                pids = find_pids(dba, ensp, sym, pmap)
            if not pids:
                notfnd.add(k)
                continue
            for pid in pids:
                rv = dba.ins_compartment( {'protein_id': pid, 'ctype': 'JensenLab Text Mining',
                                        'go_id': row[2], 'go_term': row[3],
                                        'zscore': row[4], 'conf': row[5]
                                        } )
                if not rv:
                    dba_err_ct += 1
                    continue
                comp_ct += 1
                pmark[pid] = True
            
    
    print(" \n Inserted {} new compartment rows for {} proteins".format(comp_ct, len(pmark)))


    # Prediction channel
    fn =  FILE_P
    line_ct = slmf.wcl(fn)
    with open(fn, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        ct = 0
        pmark = {}
        comp_ct = 0
        skip_ct = 0
        notfnd = set()
        dba_err_ct = 0
        for row in tsvreader:
            ct += 1
            slmf.update_progress(ct/line_ct)

            if int(row[6]) < 3: # skip rows with conf < 3
                skip_ct += 1
                continue
            ensp = row[0]
            sym = row[1]
            k = "%s|%s"%(ensp,sym)
            if k in pmap:
                # we've already found it
                pids = pmap[k]
            elif k in notfnd:
                # we've already not found it
                continue
            else:
                # look it up
                pids = find_pids(dba, ensp, sym, pmap)
            if not pids:
                notfnd.add(k)
                continue
            for pid in pids:
                rv = dba.ins_compartment( {'protein_id': pid, 'ctype': 'JensenLab Prediction',
                                        'go_id': row[2], 'go_term': row[3],
                                        'evidence': "%s %s"%(row[4], row[5]), 
                                        'conf': row[6]} )
                if not rv:
                    dba_err_ct += 1
                    continue
                comp_ct += 1
                pmark[pid] = True

            
    
    print(" \n Inserted {} new compartment rows for {} proteins".format(comp_ct, len(pmark)))


    

            

def find_pids(dba, ensp, sym, k2pids):
  pids = []
  k = "%s|%s"%(ensp,sym)
  if k in k2pids:
    pids = k2pids[ensp]
  else:
    targets = dba.find_protein_ids({'stringid': ensp})
    if not targets:
      targets = dba.find_protein_ids({'sym': sym})
    if targets:
      for t in targets:
        pids.append(t)
      k2pids[k] = pids # save mapping - k2pids is pmap in load()
  return pids                    
            
      
  
  


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
    # dataset_id = dba.ins_dataset( {'name': 'Jensen Lab COMPARTMENTS', 'source': f'Files human_compartment_knowledge_full.tsv,human_compartment_experiments_full.tsv,human_compartment_textmining_full.tsv,human_compartment_predictions_full.tsv from {BASE_URL}', 'app': PROGRAM, 'app_version': __version__, 'url': 'http://compartments.jensenlab.org/', 'comments': 'Only input rows with confidence >= 3 are loaded.'} )
    # assert dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # # Provenance
    # rv = dba.ins_provenance({'dataset_id': dataset_id, 'table_name': 'compartment'})
    # assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)
    
    dataset_id =76
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    