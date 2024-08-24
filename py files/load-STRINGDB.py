#!/usr/bin/env python

"""

Usage:
    load-STRINGDB.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-STRINGDB.py -? | --help

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

FILE ='../data/CTD/CTD_genes_diseases.tsv'
# INFILE = '../data/JensenLab/9606.protein.links.v10.5.txt'
# https://stringdb-static.org/download/protein.links.v11.5/9606.protein.links.v11.0.txt.gz
INFILE = '../data/STRING/9606.protein.links.v11.5.txt'


def load(args, dba, dataset_id, logger, logfile):
    line_ct = slmf.wcl(INFILE)
    ct = 0
    # So we only look up target(s) for each ENSP once,
    # save mapping of ENSP to list of (pid, sym) tuples
    ensp2pids = defaultdict(list)
    same12_ct = 0
    notfnd = set()
    ppi_ct = 0
    dba_err_ct = 0
    with open(INFILE, 'r') as ifh:
        for line in ifh:
        # protein1 protein2 combined_score
            line.rstrip('\n')
            ct += 1
            slmf.update_progress(ct/line_ct)
            if ct == 1:
                # skip header line
                continue
            [ensp1, ensp2, score] = line.split()
            ensp1 = ensp1.replace('9606.', '')
            ensp2 = ensp2.replace('9606.', '')
            # ENSP1
            if ensp1 in ensp2pids:
                p1s = ensp2pids[ensp1]
            elif ensp1 in notfnd:
                continue
            else:
                targets = find_targets(dba, ensp1)
                if not targets:
                    notfnd.add(ensp1)
                    continue
                p1s = []
                for t in targets:
                    p = t
                    #print(p)
                    p1s.append( (p['id'], p['sym']) )
                ensp2pids[ensp1] = p1s
            # ENSP2
            if ensp2 in ensp2pids:
                p2s = ensp2pids[ensp2]
            elif ensp2 in notfnd:
                continue
            else:
                print(ensp2)
                targets = find_targets(dba, ensp2)
                if not targets:
                    notfnd.add(ensp2)
                    continue
                p2s = []
                for t in targets:
                    p = t
                    #print(p)
                    p2s.append( (p['id'], p['sym']) )
                ensp2pids[ensp2] = p2s
            # Insert PPI(s)
            for p1 in p1s:
                for p2 in p2s:
                    if p1[0] == p2[0]:
                        same12_ct += 1
                        continue
                    rv = dba.ins_ppi( {'ppitype': 'STRINGDB', 
                                        'protein1_id': p1[0], 'protein1_str': p1[1],
                                        'protein2_id': p2[0], 'protein2_str': p2[1], 'score': score} )
                    if rv:
                        ppi_ct += 1
                    else:
                        dba_err_ct += 1
            
    print(f"inserted {ppi_ct} into ppi")

def find_targets(dba, ensp):
  targets = dba.find_proteins({'stringid': ensp})
  if not targets:
    targets = dba.find_proteins_by_xref({'xtype': 'STRING', 'value': '9606.'+ensp})
  if targets:
    return targets
  else:
    return None     
    

            
                    
            
      
  
  


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

    # # Dataset
    # dataset_id = dba.ins_dataset( {'name': 'STRINGDB', 'source': 'File %s from https://stringdb-static.org/download/protein.links.v11.5/'%(os.path.basename(INFILE)), 'app': PROGRAM, 'app_version': __version__, 'url': 'http://string-db.org/'} )
    # assert dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # # Provenance
    # rv = dba.ins_provenance({'dataset_id': dataset_id, 'table_name': 'ppi', 'where_clause': 'ppitype = "STRINGDB"'})
    # assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)


    
    dataset_id = 114
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    