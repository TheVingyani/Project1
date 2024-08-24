#!/usr/bin/env python

"""

Usage:
    load-ReactomePPIs.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-ReactomePPIs.py -? | --help

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

DOWNLOAD_DIR = '../data/Reactome/'
BASE_URL = 'https://reactome.org/download/current/interactors/'
FILENAME = 'reactome.homo_sapiens.interactions.tab-delimited.txt'



def load(args, dba, dataset_id, logger, logfile):
    infile = DOWNLOAD_DIR + FILENAME
    line_ct = slmf.wcl(infile)

    with open(infile, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        header = tsvreader.__next__() # skip header line
        ct = 1
        skip_ct = 0
        same12_ct = 0
        dup_ct = 0
        ppis = {}
        ppi_ct = 0
        up2pid = {}
        notfnd = set()
        dba_err_ct = 0
        for row in tsvreader:
            # 0: Interactor 1 uniprot id
            # 1: Interactor 1 Ensembl gene id
            # 2: Interactor 1 Entrez Gene id
            # 3: Interactor 2 uniprot id
            # 4: Interactor 2 Ensembl gene id
            # 5: Interactor 2 Entrez Gene id
            # 6: Interaction type
            # 7: Interaction context Pubmed references
            ct += 1
            slmf.update_progress(ct/line_ct)
            if not row[0].startswith('uniprotkb:'):
                continue
            if not row[3].startswith('uniprotkb:'):
                continue
            up1 = row[0].replace('uniprotkb:', '')
            up2 = row[3].replace('uniprotkb:', '')      
            if not up1 or not up2:
                skip_ct += 1
                continue
            # protein1
            if up1 in up2pid:
                pid1 = up2pid[up1]
            elif up1 in notfnd:
                continue
            else:
                t1 = find_target(dba, up1)
                if not t1:
                    notfnd.add(up1)
                    continue
                pid1 = t1
                up2pid[up1] = pid1
            # protein2
            if up2 in up2pid:
                pid2 = up2pid[up2]
            elif up2 in notfnd:
                continue
            else:
                t2 = find_target(dba, up2)
                if not t2:
                    notfnd.add(up2)
                    continue
                pid2 = t2
                up2pid[up2] = pid2
            int_type = row[6]
            ppik = up1 + "|" + up2 + 'int_type'
            if ppik in ppis:
                dup_ct += 1
                continue
            if pid1 == pid2:
                same12_ct += 1
                continue
            # Insert PPI
            rv = dba.ins_ppi( {'ppitype': 'Reactome', 'interaction_type': int_type,
                                'protein1_id': pid1, 'protein1_str': up1,
                                'protein2_id': pid2, 'protein2_str': up2} )
            if rv:
                ppi_ct += 1
                ppis[ppik] = True
            else:
                dba_err_ct += 1

            

def find_target(dba, up):
  targets = dba.find_protein_ids({'uniprot': up})
  if targets:
    return targets[0]
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

     # Dataset
    # dataset_id = dba.ins_dataset( {'name': 'Reactome Protein-Protein Interactions', 'source': "File %s"%BASE_URL+FILENAME, 'app': PROGRAM, 'app_version': __version__, 'url': 'http://www.reactome.org/'} )
    # if not dataset_id:
    #     print("WARNING: Error inserting dataset See logfile %s for details." % logfile)
    #     sys.exit(1)
    # # Provenance
    # rv = dba.ins_provenance({'dataset_id': dataset_id, 'table_name': 'ppi', 'where_clause': "ppitype = 'Reactome'"})
    # if not rv:
    #     print("WARNING: Error inserting provenance. See logfile %s for details." % logfile)
    #     sys.exit(1)
    
    dataset_id = 111
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    