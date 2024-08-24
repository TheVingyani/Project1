#!/usr/bin/env python

"""

Usage:
    load-TFs.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-TFs.py -? | --help

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

DOWNLOAD_DIR = '../data/UToronto/'
BASE_URL = 'http://humantfs.ccbr.utoronto.ca/download.php'
FILENAME = 'DatabaseExtract_v_1.01.csv'



def load(args, dba, dataset_id, logger, logfile):
    TDLs = {'Tdark': 0, 'Tbio': 0, 'Tchem': 0, 'Tclin': 0}

    ifn = DOWNLOAD_DIR + FILENAME
    line_ct = slmf.wcl(ifn)
    with open(ifn, 'r') as ifh:
        csvreader = csv.reader(ifh)
        header = csvreader.__next__() # skip header line
        ct = 0
        ti_ct = 0
        skip_ct = 0
        notfnd = set()
        dba_err_ct = 0
        for row in csvreader:    
            # 0 Ensembl ID
            # 1 HGNC symbol
            # 2 DBD
            # 3 Is TF?
            # 4 TF assessment
            # 5 Binding mode,Motif status
            # 6 Final Notes
            # 7 Final Comments
            # 8 Interpro ID(s)
            # 9 EntrezGene ID
            # 10 EntrezGene Description
            # 11 PDB ID
            # 12 TF tested by HT-SELEX?
            # 13 TF tested by PBM?
            # 14 Conditional Binding Requirements
            # 15 Original Comments
            # 16 Vaquerizas 2009 classification
            # 17 CisBP considers it a TF?
            # 18 TFCat classification
            # 19 Is a GO TF?
            # 20 Initial assessment
            # 21 Curator 1
            # 22 Curator 2
            # 23 TFclass considers
            ct += 1
            slmf.update_progress(ct/line_ct)
            #print(row )
            #print(row[3],row[1],row[9],row[0])
            if row[4] != 'Yes':
                skip_ct += 1
                continue
            sym = row[2]
            targets = dba.find_protein_ids({'sym': sym})
            if not targets:
                gid = row[10]
                if gid != 'None' and not gid.startswith('IPR'):
                    targets = dba.find_protein_ids({'geneid': gid})
            if not targets:
                ensg = row[1]
                targets = dba.find_protein_ids_by_xref({'xtype': 'Ensembl', 'value': ensg})
            if not targets:
                k = "%s|%s|%s"%(sym,gid,ensg)
                notfnd.add(k)
                continue
            t = targets[0]
            pid = t
            rv = dba.ins_tdl_info({'protein_id': pid, 'itype': 'Is Transcription Factor', 
                                    'boolean_value': 1} )
            if rv:
                ti_ct += 1
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
    # dataset_id = dba.ins_dataset( {'name': 'Transcription Factor Flags', 'source': BASE_URL+FILENAME, 'app': PROGRAM, 'app_version': __version__, 'url': 'http://humantfs.ccbr.utoronto.ca/'} )
    # assert dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # # Provenance
    # rv = dba.ins_provenance({'dataset_id': dataset_id, 'table_name': 'tdl_info', 'where_clause': "itype = 'Is Transcription Factor'"})
    # assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)
    
    dataset_id = 103
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    