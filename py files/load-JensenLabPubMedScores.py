#!/usr/bin/env python

"""

Usage:
    load-JensenLabPubMedScores.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-JensenLabPubMedScores.py -? | --help

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

DOWNLOAD_DIR = '../data/JensenLab/'
BASE_URL = 'http://download.jensenlab.org/KMC/'
FILENAME = 'protein_counts.tsv'



def load(args, dba, dataset_id, logger, logfile):
    ensp2pids = {}
    pmscores = {} # protein.id => sum(all scores)
    pms_ct = 0
    upd_ct = 0
    notfnd = {}
    dba_err_ct = 0
    infile = DOWNLOAD_DIR + FILENAME
    line_ct = slmf.wcl(infile)
    with open(infile, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        ct = 0
        for row in tsvreader:
            # sym  year  score
            ct += 1
            slmf.update_progress(ct/line_ct)
            if not row[0].startswith('ENSP'): continue
            ensp = row[0]
            if ensp in ensp2pids:
                # we've already found it
                pids = ensp2pids[ensp]
            elif ensp in notfnd:
                # we've already not found it
                continue
            else:
                targets = dba.find_protein_ids({'stringid': ensp})
                if not targets:
                    targets = dba.find_protein_ids_by_xref({'xtype': 'STRING', 'value': '9606.'+ensp})
                    if not targets:
                        notfnd[ensp] = True
                        logger.warning("No target found for {}".format(ensp))
                        continue
                pids = []
                for target in targets:
                    pids.append(target)
                ensp2pids[ensp] = pids # save this mapping so we only lookup each target once
            for pid in pids:
                rv = dba.ins_pmscore({'protein_id': pid, 'year': row[1], 'score': row[2]} )
                if rv:
                    pms_ct += 1
                else:
                    dba_err_ct += 1
                if pid in pmscores:
                    pmscores[pid] += float(row[2])
                else:
                    pmscores[pid] = float(row[2])
    print("\n{} input lines processed.".format(ct))
    print("\n  Inserted {} new pmscore rows for {} targets".format(pms_ct, len(pmscores)))
    if len(notfnd) > 0:
        print("\nNo target found for {} STRING IDs. See logfile {} for details.".format(len(notfnd), logfile))
    if dba_err_ct > 0:
        print("\nWARNING: {} DB errors occurred. See logfile {} for details.".format(dba_err_ct, logfile))
    
    print("\nLoading {} JensenLab PubMed Score tdl_infos".format(len(pmscores.keys())))
    ct = 0
    ti_ct = 0
    dba_err_ct = 0
    for pid,score in pmscores.items():
        ct += 1
        rv = dba.ins_tdl_info({'protein_id': pid, 'itype': 'JensenLab PubMed Score', 
                            'number_value': score} )
        if rv:
            ti_ct += 1
        else:
            dba_err_ct += 1
    print(f"total tdl into insert {ti_ct}")     
    

            
                    
            
      
  
  


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
    # dataset_id = dba.ins_dataset( {'name': 'JensenLab PubMed Text-mining Scores', 'source': 'File %s'%BASE_URL+FILENAME, 'app': PROGRAM, 'app_version': __version__, 'url': BASE_URL} )
    # if not dataset_id:
    #     print("WARNING: Error inserting dataset See logfile %s for details." % logfile)
    # # Provenance
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'pmscore'},
    #             {'dataset_id': dataset_id, 'table_name': 'tdl_info', 'where_clause': "itype = 'JensenLab PubMed Score'"} ]
    # for prov in provs:
    #     rv = dba.ins_provenance(prov)
    #     if not rv:
    #         print("WARNING: Error inserting provenance. See logfile %s for details." % logfile)
    #         sys.exit(1)
    
    dataset_id = 117
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    