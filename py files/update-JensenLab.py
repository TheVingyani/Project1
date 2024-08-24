#!/usr/bin/env python

"""

Usage:
    update-JensenLab.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    update-JensenLab.py -? | --help

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
FILE_K = '../data/JensenLab/human_disease_knowledge_filtered.tsv'
FILE_E = '../data/JensenLab/human_disease_experiments_filtered.tsv'
FILE_T = '../data/JensenLab/human_disease_textmining_filtered.tsv'

def load(args, dba, dataset_id, logger, logfile):

    # knowledge
    fn = FILE_K
    line_ct = slmf.wcl(fn)
    with open(fn, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        ct = 0
        pmark = {}
        notfnd = set()
        dis_ct = 0
        dba_err_ct = 0
        for row in tsvreader:
            slmf.update_progress(ct/line_ct)
            ct += 1
            ensp = row[0]
            sym = row[1]
            k = "%s|%s"%(ensp,sym)
            if k in notfnd:
                continue
            targets = dba.find_protein_ids({'stringid': ensp})
            if len(targets)==0:
                targets = dba.find_protein_ids({'sym': sym})
            if len(targets)==0:
                notfnd.add(k)
                logger.warn("No target found for {}".format(k))
                continue
            dtype = 'JensenLab Knowledge' 
            for t in targets:
                p = t
                pmark[p] = True
                init = {'protein_id': p, 'dtype': dtype, 'name': row[3],
                        'did': row[2], 'evidence': row[5], 'conf': row[6]}

                rv = dba.ins_disease(init)
                if not rv:
                    dba_err_ct += 1
                    continue
                dis_ct += 1
            
    print("Inserted {} new disease rows for {} proteins".format(dis_ct, len(pmark)))

    # Experiment channel
    fn = FILE_E
    line_ct = slmf.wcl(fn)
    with open(fn, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        ct = 0
        pmark = {}
        notfnd = set()
        dis_ct = 0
        skip_ct = 0
        dba_err_ct = 0
        for row in tsvreader:
            slmf.update_progress(ct/line_ct)
            ct += 1
            if row[6] == '0':
                # skip zero confidence rows
                skip_ct += 1
                continue
            ensp = row[0]
            sym = row[1]
            k = "%s|%s"%(ensp,sym)
            if k in notfnd:
                continue
            targets = dba.find_protein_ids({'stringid': ensp})
            if len(targets)==0:
                targets = dba.find_protein_ids({'sym': sym})
            if len(targets)==0:
                notfnd.add(k)
                logger.warn("No target found for {}".format(k))
                continue
            dtype = 'JensenLab Experiment'
            #print("targets",targets)
            for t in targets:
                p = t
                pmark[p] = True
                # ['ENSP00000000233', 'ARF5', 'DOID:535', 'Sleep disorder', 'TIGA', 'MeanRankScore = 11', '0.617']
                rv = dba.ins_disease( {'protein_id': p, 'dtype': dtype, 'name': row[3],
                                    'did': row[2], 'evidence': row[5], 'conf': row[6]} )
                if not rv:
                    dba_err_ct += 1
                    
                    continue
                
                dis_ct += 1
            
    print("Inserted {} new disease rows for {} proteins".format(dis_ct, len(pmark)))

    # text
    fn = FILE_T
    line_ct = slmf.wcl(fn)
    with open(fn, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        ct = 0
        pmark = {}
        notfnd = set()
        dis_ct = 0
        dba_err_ct = 0
        for row in tsvreader:
            slmf.update_progress(ct/line_ct)
            ct += 1
            ensp = row[0]
            sym = row[1]
            k = "%s|%s"%(ensp,sym)
            if k in notfnd:
                continue
            targets = dba.find_protein_ids({'stringid': ensp})
            if not targets:
                targets = dba.find_protein_ids({'sym': sym})
            if not targets:
                notfnd.add(k)
                logger.warn("No target found for {}".format(k))
                continue
            dtype = 'JensenLab Text Mining'
            for t in targets:
                p = t
                pmark[p] = True
                rv = dba.ins_disease( {'protein_id': p, 'dtype': dtype, 'name': row[3],
                                    'did': row[2], 'zscore': row[4], 'conf': row[5]} )
                if not rv:
                    dba_err_ct += 1
                    continue
                dis_ct += 1
            
    print("Inserted {} new disease rows for {} proteins".format(dis_ct, len(pmark)))
            
                    
            
      
  
  


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

    # dataset_id = dba.ins_dataset( {'name': 'JensenLab DISEASES', 'source': "Files human_disease_knowledge_filtered.tsv, human_disease_experiments_filtered.tsv, human_disease_textmining_filtered.tsv from http://download.jensenlab.org/", 'app': PROGRAM, 'app_version': __version__, 'url': 'http://diseases.jensenlab.org/','comments':'PubTator data was subjected to the same counting scheme used to generate JensenLab PubMed Scores.'} )
    # assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'disease','where_clause':"dtype LIKE 'JensenLab %'"}]
    # for prov in provs:
    #     rv=dba.ins_provenance(prov)
    #     assert rv, f"Error inserting the data into prov for {prov}"
    
    dataset_id = 55
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    