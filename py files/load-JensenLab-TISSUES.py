#!/usr/bin/env python

"""

Usage:
    load-eRAM.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>] [--datalevel=<int>]
    load-eRAM.py -? | --help

Options:
  -dl --datalevel DATALEVEL : 1 or 2 or 3 or 4
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
import decimal


__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"


FILE_K = '../data/JensenLab/human_tissue_knowledge_filtered.tsv'

FILE_T = '../data/JensenLab/human_tissue_textmining_filtered.tsv'

FILE_E1 = '../data/JensenLab/human_tissue_experiments_full_1.tsv'
FILE_E2 = '../data/JensenLab/human_tissue_experiments_full_2.tsv'
FILE_E3 = '../data/JensenLab/human_tissue_experiments_full_3.tsv'
FILE_E4 = '../data/JensenLab/human_tissue_experiments_full_4.tsv'

def process_row(row, dba, pmap):
    notfnd = set()
    nouid = set()
    pmark = {}
    sym = row[1].split()[0] if ' ' in row[1] else row[1]
    k = f"{row[0]}|{sym}"  # ENSP|sym
    if k in notfnd:
        return
    try:
        pids = find_pids(dba, k, pmap)
    except ValueError:
        print(f"[ERROR] Row: {str(row)}; k: {k}")
        return

    if not pids:
        notfnd.add(k)
        return

    etype = 'JensenLab Experiment ' + row[4]
    init = {
        'etype': etype,
        'tissue': row[3],
        'string_value': row[5],
        'oid': row[2],
        'conf': row[6]
    }

    uberon_id = dba.get_uberon_id(row[3])
    if uberon_id:
        init['uberon_id'] = uberon_id[0]
    else:
        nouid.add(row[3])

    for pid in pids:
        pmark[pid] = True
        init['protein_id'] = pid
        rv = dba.ins_expression(init)
        if not rv:
            dba_err_ct += 1

def find_pids(dba, k, k2pids):
  # k is 'ENSP|sym'
  pids = []
  if k in k2pids:
    pids = k2pids[k]
  else:
    
    (ensp, sym) = k.split("|")
    # First try to find target(s) by stringid - the most reliable way
    targets = dba.find_protein_ids({'stringid': ensp})
    if targets:
        pids=targets
        k2pids[k] = pids
        return pids
    if not targets:
      # Next, try by symbol
      targets = dba.find_protein_ids({'sym': sym})
      if targets:
        pids=targets
        k2pids[k] = pids
        return pids
    if not targets:
      # Finally, try by Ensembl xref
      targets = dba.find_protein_ids_by_xref({'xtype': 'Ensembl', 'value': ensp})
      if targets:
        pids=targets
        k2pids[k] = pids
        return pids
  return pids

def load(args, dba, dataset_id, logger, logfile):
    # Knowledge channel
    fn = FILE_K
    pmap = {}
    line_ct = slmf.wcl(fn)
    with open(fn, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        ct = 0
        pmark = {}
        exp_ct = 0
        notfnd = set()
        nouid = set()
        dba_err_ct = 0
        for row in tsvreader:
            break
            #print(row)
            ct += 1
            slmf.update_progress(ct/line_ct)
            k = "%s|%s" % (row[0], row[1]) # ENSP|sym
            if k in notfnd:
                continue
            pids = find_pids(dba, k, pmap)
            #print(pids)
            if not pids:
                notfnd.add(k)
                continue
            etype = 'JensenLab Knowledge ' + row[4]
            init = {'etype': etype, 'tissue': row[3],'boolean_value': 1, 
                    'oid': row[2], 'evidence': row[5], 'conf': row[6]}
            # Add Uberon ID, if we can find one
            uberon_id = dba.get_uberon_id(row[3])
            # if not uberon_id and row[3] in tiss2uid:
            #     uberon_id = tiss2uid[row[3]]
            if uberon_id:
                init['uberon_id'] = uberon_id[0]
            else:
                nouid.add(row[3])
            for pid in pids:
                init['protein_id'] = pid
                rv = dba.ins_expression(init)
                if not rv:
                    dba_err_ct += 1
                    continue
                exp_ct += 1
                pmark[pid] = True
            
            
    print(f"total knowledge data {exp_ct}")

    # Experiment channel
    datalvl= int(args['--datalevel'])
    print(datalvl)
    print(type(datalvl))
    if datalvl==1:
       FILE_E=FILE_E1
    if datalvl==2:
       FILE_E=FILE_E2
    if datalvl==3:
       FILE_E=FILE_E3
    if datalvl==4:
       FILE_E=FILE_E4 
    fn = FILE_E
    line_ct = slmf.wcl(fn)
    with open(fn, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        ct = 0
        pmark = {}
        exp_ct = 0
        notfnd = set()
        nouid = set()
        skip_ct = 0
        dba_err_ct = 0
        for row in tsvreader:
            ct += 1
            slmf.update_progress(ct / line_ct)
            if row[6] == '0':
                skip_ct += 1
                continue
            process_row(row, dba, pmap)
            
            
            
    print(f"total experiment data {exp_ct}")

    # Text Mining channel
    fn = FILE_T
    line_ct = slmf.wcl(fn)
    with open(fn, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        ct = 0
        pmark = {}
        exp_ct = 0
        notfnd = set()
        nouid = set()
        dba_err_ct = 0
        #print(tsvreader)
        for row in tsvreader:
            break
            #print(row)
            ct += 1
            slmf.update_progress(ct/line_ct)

            k = "%s|%s" % (row[0], row[1]) # ENSP|sym
            if k in notfnd:
                continue
            pids = find_pids(dba, k, pmap)
            if not pids:
                notfnd.add(k)
                logger.warn("No target found for {}".format(k))
                continue
            etype = 'JensenLab Text Mining'
            init = {'etype': etype, 'tissue': row[3], 'boolean_value': 1,
                    'oid': row[2], 'zscore': row[4], 'conf': row[5], 'url': row[6]}
            # Add Uberon ID, if we can find one
            if row[3]:
                uberon_id = dba.get_uberon_id(row[3])
            # if not uberon_id and row[3] in tiss2uid:
            #     uberon_id = tiss2uid[row[3]]
            if uberon_id:
                init['uberon_id'] = uberon_id[0]
            else:
                nouid.add(row[3])
            for pid in pids:

                pmark[pid] = True
                init['protein_id'] = pid
                rv = dba.ins_expression(init)
                if not rv:
                    dba_err_ct += 1
                    continue
                exp_ct += 1
    print(f"total text mining data {exp_ct}")

                 
        

            
                    
            
      
  
  


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

    # dataset_id = dba.ins_dataset( {'name': 'JensenLab TISSUES', 'source': "Files human_tissue_knowledge_filtered.tsv, human_tissue_experiments_filtered.tsv, human_tissue_textmining_filtered.tsv from http://download.jensenlab.org/", 'app': PROGRAM, 'app_version': __version__, 'url': 'http://tissues.jensenlab.org/'} )
    # assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'expression','where_clause':"type LIKE 'JensenLab %'"}]
    # for prov in provs:
    #     rv=dba.ins_provenance(prov)
    #     assert rv, f"Error inserting the data into prov for {prov}"
    
    dataset_id = 63
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    