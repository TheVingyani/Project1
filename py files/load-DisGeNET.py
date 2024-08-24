#!/usr/bin/env python

"""

Usage:
    load-DisGeNET.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-DisGeNET.py -? | --help

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

#FILE ='../data/CTD/CTD_genes_diseases.tsv'
BASE_URL = 'https://www.disgenet.org/static/disgenet_ap1/files/downloads/curated_gene_disease_associations.tsv.gz'
INPUT_FILE = '../data/disgenet/curated_gene_disease_associations.tsv'



def load(args, dba, dataset_id, logger, logfile):
    infile = INPUT_FILE
    line_ct = slmf.wcl(infile)
    with open(infile, 'r') as f:
        ct = 0
        k2pids = {}
        pmark = {}
        notfnd = set()
        dis_ct = 0
        dba_err_ct = 0
        for line in f:
            # 0: geneId
            # 1: geneSymbol
            # 2: DSI
            # 3: DPI
            # 4: diseaseId
            # 5: diseaseName
            # 6: diseaseType
            # 7: diseaseClass
            # 8: diseaseSemanticType
            # 9: score
            # 10: EI
            # 11: YearInitial
            # 12: YearFinal
            # 13: NofPmids
            # 14: NofSnps
            # 15: source
            ct += 1
            slmf.update_progress(ct/line_ct)
            if line.startswith('#'):
                continue
            if line.startswith('geneId'):
                # header row
                continue
            data = line.split('\t')
            geneid = data[0].strip()
            sym = data[1]
            k = "%s|%s"%(sym,geneid)
            if k in k2pids:
                # we've already found it
                pids = k2pids[k]
            elif k in notfnd:
                # we've already not found it
                continue
            else:
                targets = dba.find_protein_ids({'sym': sym})
                if not targets:
                    targets = dba.find_protein_ids({'geneid': geneid})
                if not targets:
                    notfnd.add(k)
                    logger.warn("No target found for {}".format(k))
                    continue
                pids = []
                for t in targets:
                    p = t
                    pmark[p] = True
                    pids.append(p)
                k2pids[k] = pids # save this mapping so we only lookup each target once
            pmid_ct = data[13].strip()
            snp_ct = data[14].strip()
            if pmid_ct != '0':
                if snp_ct != '0':
                    ev = "%s PubMed IDs; %s SNPs"%(pmid_ct, snp_ct)
                else:
                    ev = "%s PubMed IDs"%pmid_ct
            else:
                ev = "%s SNPs"%snp_ct
            for pid in pids:
                rv = dba.ins_disease( {'protein_id': pid, 'dtype': 'DisGeNET', 'name': data[5],
                                    'did': data[4], 'score': data[9], 'source': data[15].strip(),
                                    'evidence': ev} )
                if not rv:
                    dba_err_ct += 1
                    continue
                dis_ct += 1

    print("\nLoaded {} new disease rows for {} proteins.".format(dis_ct, len(pmark)))     
        

            
                    
            
      
  
  


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
    # dataset_id = dba.ins_dataset( {'name': 'DisGeNET Disease Associations', 'source': 'File %s from %s.'%(INPUT_FILE, BASE_URL), 'app': PROGRAM, 'app_version': __version__, 'url': 'https://www.disgenet.org/static/disgenet_ap1/files/downloads/curated_gene_disease_associations.tsv.gz'} )
    # assert dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # # Provenance
    # rv = dba.ins_provenance({'dataset_id': dataset_id, 'table_name': 'disease', 'where_clause': "dtype = 'DisGeNET'"})
    # assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)
    
    dataset_id = 109
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    