#!/usr/bin/env python

"""

Usage:
    load-GWASCatalog.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-GWASCatalog.py -? | --help

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
import re


__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"

FILE ='../data/GWAS/gwas_catalog_v1.0.2-associations_e110_r2023-09-25.tsv'



def load(args, dba, dataset_id, logger, logfile):
    line_ct = slmf.wcl(FILE)
    line_ct -= 1 
    outlist = []
    with open(FILE, 'r',encoding='utf-8') as tsvfile:
        tsvreader = csv.reader(tsvfile, delimiter='\t')
        header = tsvreader.__next__() # skip header line
        ct = 0
        notfnd = set()
        pmark = {}
        gwas_ct = 0
        dba_err_ct = 0
        # 0: DATE ADDED TO CATALOG
        # 1: PUBMEDID
        # 2: FIRST AUTHOR
        # 3: DATE
        # 4: JOURNAL
        # 5: LINK
        # 6: STUDY
        # 7: DISEASE/TRAIT
        # 8: INITIAL SAMPLE SIZE
        # 9: REPLICATION SAMPLE SIZE
        # 10: REGION
        # 11: CHR_ID
        # 12: CHR_POS
        # 13: REPORTED GENE(S)
        # 14: MAPPED_GENE
        # 15: UPSTREAM_GENE_ID
        # 16: DOWNSTREAM_GENE_ID
        # 17: SNP_GENE_IDS
        # 18: UPSTREAM_GENE_DISTANCE
        # 19: DOWNSTREAM_GENE_DISTANCE
        # 20: STRONGEST SNP-RISK ALLELE
        # 21: SNPS
        # 22: MERGED
        # 23: SNP_ID_CURRENT
        # 24: CONTEXT
        # 25: INTERGENIC
        # 26: RISK ALLELE FREQUENCY
        # 27: P-VALUE
        # 28: PVALUE_MLOG
        # 29: P-VALUE (TEXT)
        # 30: OR or BETA
        # 31: 95% CI (TEXT)
        # 32: PLATFORM [SNPS PASSING QC]
        # 33: CNV
        # 34: MAPPED_TRAIT
        # 35: MAPPED_TRAIT_URI
        # 36: STUDY ACCESSION
        # 37: GENOTYPING TECHNOLOGY
        symregex = re.compile(r' ?[-,;] ?')
        for row in tsvreader:
            #print(header)
            ct += 1
            if len(row) < 14: continue
            symstr = row[14]
            if symstr == 'NR': continue
            symlist = symregex.split(symstr)
            for sym in symlist:
                if sym in notfnd:
                    continue
                targets = dba.find_protein_ids({'sym': sym})
                if not targets:
                    notfnd.add(sym)
                    logger.warn("No target found for symbol {}".format(sym))
                    continue
                for t in targets:
                    p = t
                    try:
                        pval = float(row[27])
                    except:
                        pval = None
                    try:
                        orbeta = float(row[30])
                    except:
                        orbeta = None
                    if row[25]:
                        ig = int(row[25])
                    else:
                        ig = None
                    rv = dba.ins_gwas({'protein_id': p, 'disease_trait': row[7], 'snps': row[21],
                                        'pmid': row[1], 'study': row[6], 'context': row[24], 'intergenic': ig,
                                        'p_value': pval, 'or_beta': orbeta, 'cnv': row[33],
                                        'mapped_trait': row[34], 'mapped_trait_uri': row[35]})
                    if not rv:
                        dba_err_ct += 1
                        continue
                    pmark[p] = True
                    gwas_ct += 1
            slmf.update_progress(ct/line_ct)
            
                
    

            
                    
            
      
  
  


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

    # dataset_id = dba.ins_dataset( {'name': 'GWAS Catalog', 'source': "File gwas_catalog_v1.0.2-associations_e96_r2019-04-06.tsv from http://www.ebi.ac.uk/gwas/docs/file-downloads", 'app': PROGRAM, 'app_version': __version__, 'url': 'https://www.ebi.ac.uk/gwas/home'} )
    # assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'gwas'}]
    # for prov in provs:
    #     rv=dba.ins_provenance(prov)
    #     assert rv, f"Error inserting the data into prov for {prov}"
    
    dataset_id = 61
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    