#!/usr/bin/env python

"""

Usage:
    load-HomoloGene.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-HomoloGene.py -? | --help

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
FILE='../data/HomoloGene/homologene.txt'

TAXIDS = [9606, 10090, 10116]
def load(args, dba, dataset_id, logger, logfile):
    fn = FILE
    line_ct = slmf.wcl(fn)
    i=0
    x = 0
    print("line_ct",line_ct)
    with open(fn, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        ct = 0
        skip_ct = 0
        for row in tsvreader:
            
            ct +=1
            slmf.update_progress(ct/line_ct)
            # homologene_group_id    tax_id    ncbi_gene_id    symbol    protein_gi    ref_seq
            # print(row)
            # print('\n',row[1])
            # print(type(row[1][0]))
            taxid = int(row[1])
            
            if taxid not in TAXIDS:
                skip_ct += 1
                continue
            
            #print(x)
            if taxid == 9606:
                targets = dba.find_protein_ids({'geneid': row[2]})
                #print(targets)
                if len(targets)==0:
                    #print('geneid',row[2])
                    logger.warn("No target found for {}".format(row))
                    continue
                for p in targets:
                    x +=1
                    rv = dba.ins_homologene({'protein_id': p, 'groupid': int(row[0]), 'taxid': taxid})
                    if not rv:
                        print("error in sql",{'protein_id': p, 'groupid': int(row[0]), 'taxid': taxid})
            else:
                nhproteins = dba.find_nhprotein_ids({'geneid': row[2]})
                if len(nhproteins)==0:
                    #print('geneid',row[2])
                    logger.warn("No nhprotein found for {}".format(row))
                    continue
                for nhp in nhproteins:
                    x +=1
                    rv = dba.ins_homologene({'nhprotein_id': nhp, 'groupid': int(row[0]), 'taxid': taxid})
                    if not rv:
                        print("error in sql",{'nhprotein_id': nhp, 'groupid': int(row[0]), 'taxid': taxid})
            

    print(x)

            
      
  
  


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

    # dataset_id = dba.ins_dataset( {'name': 'HomoloGene', 'source': "File ftp://ftp.ncbi.nih.gov/pub/HomoloGene/current/homologene.data", 'app': PROGRAM, 'app_version': __version__, 'url': 'https://www.ncbi.nlm.nih.gov/homologene','comment':'Only Human, Mouse and Rat members of HomoloGene groups are loaded. These relate protein to nhprotein.'} )
    # assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    dataset_id=50
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'homology'}]
    # for prov in provs:
    #     rv=dba.ins_provenance(prov)
    #     assert rv, f"Error inserting the data into prov for {prov}"
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")