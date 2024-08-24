#!/usr/bin/env python

"""

Usage:
    update-TIGA.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    update-TIGA.py -? | --help

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

DOWNLOAD_DIR = '../data/TIGA/'
BASE_URL = 'https://unmtid-shinyapps.net/download/TIGA/latest/'
TIGA_FILE = 'tiga_gene-trait_stats.tsv.gz'
TIGA_PROV_FILE = 'tiga_gene-trait_provenance.tsv.gz'



def load(args, dba, dataset_id, logger, logfile):
    infile = DOWNLOAD_DIR + TIGA_FILE.replace('.gz', '')
    line_ct = slmf.wcl(infile)
    print("\nProcessing {} lines in TIGA file {}".format(line_ct, infile))

    ct = 0
    k2pids = defaultdict(list) # map sym|ENSG to TCRD pids
    notfnd = set()
    dba_err_ct = 0
    pmark = {}
    tiga_ct = 0
    with open(infile, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        header = tsvreader.__next__() # skip header line
        print(header)
        ct += 1
        for row in tsvreader:
            # ['ensemblId', 'efoId', 'trait', 'n_study', 'n_snp', 'n_snpw', 'geneNtrait', 'geneNstudy', 'traitNgene', 'traitNstudy', 
            #  'pvalue_mlog_median', 'pvalue_mlog_max', 'or_median', 'n_beta', 'study_N_mean', 'rcras', 'geneSymbol', 'TDL', 'geneFamily', 'geneIdgList', 'geneName', 'meanRank', 'meanRankScore']
            # 0: ensemblId
            # 1: efoId
            # 2: trait
            # 3: n_study
            # 4: n_snp
            # 5: n_snpw
            # 6: geneNtrait
            # 7: geneNstudy
            # 8: traitNgene
            # 9: traitNstudy
            # 10: pvalue_mlog_median
            # 11: or_median
            # 12: n_beta
            # 13: study_N_mean
            # 14: rcras
            # 15: geneSymbol
            # 16: geneIdgTdl
            # 17: geneFamily
            # 18: geneIdgList
            # 19: geneName
            # 20: meanRank
            # 21: meanRankScore
            ct += 1
            slmf.update_progress(ct/line_ct)
            sym = row[15]
            ensg = row[0]
            #ensg = re.sub('\.\d+$', '', row[0]) # get rid of version if present
            k = sym + '|' + ensg
            if k in k2pids:
            # we've already found it
                pids = k2pids[k]
            elif k in notfnd:
            # we've already not found it
                continue
            else:
            # look it up
                targets = dba.find_protein_ids({'sym': sym}, False)
                if not targets:
                    targets = dba.find_protein_ids_by_xref({'xtype': 'Ensembl', 'value': ensg})
                if not targets:
                    notfnd.add(k)
                    continue
            pids = []
            for t in targets:
                pids.append(t)
            k2pids[ensg] = pids # save this mapping so we only lookup each target once
            ormed = None
            if row[12] != 'NA':
                ormed = row[12]
            init = {'ensg': row[0], 'efoid': row[1], 'trait': row[2], 'n_study': row[3], 'n_snp': row[4],
                    'n_snpw': row[5], 'geneNtrait': row[6], 'geneNstudy': row[7], 'traitNgene': row[8],
                    'traitNstudy': row[9], 'pvalue_mlog_median': row[10], 'or_median': ormed,
                    'n_beta': row[13], 'study_N_mean': row[14], 'rcras': row[15], 'meanRank': row[21],
                    'meanRankScore': row[22]}
            for pid in pids:
                init['protein_id'] = pid
                rv = dba.ins_tiga(init)
                if not rv:
                    dba_err_ct += 1
                    continue
                tiga_ct += 1
                pmark[pid] = True

            

    for k in notfnd:
        logger.warn("No target found for {}".format(k))
    print("Processed {} lines".format(ct))
    print("  Inserted {} new tiga rows for {} proteins".format(tiga_ct, len(pmark)))
    if notfnd:
        print("  No target found for {} ENSGs. See logfile {} for details.".format(len(notfnd), logfile))
    if dba_err_ct > 0:
        print("WARNING: {} DB errors occurred. See logfile {} for details.".format(dba_err_ct, logfile))

    infile = DOWNLOAD_DIR + TIGA_PROV_FILE.replace('.gz', '')
    line_ct = slmf.wcl(infile)
    print("\nProcessing {} lines in TIGA provenance file {}".format(line_ct, infile))
    ct = 0
    tigaprov_ct = 0
    dba_err_ct = 0
    with open(infile, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        header = tsvreader.__next__() # skip header line
        ct += 1
        for row in tsvreader:
            # 0: ensemblId
            # 1: TRAIT_URI
            # 2: STUDY_ACCESSION
            # 3: PUBMEDID
            # 4: efoId
            ct += 1
            rv = dba.ins_tiga_provenance( {'ensg': row[0], 'efoid': row[4],
                                            'study_acc': row[2], 'pubmedid': row[3]} )
            if not rv:
                dba_err_ct += 1
                continue
            tigaprov_ct += 1
            slmf.update_progress(ct/line_ct)
            

    print("Processed {} lines".format(ct))
    print("  Inserted {} new tiga rows".format(tigaprov_ct))
    if dba_err_ct > 0:
        print("WARNING: {} DB errors occurred. See logfile {} for details.".format(dba_err_ct, logfile))  


            
                    
            
      
  
  


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
    # dataset_id = dba.ins_dataset( {'name': 'TIGA', 'source': 'IDG-KMC generated data by Jeremy Yang at UNM .', 'app': PROGRAM, 'app_version': __version__, 'url': 'https://unmtid-shinyapps.net/shiny/tiga/'} )
    # assert dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # # Provenance
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'tiga'},
    #             {'dataset_id': dataset_id, 'table_name': 'tiga_provenance'} ]
    # for prov in provs:
    #     rv = dba.ins_provenance(prov)
    #     assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)
    
    dataset_id = 121
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    