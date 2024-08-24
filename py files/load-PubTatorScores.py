#!/usr/bin/env python

"""

Usage:
    load-PubTatorScores.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-PubTatorScores.py -? | --help

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
FILE='../data/PubTator/pubtator_counts.tsv'

TAXIDS = [9606, 10090, 10116]
def load(args, dba, dataset_id, logger, logfile):
    ptscores = {}
    fn = FILE
    line_ct = slmf.wcl(fn)
    i=0
    x = 0
    print("line_ct",line_ct)
    with open(fn, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        ct = 0
        pts_ct = 0
        dba_err_ct = 0
        geneid2pid = {}
        notfnd = set()
        for row in tsvreader:
            slmf.update_progress(ct/line_ct)
            # NCBI Gene ID  year  score
            ct += 1
            gidstr = row[0].replace(',', ';')
            geneids = gidstr.split(';')
            for geneid in geneids:
                if not geneid or '(tax:' in geneid:
                    continue
                if geneid in geneid2pid:
                # we've already found it
                    pids = geneid2pid[geneid]
                elif geneid in notfnd:
                # we've already not found it
                    continue
                else:
                    targets = dba.find_protein_ids({'geneid': geneid})
                    if len(targets)==0:
                        notfnd.add(geneid)
                        logger.warn("No target found for {}".format(geneid))
                        continue
                    pids = []
                    for target in targets:
                        pids.append(target)
                        geneid2pid[geneid] = pids # save this mapping so we only lookup each target once
                for pid in pids:
                    rv = dba.ins_ptscore({'protein_id': pid, 'year': row[1], 'score': row[2]} )
                    if rv:
                        pts_ct += 1
                    else:
                        dba_err_ct += 1
                    if pid in ptscores:
                        ptscores[pid] += float(row[2])
                    else:
                        ptscores[pid] = float(row[2])
            
    ct = 0
    ti_ct = 0
    dba_err_ct = 0
    for pid,score in ptscores.items():
        ct += 1
        rv = dba.ins_tdl_info({'protein_id': pid, 'itype': 'PubTator Score', 
                            'number_value': score} )
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

    # dataset_id = dba.ins_dataset( {'name': 'PubTator Text-mining Scores', 'source': "File https://download.jensenlab.org/KMC/pubtator_counts.tsv", 'app': PROGRAM, 'app_version': __version__, 'url': 'https://www.ncbi.nlm.nih.gov/CBBresearch/Lu/Demo/PubTator/','comment':'PubTator data was subjected to the same counting scheme used to generate JensenLab PubMed Scores.'} )
    # assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'tdl_info','where_clause':'itype=PubTator PubMed Score'},
    # {'dataset_id': dataset_id, 'table_name': 'ptscore'}]
    # for prov in provs:
    #     rv=dba.ins_provenance(prov)
    #     assert rv, f"Error inserting the data into prov for {prov}"
    dataset_id = 54
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")