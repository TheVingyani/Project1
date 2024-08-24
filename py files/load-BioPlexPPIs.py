#!/usr/bin/env python

"""

Usage:
    load-BioPlexPPIs.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-BioPlexPPIs.py -? | --help

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

BIOPLEX_FILE = '../data/BioPlex/BioPlex_interactionList_v4a.tsv'
UPD_FILES = ['../data/BioPlex/interactome_update_Dec2015.tsv',
             '../data/BioPlex/interactome_update_May2016.tsv',
             '../data/BioPlex/interactome_update_Aug2016.tsv',
             '../data/BioPlex/interactome_update_Dec2016.tsv',
             '../data/BioPlex/interactome_update_April2017.tsv',
             '../data/BioPlex/interactome_update_Nov2017.tsv']
SRC_FILES = [os.path.basename(BIOPLEX_FILE)] + [os.path.basename(f) for f in UPD_FILES]



def load(args, dba, dataset_id, logger, logfile):
    f = BIOPLEX_FILE
    line_ct = slmf.wcl(f)
    line_ct -= 1
     
    with open(f, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        header = tsvreader.__next__() # skip header line
        # GeneA   GeneB   UniprotA        UniprotB        SymbolA SymbolB pW      pNI     pInt
        ct = 0
        ppi_ct = 0
        same12_ct = 0
        k2pid = {}
        notfnd = set()
        dba_err_ct = 0
        for row in tsvreader:
            ct += 1
            slmf.update_progress(ct/line_ct)
            geneid1 = row[0]
            geneid2 = row[1]
            up1 = row[2]
            up2 = row[3]
            sym1 = row[4]
            sym2 = row[5]
            pw = row[6]
            pni = row[7]
            pint = row[8]
            # protein1
            k1 = "%s|%s|%s" % (up1, sym1, geneid1)
            if k1 in k2pid:
                pid1 = k2pid[k1]
            elif k1 in notfnd:
                continue
            else:
                t1 = find_target(dba, k1)
            if not t1:
                notfnd.add(k1)
                continue
            pid1 = t1
            k2pid[k1] = pid1
            # protein2
            k2 = "%s|%s|%s" % (up2, sym2, geneid2)
            if k2 in k2pid:
                pid2 = k2pid[k2]
            elif k2 in notfnd:
                continue
            else:
                t2 = find_target(dba, k2)
            if not t2:
                notfnd.add(k2)
                continue
            pid2 = t2
            k2pid[k2] = pid2
            if pid1 == pid2:
                same12_ct += 1
                continue
            # Insert PPI
            rv = dba.ins_ppi( {'ppitype': 'BioPlex','p_int': pint, 'p_ni': pni, 'p_wrong': pw,
                                'protein1_id': pid1, 'protein1_str': k1,
                                'protein2_id': pid2, 'protein2_str': k2} )
            if rv:
                ppi_ct += 1
            else:
                dba_err_ct += 1
            

    for k in notfnd:
        logger.warn("No target found for: {}".format(k))
    print("\n{} BioPlex PPI rows processed.".format(ct))
    print("\nInserted {} new ppi rows".format(ppi_ct))
    if same12_ct:
        print("\nSkipped {} PPIs involving the same protein".format(same12_ct))
    if notfnd:
        print("\nNo target found for {} UniProts/Syms/GeneIDs. See logfile {} for details.".format(len(notfnd), logfile))
    if dba_err_ct > 0:
        print("\nWARNNING: {} DB errors occurred. See logfile {} for details.".format(dba_err_ct, logfile))

    for f in UPD_FILES[1:]:
        start_time = time.time()
        line_ct = slmf.wcl(f)
        line_ct -= 1
        with open(f, 'r') as tsv:
            tsvreader = csv.reader(tsv, delimiter='\t')
            header = tsvreader.__next__() # skip header line
            # plate_num       well_num        db_protein_id   symbol  gene_id bait_symbol     bait_geneid     pWrongID        pNoInt  pInt
            ct = 0
            ppi_ct = 0
            same12_ct = 0
            k2pid = {}
            notfnd = set()
            dba_err_ct = 0
            for row in tsvreader:
                ct += 1
                slmf.update_progress(ct/line_ct)
                geneid1 = row[6]
                geneid2 = row[4]
                sym1 = row[5]
                sym2 = row[3]
                pw = row[7]
                pni = row[8]
                pint = row[9]
                # protein1
                k1 = "|%s|%s" % (sym1, geneid1)
                if k1 in k2pid:
                    pid1 = k2pid[k1]
                elif k1 in notfnd:
                    continue
                else:
                    t1 = find_target(dba, k1)
                    if not t1:
                        notfnd.add(k1)
                        continue
                    pid1 = t1
                    k2pid[k1] = pid1
                # protein2
                k2 = "|%s|%s" % (sym2, geneid2)
                if k2 in k2pid:
                    pid2 = k2pid[k2]
                elif k2 in notfnd:
                    continue
                else:
                    t2 = find_target(dba, k2)
                    if not t2:
                        notfnd.add(k2)
                        continue
                    pid2 = t2
                    k2pid[k2] = pid2
                if pid1 == pid2:
                    same12_ct += 1
                    continue
                # Insert PPI
                rv = dba.ins_ppi( {'ppitype': 'BioPlex','p_int': pint, 'p_ni': pni, 'p_wrong': pw,
                                    'protein1_id': pid1, 'protein1_str': k1,
                                    'protein2_id': pid2, 'protein2_str': k2} )
                if rv:
                    ppi_ct += 1
                else:
                    dba_err_ct += 1     


            
def find_target(dba, k):
  (up, sym, geneid) = k.split("|")
  targets = False
  if up != '': # No UniProt accessions in update files
    targets = dba.find_protein_ids({'uniprot': up})
  if not targets:
    targets = dba.find_protein_ids({'sym': sym})
  if not targets:
    targets = dba.find_protein_ids({'geneid': geneid})
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
    # dataset_id = dba.ins_dataset( {'name': 'BioPlex Protein-Protein Interactions', 'source': "Files %s from https://bioplex.hms.harvard.edu/data/"%", ".join(SRC_FILES), 'app': PROGRAM, 'app_version': __version__, 'url': 'https://bioplex.hms.harvard.edu/data/'} )
    # assert dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # # Provenance
    # rv = dba.ins_provenance({'dataset_id': dataset_id, 'table_name': 'ppi', 'where_clause': "ppitype = 'BioPlex'"})
    # assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)
    
    
    dataset_id = 110
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    