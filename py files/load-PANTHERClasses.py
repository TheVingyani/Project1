#!/usr/bin/env python

"""

Usage:
    load-eRAM.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-eRAM.py -? | --help

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
import re


__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"

# http://data.pantherdb.org/ftp/sequence_classifications/current_release/PANTHER_Sequence_Classification_files/
P2PC_FILE = '../data/PANTHER/PTHR18.0_human'
# http://data.pantherdb.org/PANTHER14.1/ontology/Protein_Class_14.0
CLASS_FILE = '../data/PANTHER/Protein_Class_14.0.txt'
# http://data.pantherdb.org/PANTHER14.1/ontology/Protein_class_relationship
RELN_FILE = '../data/PANTHER/Protein_class_relationship.txt'



def load(args, dba, dataset_id, logger, logfile):
    relns = {}
    line_ct = slmf.wcl(RELN_FILE)
    with open(RELN_FILE, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        ct = 0
        for row in tsvreader:
            ct += 1
            pcid = row[0]
            parentid = row[2]
            if pcid in relns:
                relns[pcid].append(parentid)
            else:
                relns[pcid] = [parentid]
    print("\n{} input lines processed.".format(ct))
    print(" \n Got {} PANTHER Class relationships".format(len(relns)))

    pc2dbid = {}
    line_ct = slmf.wcl(CLASS_FILE)
    with open(CLASS_FILE, 'r',encoding='utf-8') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        ct = 0
        pc_ct = 0
        pcmark = {}
        dba_err_ct = 0
        for row in tsvreader:
            ct += 1
            slmf.update_progress(ct/line_ct)
            pc = row[0]
            init = {'pcid': pc, 'name': row[2]}
            if row[3]:
                init['desc'] = row[3]
            if pc in relns:
                init['parent_pcids'] = "|".join(relns[pc])
            # there are duplicates in this file too, so only insert if we haven't
            if pc not in pcmark:
                rv = dba.ins_panther_class(init)
                if rv:
                    pc_ct += 1
                else:
                    dba_err_ct += 1
                pc2dbid[pc] = rv
                pcmark[pc] = True
    print("\n{} lines processed.".format(ct))
    print("\n  Inserted {} new panther_class rows".format(pc_ct))
    if dba_err_ct > 0:
        print("\n WARNING: {} DB errors occurred. See logfile {} for details.".format(dba_err_ct, logfile))

    line_ct = slmf.wcl(P2PC_FILE)
    regex = re.compile(r'#(PC\d{5})')
    with open(P2PC_FILE, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        ct = 0
        skip_ct = 0
        pmark = {}
        p2pc_ct = 0
        notfnd = set()
        dba_err_ct = 0
        for row in tsvreader:
            #print(row)
            ct += 1
            slmf.update_progress(ct/line_ct)
            [sp,hgnc,up] = row[0].split('|')
            up = up.replace('UniProtKB=', '')
            hgnc = hgnc.replace('HGNC=', '')
            if not row[8]:
                skip_ct += 1
                continue
            #print "[DEBUG] searching by uniprot", up 
            targets = dba.find_protein_ids({'uniprot': up})
            if not targets:
                #print "[DEBUG] searching by Ensembl xref", ensg 
                targets = dba.find_protein_ids_by_xref({'xtype': 'HGNC', 'value': hgnc})
            if not targets:
                k = "%s|%s"%(up,hgnc)
                notfnd.add(k)
                continue
            t = targets[0]
            pid = t
            pmark[pid] = True
            #print "[DEBUG] PCs:",  row[8]
            #print(row[9])
            # print(regex.findall(row[8]))
            for pc in regex.findall(row[9]):
                #print "[DEBUG]    ", pc
                if pc in pc2dbid:
                    pcid = pc2dbid[pc]
                    #print(pid,pcid)
                    rv = dba.ins_p2pc({'protein_id': pid, 'panther_class_id': pcid})
                    if rv:
                        p2pc_ct += 1
                    else:
                        dba_err_ct += 1
                        print("error")
    
    for k in notfnd:
        logger.warn("No target found for {}".format(k))
    print("\n {} lines processed.".format(ct))
    print(" \n Inserted {} new p2pc rows for {} distinct proteins".format(p2pc_ct, len(pmark)))
    print(" \n Skipped {} rows without PCs".format(skip_ct))
    if notfnd:
        print("\nNo target found for {} UniProt/HGNCs. See logfile {} for details.".format(len(notfnd), logfile))
    if dba_err_ct > 0:
        print("\nWARNING: {} DB errors occurred. See logfile {} for details.".format(dba_err_ct, logfile))

         
    

            
                    
            
      
  
  


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

    # dataset_id = dba.ins_dataset( {'name': 'PANTHER protein classes', 'source': 'File %s from http://data.pantherdb.org/ftp/sequence_classifications/current_release/PANTHER_Sequence_Classification_files/, and files %s and %s from http://data.pantherdb.org/PANTHER14.1/ontology/'%(os.path.basename(P2PC_FILE), os.path.basename(CLASS_FILE), os.path.basename(RELN_FILE)), 'app': PROGRAM, 'app_version': __version__, 'url': 'http://www.pantherdb.org/'} )
    # assert dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # # Provenance
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'panther_class'},
    #             {'dataset_id': dataset_id, 'table_name': 'p2pc'} ]
    # for prov in provs:
    #     rv = dba.ins_provenance(prov)
    #     assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)
    
    dataset_id = 102
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    