#!/usr/bin/env python

"""

Usage:
    load-DrugCentral.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-DrugCentral.py -? | --help

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

FILES_DATE = '05122020' ## Make sure this is correct!!!
TCLIN_FILE = '../data/DrugCentral/tclin_%s.tsv'%FILES_DATE
TCHEM_FILE = '../data/DrugCentral/tchem_drugs_%s.tsv'%FILES_DATE
DRUGINFO_FILE = '../data/DrugCentral/drug_info_%s.tsv'%FILES_DATE
DRUGIND_FILE = '../data/DrugCentral/drug_indications_%s.tsv'%FILES_DATE
NAME_ID_FILE = '../data/DrugCentral/drug.target.interaction.tsv'
SRC_FILES = [os.path.basename(TCLIN_FILE),
             os.path.basename(TCHEM_FILE),
             os.path.basename(DRUGINFO_FILE),
             os.path.basename(DRUGIND_FILE),
             os.path.basename(NAME_ID_FILE)]



def load(args, dba, dataset_id, logger, logfile):
    # First get mapping of DrugCentral names to ids
    name2id = {}
    print(NAME_ID_FILE)
    line_ct = slmf.wcl(NAME_ID_FILE)
    print("\nProcessing {} input lines in file {}".format(line_ct, NAME_ID_FILE))
    with open(NAME_ID_FILE, 'r',encoding='utf-8') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        ct = 0
        for row in tsvreader:
            ct += 1
            if row[0].startswith('#'): continue
            name2id[row[0]] = row[1].replace("\n", '')
    print("{} input lines processed.".format(ct))
    print("Saved {} keys in infos map".format(len(name2id)))  

    # Next get drug info fields
    infos = {}
    line_ct = slmf.wcl(DRUGINFO_FILE)

    print("\nProcessing {} input lines in file {}".format(line_ct, DRUGINFO_FILE))
    with open(DRUGINFO_FILE, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        ct = 0
        for row in tsvreader:
            ct += 1
            if row[0].startswith('#'): continue
            infos[row[0]] = row[1].replace("\n", '')
    print("{} input lines processed.".format(ct))
    print("Saved {} keys in infos map".format(len(infos)))

    #
    # MOA activities
    #
    drug2tids = defaultdict(list)
    line_ct = slmf.wcl(TCLIN_FILE)
    line_ct -= 1
    print("\nProcessing {} lines from DrugDB MOA activities file {}".format(line_ct, TCLIN_FILE))
    with open(TCLIN_FILE, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        header = tsvreader.__next__() # skip header line
        # uniprot swissprot       drug_name       act_value       act_type        action_type     source_name     reference       smiles  ChEMBL_Id
        ct = 0
        da_ct = 0
        err_ct = 0
        notfnd = []
        dba_err_ct = 0
        for row in tsvreader:
            ct += 1
            slmf.update_progress(ct/line_ct)
            up = row[0]
            sp = row[1]
            drug = row[2]
            if drug not in name2id:
                err_ct += 1
                logger.warn("No DrugCentral id found for {}".format(drug))
                continue
            dcid = name2id[drug]
            targets = dba.find_protein_ids({'uniprot': up})
            if not targets:
                targets = dba.find_protein_ids({'name': sp})
            if not targets:
                notfnd.append(up)
                continue
            tid = targets[0]
            drug2tids[drug].append(tid)
            init = {'target_id': tid, 'drug': drug, 'dcid': dcid, 'has_moa': 1, 'source': row[5]}
            if row[3]:
                init['act_value'] = row[3]
            if row[4]:
                init['act_type'] = row[4]
            if row[5]:
                init['action_type'] = row[5]
            if row[6]:
                init['source'] = row[6]
            if row[7]:
                init['reference'] = row[7]
            if row[8]:
                init['smiles'] = row[8]
            if row[9]:
                init['cmpd_chemblid'] = row[9]
            if drug in infos:
                init['nlm_drug_info'] = infos[drug]
            rv = dba.ins_drug_activity(init)
            if rv:
                da_ct += 1
            else:
                dba_err_ct += 1
            
    print("{} DrugCentral Tclin rows processed.".format(ct))
    print("  Inserted {} new drug_activity rows".format(da_ct))
    if len(notfnd) > 0:
        print("WARNNING: {} Uniprot/Swissprot Accessions NOT FOUND in TCRD:".format(len(notfnd)))
    for up in notfnd:
        print(up)
    if err_ct > 0:
        print("WARNNING: DrugCentral ID not found for {} drug names. See logfile {} for details.".format(err_ct, logfile))
    if dba_err_ct > 0:
        print("WARNNING: {} DB errors occurred. See logfile {} for details.".format(dba_err_ct, logfile))


    #
    # Non-MOA activities
    #
    line_ct = slmf.wcl(TCHEM_FILE)
    line_ct -= 1
    print("\nProcessing {} lines from Non-MOA activities file {}".format(line_ct, TCHEM_FILE))
    with open(TCHEM_FILE, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        header = tsvreader.__next__() # skip header line
        # uniprot swissprot       drug_name       act_value       act_type        action_type     source_name     reference       smiles  ChEMBL_Id
        ct = 0
        da_ct = 0
        err_ct = 0
        notfnd = []
        dba_err_ct = 0
        for row in tsvreader:
            ct += 1
            slmf.update_progress(ct/line_ct)
            up = row[0]
            sp = row[1]
            drug = row[2]
            if drug not in name2id:
                err_ct += 1
                logger.warn("No DrugCentral id found for {}".format(drug))
                continue
            dcid = name2id[drug]
            targets = dba.find_protein_ids({'uniprot': up})
            if not targets:
                targets = dba.find_protein_ids({'name': sp})
            if not targets:
                notfnd.append(up)
                continue
            tid = targets[0]
            drug2tids[drug].append(tid)
            init = {'target_id': tid, 'drug': drug, 'dcid': dcid, 'has_moa': 0, 'source': row[5]}
            if row[3]:
                init['act_value'] = row[3]
            if row[4]:
                init['act_type'] = row[4]
            if row[5]:
                init['action_type'] = row[5]
            if row[6]:
                init['source'] = row[6]
            if row[7]:
                init['reference'] = row[7]
            if row[8]:
                init['smiles'] = row[8]
            if row[9]:
                init['chemblid'] = row[9]
            if drug in infos:
                init['nlm_drug_info'] = infos[drug]
            rv = dba.ins_drug_activity(init)
            if rv:
                da_ct += 1
            else:
                dba_err_ct += 1
            
    print("{} DrugCentral Tchem rows processed.".format(ct))
    print("  Inserted {} new drug_activity rows".format(da_ct))
    if len(notfnd) > 0:
        print("WARNNING: {} DrugDB Uniprot Accessions NOT FOUND in TCRD:".format(len(notfnd)))
    for up in notfnd:
        print(up)
    if err_ct > 0:
        print("WARNNING: DrugCentral ID not found for {} drug names. See logfile {} for details.".format(err_ct, logfile))
    if dba_err_ct > 0:
        print("WARNING: {} DB errors occurred. See logfile {} for details.".format(dba_err_ct, logfile))


    #
    # Indications (diseases)
    #
    line_ct = slmf.wcl(DRUGIND_FILE)
    line_ct -= 1
    print("\nProcessing {} lines from indications file {}".format(line_ct, DRUGIND_FILE))
    with open(DRUGIND_FILE, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        header = tsvreader.__next__() # skip header line
        # DRUG_ID DRUG_NAME       INDICATION_FDB  UMLS_CUI        SNOMEDCT_CUI    DOID
        ct = 0
        t2d_ct = 0
        notfnd = {}
        dba_err_ct = 0
        for row in tsvreader:
            ct += 1
            slmf.update_progress(ct/line_ct)
            drug = row[1]
            if drug not in drug2tids:
                notfnd[drug] = True
                continue
            init = {'dtype': 'DrugCentral Indication',
                    'name': row[2], 'drug_name': drug}
            if row[5] != '':
                init['did'] = row[5]
            for tid in drug2tids[drug]:
                # NB> Using target_id as protein_id works for now, but will not if/when we have multiple protein targets
                init['protein_id'] = tid
                rv = dba.ins_disease(init)
                if rv:
                    t2d_ct += 1
                else:
                    dba_err_ct += 1
            
    print("{} DrugCentral indication rows processed.".format(ct))
    print("  Inserted {} new disease rows".format(t2d_ct))
    if len(notfnd.keys()) > 0:
        print("WARNNING: {} drugs NOT FOUND in activity files:".format(len(notfnd)))
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

    # # Dataset
    # dataset_id = dba.ins_dataset( {'name': 'Drug Central', 'source': "Drug Central files download files: %s"%", ".join(SRC_FILES), 'app': PROGRAM, 'app_version': __version__, 'url': 'http://drugcentral.org/'} )
    # if not dataset_id:
    #     print("WARNING: Error inserting dataset. See logfile {} for details.".format(logfile))
    #     sys.exit(1)
    # # Provenance
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'drug_activity'},
    #             {'dataset_id': dataset_id, 'table_name': 'disease', 'where_clause': "dtype = 'DrugCentral Indication'"} ]
    # for prov in provs:
    #     rv = dba.ins_provenance(prov)
    #     if not rv:
    #         print("WARNING: Error inserting provenance. See logfile {} for details.".format(logfile))
    #         sys.exit(1)
        
    dataset_id = 112
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    