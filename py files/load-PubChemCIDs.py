#!/usr/bin/env python

"""

Usage:
    load-PubChemCIDs.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-PubChemCIDs.py -? | --help

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

DOWNLOAD_DIR = '../data/ChEMBL/'
BASE_URL = 'ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/wholeSourceMapping/src_id1/'
# For src info, see https://www.ebi.ac.uk/unichem/ucquery/listSources
FILENAME = 'src1src22.txt.gz'



def load(args, dba, dataset_id, logger, logfile):
    infile = DOWNLOAD_DIR + FILENAME
    infile = infile.replace('.gz', '')
    line_ct = slmf.wcl(infile)

    print("\nProcessing {} lines in file {}".format(line_ct, infile))
    chembl2pc = {}
    with open(infile, 'r') as tsv:
        ct = 0
        tsv.readline() # skip header line
        for line in tsv:
            data = line.split('\t')
            chembl2pc[data[0]] = int(data[1])
   
    print("Got {} ChEMBL to PubChem mappings".format(len(chembl2pc)))


    chembl_activities = dba.get_cmpd_activities(catype = 'ChEMBL')

    print("\nLoading PubChem CIDs for {} ChEMBL activities".format(len(chembl_activities)))
    logger.info("Loading PubChem CIDs for {} ChEMBL activities".format(len(chembl_activities)))
    ct = 0
    pcid_ct = 0
    notfnd = set()
    dba_err_ct = 0
    line_ct = len(chembl_activities)
    for ca in chembl_activities:
        ct += 1
        slmf.update_progress(ct/line_ct)
        if ca['cmpd_id_in_src'] not in chembl2pc:
            notfnd.add(ca['cmpd_id_in_src'])
            logger.warn("{} not found".format(ca['cmpd_id_in_src']))
            continue
        pccid = chembl2pc[ca['cmpd_id_in_src']]
        rv = dba.do_update({'table': 'cmpd_activity', 'id': ca['id'],
                            'col': 'cmpd_pubchem_cid', 'val': pccid})
        if rv:
            pcid_ct += 1
        else:
            dba_err_ct += 1
        
        
    
    
    print("\n{} ChEMBL activities processed.".format(ct))
    print("  Inserted {} new PubChem CIDs".format(pcid_ct))
    if len(notfnd) > 0:
        print("  {} ChEMBL IDs not found. See logfile {} for details.".format(len(notfnd), logfile))
    if dba_err_ct > 0:
        print("WARNNING: %d DB errors occurred. See logfile %s for details." % (dba_err_ct, logfile))

    drug_activities = dba.get_drug_activities()

    print("\nLoading PubChem CIDs for {} drug activities".format(len(drug_activities)))
    logger.info("Loading PubChem CIDs for {} drug activities".format(len(drug_activities)))
    
    ct = 0
    pcid_ct = 0
    skip_ct = 0
    notfnd = set()
    dba_err_ct = 0
    line_ct = len(drug_activities)
    for da in drug_activities:
        ct += 1
        if not da['cmpd_chemblid']:
            skip_ct += 1
            continue
        if da['cmpd_chemblid'] not in chembl2pc:
            notfnd.add(da['cmpd_chemblid'])
            logger.warn("{} not found".format(da['cmpd_chemblid']))
            continue
        pccid = chembl2pc[da['cmpd_chemblid']]
        rv = dba.do_update({'table': 'drug_activity', 'id': da['id'],
                            'col': 'cmpd_pubchem_cid', 'val': pccid})
        if rv:
            pcid_ct += 1
        else:
            dba_err_ct += 1
        slmf.update_progress(ct/line_ct)
        
    
    print("\n{} drug activities processed.".format(ct))
    print("  Inserted {} new PubChem CIDs".format(pcid_ct))
    print("  Skipped {} drug activities with no ChEMBL ID".format(skip_ct))
    if len(notfnd) > 0:
        print("  {} ChEMBL IDs not found. See logfile {} for details.".format(len(notfnd), logfile))
    if dba_err_ct > 0:
        print("WARNNING: {} DB errors occurred. See logfile {} for details.".format(dba_err_ct, logfile))
     
    

            
                    
            
      
  
  


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
    # dataset_id = dba.ins_dataset( {'name': 'PubChem CIDs', 'source': 'File %s'%BASE_URL+FILENAME, 'app': PROGRAM, 'app_version': __version__} )
    # assert dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # # Provenance
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'cmpd_activity', 'column_name': 'pubchem_cid', 'comment': "Loaded from UniChem file mapping ChEMBL IDs to PubChem CIDs."},
    #             {'dataset_id': dataset_id, 'table_name': 'drug_activity', 'column_name': 'pubchem_cid', 'comment': "Loaded from UniChem file mapping ChEMBL IDs to PubChem CIDs."} ]
    # for prov in provs:
    #     rv = dba.ins_provenance(prov)
    #     assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)
    
    dataset_id = 125
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    