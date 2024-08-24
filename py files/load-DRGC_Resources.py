#!/usr/bin/env python

"""

Usage:
    load-DRGC_Resources.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-DRGC_Resources.py -? | --help

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
import json
from urllib.request import urlopen
import urllib
import requests

from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Disable the InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"

FILE ='../data/CTD/CTD_genes_diseases.tsv'

RSS_API_BASE_URL = 'https://rss.ccs.miami.edu/rss-api/'

def load(args, dba, dataset_id, logger, logfile):
    target_data = get_target_data()
    print(len(target_data))
    print("  Got {} targets with DRGC resource(s)".format(len(target_data)))
    ct = 0
    res_ct = 0
    tmark = {}
    notfnd = set()
    derr_ct = 0
    mulfnd = set()
    dba_err_ct = 0
    line_ct = len(target_data)
    re1 = re.compile(r'NanoLuc.+-\s*(\w+)')
    re2 = re.compile(r'(\w+)\s*-NanoLuc.+')
    re3 = re.compile(r'NanoLuc.+-fused\s*(\w+)')
    for d in target_data:
        ct += 1
        slmf.update_progress(ct/line_ct)
        logger.info("Processing target data: {}".format(d))
        rt = d['resourceType'].replace(' ', '').lower()
        # for now, skip Datasets
        if rt == 'dataset':
            continue
            # m = re1.search(d['target'])
            # if not m:
            #   m = re2.search(d['target'])
            # if not m:
            #   m = re3.search(d['target'])
            # if not m:
            #   logger.warn("No target symbol found for data dict: {}".format(d))
            #   derr_ct += 1
            #   continue
            # sym = m.groups(1)
        else:
            sym = d['target']
        # print(rt)
        # print(d)
        resource_data = get_resource_data(d['id'])
        if resource_data:
            dbjson = json.dumps(resource_data['data'][0]['resource'])
            targets = dba.find_protein_ids({'sym': sym}, False)
            if not targets:
                targets = dba.find_target_ids({'sym': sym}, incl_alias=True)
                if not targets:
                    notfnd.add(sym)
                    logger.warn("No target found for {}".format(sym))
                    continue
            if len(targets) > 1:
                mulfnd.add(sym)
                logger.warn("Multiple targets found for {}".format(sym))
            tid = targets[0]
            rv = dba.ins_drgc_resource( {'target_id': tid, 'resource_type': d['resourceType'],
                                            'json': dbjson} )
            if not rv:
                dba_err_ct += 1
                continue
            tmark[tid] = True
            res_ct += 1
    print("{} targets processed.".format(ct))
    print("  Inserted {} new drgc_resource rows for {} targets".format(res_ct, len(tmark)))
    if notfnd:
        print("No target found for {} symbols. See logfile {} for details.".format(len(notfnd), logfile))
    if dba_err_ct > 0:
        print("WARNING: {} DB errors occurred. See logfile {} for details.".format(dba_err_ct, logfile))    
    

            
def get_target_data():
  url = f"{RSS_API_BASE_URL}target"
  jsondata = None
  resp = requests.get(url, verify=False)
  if resp.status_code == 200:
    return resp.json()
  else:
    return False

def get_resource_data(idval):
  url = f"{RSS_API_BASE_URL}target/id?id={idval}"
  jsondata = None
  resp = requests.get(url, verify=False)
  if resp.status_code == 200:
    return resp.json()
  else:
    return False                  
            
      
  
  


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
    # dataset_id = dba.ins_dataset( {'name': 'DRGC Resources', 'source': 'RSS APIs at http://dev3.ccs.miami.edu:8080/rss-apis/', 'app': PROGRAM, 'app_version': __version__, 'url': 'http://dev3.ccs.miami.edu:8080/rss-apis/'} )
    # if not dataset_id:
    #     print("WARNING: Error inserting dataset See logfile %s for details." % logfile)
    #     sys.exit(1)
    # # Provenance
    # rv = dba.ins_provenance({'dataset_id': dataset_id, 'table_name': 'drgc_resource'})
    # if not rv:
    #     print("WARNING: Error inserting provenance. See logfile %s for details." % logfile)
    #     sys.exit(1)
    
    dataset_id = 126
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    