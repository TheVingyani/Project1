#!/usr/bin/env python

"""

Usage:
    load-ConsensusExpressions.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-ConsensusExpressions.py -? | --help

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
import operator


__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"

TISSUESTYPED_FILE ='../data/Tissues_Typed_v2.1.csv'

def calculate_consensus(vals):
  rvalmap = {0: 'Not Detected', 1: 'Low', 2: 'Medium', 3: 'High'}
  # sorted_vals will be a list of tuples sorted by the second element in each tuple (ie. the count)
  sorted_vals = sorted(vals.items(), key=operator.itemgetter(1), reverse=True)
  # consensus value is simply the mode:
  cons = rvalmap[sorted_vals[0][0]]
  # calculate confidence score
  if cons == 'High':
    if vals[3] > 4:
      if vals[2]+vals[1]+vals[0] == 0:
        conf = 5
      elif vals[2] == 1 and vals[1]+vals[0] == 0:
        conf = 4
      elif vals[2] == 2 and vals[1]+vals[0] == 0:
        conf = 3
      elif vals[2] == 3 and vals[1]+vals[0] == 0:
        conf = 2
      else:
        conf = 1
    elif vals[3] == 4:
      if vals[2]+vals[1]+vals[0] == 0:
        conf = 4
      elif vals[2] == 1 and vals[1]+vals[0] == 0:
        conf = 3
      elif vals[2] == 2 and vals[1]+vals[0] == 0:
        conf = 2
      else:
        conf = 1
    elif vals[3] == 3:
      if vals[2]+vals[1]+vals[0] == 0:
        conf = 3
      elif vals[2] == 1 and vals[1]+vals[0] == 0:
        conf = 2
      else:
        conf = 1
    elif vals[3] == 2:
      if vals[2]+vals[1]+vals[0] == 0:
        conf = 2
      else:
        conf = 1
    else:
      conf = 0
  elif cons == 'Medium':
    if vals[2]+vals[3] > 4:
      if vals[1]+vals[0] == 0:
        conf = 5
      elif vals[1] == 1 and vals[0] == 0:
        conf = 4
      elif vals[1] == 2 and vals[0] == 0:
        conf = 3
      elif vals[1] == 3 and vals[0] == 0:
        conf = 2
      else:
        conf = 1
    elif vals[2]+vals[3] == 4:
      if vals[1]+vals[0] == 0:
        conf = 4
      elif vals[1] == 1 and vals[0] == 0:
        conf = 3
      elif vals[1] == 2 and vals[0] == 0:
        conf = 2
      else:
        conf = 1
    elif vals[2]+vals[3] == 3:
      if vals[1]+vals[0] == 0:
        conf = 3
      elif vals[1] == 1 and vals[0] == 0:
        conf = 2
      else:
        conf = 1
    elif vals[2]+vals[3] == 2:
      if vals[1]+vals[0] == 0:
        conf = 2
      else:
        conf = 1
    else:
      conf = 0
  elif cons == 'Low':
    if vals[1]+vals[2]+vals[3] > 4:
      if vals[0] == 0:
        conf = 5
      elif vals[0] == 1 and vals[3] == 0:
        conf = 4
      elif vals[0] == 2 and vals[3] == 0:
        conf = 3
      elif vals[0] == 3 and vals[3] == 0:
        conf = 2
      else:
        conf = 1
    elif vals[1]+vals[2]+vals[3] == 4:
      if vals[0] == 0:
        conf = 4
      elif vals[0] == 1 and vals[3] == 0:
        conf = 3
      elif vals[0] == 2 and vals[3] == 0:
        conf = 2
      else:
        conf = 1
    elif vals[1]+vals[2]+vals[3] == 3:
      if vals[0] == 0:
        conf = 3
      elif vals[0] == 1 and vals[3] == 0:
        conf = 2
      else:
        conf = 1
    else:
      conf = 0
  elif cons == 'Not Detected':
    if vals[0] > 4:
      if vals[1]+vals[2]+vals[3] == 0:
        conf = 5
      elif vals[1] == 1 and vals[2]+vals[3] == 0:
        conf = 4
      elif vals[1] == 2 and vals[2]+vals[3] == 0:
        conf = 3
      elif vals[1] == 3 and vals[2]+vals[3] == 0:
        conf = 2
      else:
        conf = 1
    elif vals[0] == 4:
      if vals[1]+vals[2]+vals[3] == 0:
        conf = 4
      elif vals[1] == 1 and vals[2]+vals[3] == 0:
        conf = 3
      elif vals[1] == 2 and vals[2]+vals[3] == 0:
        conf = 2
      else:
        conf = 1
    elif vals[0] == 3:
      if vals[1]+vals[2]+vals[3] == 0:
        conf = 3
      elif vals[1] == 1 and vals[2]+vals[3] == 0:
        conf = 2
      else:
        conf = 1
    elif vals[0] == 2:
      if vals[1]+vals[2]+vals[3] == 0:
        conf = 2
      else:
        conf = 1
    else:
      conf = 0
  return(cons, conf)

def default_factory():
  return {0: 0, 1: 0, 2: 0, 3: 0}

def aggregate_exps(exps, gtexs, tmap):
  aggexps = defaultdict(default_factory)
  fvalmap = {'Not Detected': 0, 'Low': 1, 'Medium': 2, 'High': 3}
  for e in exps:
    tissue = e['tissue'].lower()
    if tissue not in tmap:
      continue
    k1 = tmap[tissue]
    if e['qual_value'] in fvalmap:
      k2 = fvalmap[e['qual_value']]
    else:
      continue
    aggexps[k1][k2] += 1
  if gtexs:
    for g in gtexs:
      tissue = g['tissue'].lower()
      if tissue not in tmap:
        continue
      k1 = tmap[tissue]
      if g['tpm_level'] in fvalmap:
        k2 = fvalmap[g['tpm_level']]
      else:
        continue
      aggexps[k1][k2] += 1
  return aggexps

def load(args, dba, dataset_id, logger, logfile):
    tmap = {} # tissue name to Tissue Type as per TIO
    line_ct = slmf.wcl(TISSUESTYPED_FILE)
    line_ct -= 1
    with open(TISSUESTYPED_FILE, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        header = csvreader.__next__() # skip header line
        ct = 0
        for row in csvreader:
            ct += 1
            tissue = row[0].lower()
            tmap[tissue] = row[2]  

    line_ct = dba.get_protein_counts()['total']
    print(line_ct)
    ct = 0
    nouid = set()
    exp_ct = 0
    dba_err_ct = 0
    for t in dba.get_protein_ids():
        ct += 1
        slmf.update_progress(ct/line_ct)
        p = t
        exps = dba.get_exps(p)
        gtexs=dba.get_gtex(p)

        if not exps and not gtexs:
          continue

        if not gtexs:
            gtexs = None
        aggexps = aggregate_exps(exps, gtexs, tmap)
        for tissue, vals in aggexps.items():
            (cons, conf) = calculate_consensus(vals)
            init = {'protein_id': p, 'etype': 'Consensus', 'tissue': tissue,
                    'qual_value': cons, 'confidence': conf}
            rv = dba.ins_expression(init)
            if rv:
                exp_ct += 1
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

    # Dataset
    # dataset_id = dba.ins_dataset( {'name': 'Consensus Expression Values', 'source': 'IDG-KMC generated data by Steve Mathias at UNM.', 'app': PROGRAM, 'app_version': __version__, 'comments': 'Consensus of GTEx, HPM and HPA expression values are calculated by the loader app.'} )
    # assert dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # # Provenance
    # rv = dba.ins_provenance({'dataset_id': dataset_id, 'table_name': 'expression', 'where_clause': "etype = 'Consensus'"})
    # assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)
    
    dataset_id = 75
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    