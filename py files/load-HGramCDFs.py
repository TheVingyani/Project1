#!/usr/bin/env python

"""

Usage:
    load-HGramCDFs.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-HGramCDFs.py -? | --help

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
import numpy
import math

__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"




def load(args, dba, dataset_id, logger, logfile):
    # Create a dictionary of gene_attribute_type.name => [] pairs
    counts = {}
    # Create a dictionary of gene_attribute_type.name => {} pairs
    stats = {}
    gatypes = dba.get_gene_attribute_types()
    #print("gatypes",gatypes)

    for ga in gatypes:
        counts[ga] = []
        stats[ga] = {}

    tct = dba.get_protein_counts()['total']
    ct = 0
    for t in dba.get_proteins():
        ct += 1
        slmf.update_progress(ct/tct)
        p = t
        pid = p['id']
        gene_attribute_counts = dba.gene_attribute_counts(pid)
        #print(gene_attribute_counts)
        if not  gene_attribute_counts: continue
        for type,attr_count in gene_attribute_counts.items():
            #print(type,attr_count)
            counts[type].append(attr_count)

    #print(counts)

    print("\nCalculatig Gene Attribute stats. See logfile {}.".format(logfile))
    logger.info("Calculatig Gene Attribute stats:")
    new_counts ={}
    for type,l in counts.items():
        if len(l) == 0:
            continue
        new_counts[type]=l
        npa = numpy.array(l)
        #print(npa)
        #print("  %s: %d counts; mean: %.2f; std: %.2f" % (type, len(l), npa.mean(), npa.std()))
        stats[type]['mean'] = npa.mean()
        stats[type]['std'] = npa.std()

    counts = new_counts

    print("\nLoading HGram CDFs for {} TCRD targets".format(tct))
    ct = 0
    nan_ct = 0
    cdf_ct = 0
    dba_err_ct = 0
    for t in dba.get_proteins():
        ct += 1
        slmf.update_progress(ct/tct)
        p = t
        pid = p['id']
        gene_attribute_counts = dba.gene_attribute_counts(pid)
        if not gene_attribute_counts: continue
        for type,attr_count in gene_attribute_counts.items():
            #print(attr_count, stats[type]['mean'], stats[type]['std'])
            attr_cdf = gaussian_cdf(attr_count, stats[type]['mean'], stats[type]['std'])
            #print(attr_cdf)
            if math.isnan(attr_cdf):
                attr_cdf = 1.0 / (1.0 + math.exp(-1.702*((attr_count-stats[type]['mean']) / stats[type]['std'] )))
            if math.isnan(attr_cdf):
                nan_ct += 1
                continue
            rv = dba.ins_hgram_cdf({'protein_id': p['id'], 'type': type,
                                    'attr_count': attr_count, 'attr_cdf': attr_cdf})
            if not rv:
                dba_err_ct += 1
                continue
            cdf_ct += 1

    print("\nProcessed {} targets.".format(ct)) 
    print("\n  Loaded {} new hgram_cdf rows".format(cdf_ct)) 
    print("\n  Skipped {} NaN CDFs".format(nan_ct)) 
    if dba_err_ct > 0:
        print("WARNING: {} DB errors occurred. See logfile {} for details.".format(dba_err_ct, logfile))      
    

            

def gaussian_cdf(ct, mu, sigma):
  err = math.erf((ct - mu) / (sigma * math.sqrt(2.0)))
  cdf = 0.5 * ( 1.0 + err )
  return cdf           
      
  
  


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
    # dataset_id = dba.ins_dataset( {'name': 'Harmonogram CDFs', 'source': 'IDG-KMC generated data by Steve Mathias at UNM.', 'app': PROGRAM, 'app_version': __version__, 'comments': 'CDFs are calculated by the loader app based on gene_attribute data in TCRD.'} )
    # assert dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # # Provenance
    # rv = dba.ins_provenance({'dataset_id': 1, 'table_name': 'hgram_cdf'})
    # assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)
    
    dataset_id = 108
    
    #print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    