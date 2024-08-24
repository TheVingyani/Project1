#!/usr/bin/env python

"""

Usage:
    load-eRAM.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>] --command=<str>
    load-eRAM.py -? | --help

Options:
  -c --command CMD     : map|load to map symbold to Harmonizome or load all Harmonizome data
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
import pickle
import json
import urllib
from urllib.request import urlopen


__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"

HARMO_API_BASE_URL = 'http://amp.pharm.mssm.edu/Harmonizome/'
SYM2PID_P = '../data/Sym2pid.p'
DATASET_DONE_FILE = '../data/DatasetsDone.txt'

def pickle_sym2pid(args, dba, logger):
    '''
    This goes through all TCRD targets and checks (by symbol) if the Harmonizome has a corresponding gene. An additional check is done to make sure the NCBI Gene ID in the Harmonizome matches that in TCRD. For those that do, results are saved to a pickle file. 
    '''
    tct = dba.get_protein_counts()['total']
    print("\nMapping {} TCRD targets to Harmonizome genes".format(tct))
    logger.info("Mapping {} TCRD targets to Harmonizome genes".format(tct))
    ct = 0
    sym2pid = {}
    skip_ct = 0
    err_ct = 0
    mm_ct = 0
    for target in dba.get_proteins():
        ct += 1
        slmf.update_progress(ct/tct)
        logger.info("Processing target {}".format(target['id']))
        p = target
        #print("protein_id",p['id'])
        k = "%d:%s"%(target['id'],p['sym'])
        if not p['sym']:
            logger.info("Skipping - target has no symbol")
            skip_ct += 1
            continue
        jsondata = get_gene(p['sym'])
        if not jsondata:
            logger.error("No JSON for {}".format(k))
            #print('not jsondata')
            err_ct += 1
        elif 'status' in jsondata and jsondata['status'] == 400:
            # Bad request:
            logger.warn("400 Bad Request for {}".format(p['sym']))
            #print('400 Bad Request ')
            err_ct += 1
        else:
            # success
            sym2pid[p['sym']] = p['id']
            if p['geneid'] and 'ncbiEntrezGeneId' in jsondata:
                if jsondata['ncbiEntrezGeneId'] != p['geneid']:
                    logger.warning('GeneID mismatch: Harmonizome: {} vs TCRD: {}'.format(jsondata['ncbiEntrezGeneId'], p['geneid']))
                    mm_ct += 1
            else:
                sym2pid[p['sym']] = p['id']
    print("\n  {} targets processed.".format(ct))
    print("  Dumping {} sym => TCRD protein_id mappings to file {}".format(len(sym2pid), SYM2PID_P))

    pickle.dump(sym2pid, open(SYM2PID_P, 'wb'))
    if skip_ct > 0:
        print("\n  Skipped {} targets with no sym".format(skip_ct))
    if err_ct > 0:
        print(" \n {} errors encountered. See logfile {} for details.".format(err_ct, LOGFILE))
    if mm_ct > 0:
        print(" \n {} GeneID mismatches. See logfile {} for details.".format(mm_ct, LOGFILE))
    return True


def get_gene(sym):
  url = HARMO_API_BASE_URL + 'api/1.0/gene/' + sym
  #print(url)
  #logger.debug("Getting %s" % url)
  jsondata = None
  attempts = 0
  while attempts < 5:
    try:
        response = urlopen(url)
        data = response.read().decode('utf-8')
        jsondata = json.loads(data)
        break
    except Exception as e:
        print(e)
        attempts += 1
  if jsondata:
    return jsondata
  else:
    print("no json data")
    return False



def get_datasets(api_base_url):
  datasets = {}
  endpoint = '/api/1.0/dataset?'
  url = api_base_url + endpoint
  while 1:
    #print "[DEBUG] Fetching URL: %s" % url 
    try:
      # API returns 100 datasets at a time and gives endpoint for next 100 gene sets
        response = urlopen(url)
        data = response.read().decode('utf-8')
        jsondata = json.loads(data)
    except Exception as e:
      #try again
      try:
        response = urlopen(url)
        data = response.read().decode('utf-8')
        jsondata = json.loads(data)
      except Exception as e:
        break
    if not jsondata['entities']: break
    for d in jsondata['entities']:
      datasets[d['name']] = d['href']
    #print "[DEBUG] Now have %d datasets" % len(datasets.keys()) 
    endpoint = jsondata['next']
    #print "[DEBUG] Next endpoint: %s" % endpoint
    url = api_base_url + endpoint
  return datasets

def get_dataset(api_base_url, endpoint):
  url = api_base_url + endpoint
  jsondata = None
  attempts = 0
  while attempts < 5:
    try:
        response = urlopen(url)
        data = response.read().decode('utf-8')
        jsondata = json.loads(data)
        break
    except Exception as e:
        attempts += 1
  if jsondata:
    return jsondata
  else:
    return False
  
def get_resource(api_base_url, endpoint):
  url = api_base_url + endpoint
  jsondata = None
  attempts = 0
  while attempts < 5:
    try:
        response = urlopen(url)
        data = response.read().decode('utf-8')
        jsondata = json.loads(data)
        break
    except Exception as e:
        attempts += 1
  if jsondata:
    return jsondata
  else:
    return False

def get_geneset(api_base_url, endpoint):
  url = api_base_url + endpoint
  jsondata = None
  attempts = 0
  while attempts < 5:
    try:
        response = urlopen(url)
        data = response.read().decode('utf-8')
        jsondata = json.loads(data)
        break
    except Exception as e:
        attempts += 1
  if jsondata:
    return jsondata
  else:
    return False


def load(args, dba, logger):
    assert os.path.exists(SYM2PID_P), "Error: No mapping file {}. Run with -c map first.".format(SYM2PID_P)
    print("\nLoading mapping of TCRD targets to Harmonizome genes from pickle file {}".format(SYM2PID_P))
    sym2pid = pickle.load(open(SYM2PID_P, 'rb') )
    print("\n  Got {} symbol to protein_id mappings".format(len(sym2pid)))

    if os.path.isfile(DATASET_DONE_FILE):
        # If we are restarting, this file has the names of datasets already loaded
        with open(DATASET_DONE_FILE) as f:
            datasets_done = f.read().splitlines()
    else:
            datasets_done = []
    datasets = get_datasets(HARMO_API_BASE_URL)
    print("\nProcessing {} Harmonizome datasets".format(len(datasets)))
    ct = 0
    gat_ct = 0
    total_ga_ct = 0
    err_ct = 0
    dba_err_ct = 0
    line_ct = len(list(datasets.keys()))
    for dsname in datasets.keys():
        ct += 1
        slmf.update_progress(ct/line_ct)
        ds_start_time = time.time()
        if dsname in datasets_done:
            print("\n  Skipping previously loaded dataset \"{}\"".format(dsname))
            continue
        ds_ga_ct = 0
        ds = get_dataset(HARMO_API_BASE_URL, datasets[dsname])
        if not ds:
            logger.error("Error getting dataset {} ({})".format(dsname, datasets[dsname]))
            err_ct += 1
            continue
        logger.info("Processing dataset \"{}\" containing {} gene sets".format(dsname, len(ds['geneSets'])))
        rsc = get_resource(HARMO_API_BASE_URL, ds['resource']['href'])
        get_gat_id = dba.get_gat_id(ds['name'])
        print(get_gat_id)
        if get_gat_id:
           gat_id=get_gat_id
        else:
            gat_id = dba.ins_gene_attribute_type( {'name': ds['name'], 'association': ds['association'], 'description': rsc['description'], 'resource_group': ds['datasetGroup'], 'measurement': ds['measurement'], 'attribute_group': ds['attributeGroup'], 'attribute_type': ds['attributeType'], 'pubmed_ids': "|".join([str(pmid) for pmid in rsc['pubMedIds']]), 'url': rsc['url']} )
        if gat_id:
            gat_ct += 1
        else:
            dba_err_ct += 1
        for d in ds['geneSets']:
            name = d['name'].encode('utf-8')
            gs = get_geneset(HARMO_API_BASE_URL, d['href'])
            if not gs:
                logger.error("Error getting gene set {} ({})".format(name, d['href']))
                err_ct += 1
                continue
            if 'associations' not in gs: # used to be 'features'
                logger.error("No associations in gene set {}".format(name))
                err_ct += 1
                continue
            logger.info("  Processing gene set \"{}\" containing {} associations".format(name, len(gs['associations'])))
            ga_ct = 0
            for f in gs['associations']: # used to be 'features'
                sym = f['gene']['symbol']
                if sym not in sym2pid: continue # symbol does not map to a TCRD target
                rv = dba.ins_gene_attribute( {'protein_id': sym2pid[sym], 'gat_id': gat_id,
                                            'name': name, 'value': f['thresholdValue']} )
                if not rv:
                    dba_err_ct += 1
                else:
                    ga_ct += 1
            ds_ga_ct += ga_ct
            time.sleep(1)
        total_ga_ct += ds_ga_ct
        ds_elapsed = time.time() - ds_start_time
        logger.info("  Inserted a total of {} new gene_attribute rows for dataset {}. Elapsed time: {}".format(ds_ga_ct, dsname, slmf.secs2str(ds_elapsed)))
        if err_ct > 0:
            logger.info("  WARNING: Error getting {} gene set(s) ".format(err_ct))
        # Save dataset names that are loaded, in case we need to restart
        with open(DATASET_DONE_FILE, "a") as dsdfile:
            dsdfile.write(dsname+'\n')
    print("\nProcessed {} Ma'ayan Lab datasets.".format(ct))
    print("Inserted {} new gene_attribute_type rows".format(gat_ct))
    print("Inserted a total of {} gene_attribute rows".format(total_ga_ct))
    if err_ct > 0:
        print("WARNING: {} errors occurred. See logfile {} for details.".format(err_ct, LOGFILE))
    if dba_err_ct > 0:
        print("WARNING: {} DB errors occurred. See logfile {} for details.".format(dba_err_ct, LOGFILE) )   
    

            
                    
            
      
  
  


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
    # dataset_id = dba.ins_dataset( {'name': 'Harmonizome', 'source': "API at %s"%HARMO_API_BASE_URL, 'app': PROGRAM, 'app_version': __version__, 'url': 'http://amp.pharm.mssm.edu/Harmonizome/'} )
    # assert dataset_id, "Error inserting dataset See logfile {} for details.".format(LOGFILE)
    # # Provenance
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'gene_attribute'},
    #             {'dataset_id': dataset_id, 'table_name': 'gene_attribute_type'},
    #             {'dataset_id': dataset_id, 'table_name': 'hgram_cdf'} ]
    # for prov in provs:
    #     rv = dba.ins_provenance(prov)
    #     assert rv, "Error inserting provenance. See logfile {} for details.".format(LOGFILE)
    
    if args['--command'] == 'map':
        pickle_sym2pid(args, dba, logger)
    elif args['--command'] == 'load':
        load(args, dba, logger)

    #load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    