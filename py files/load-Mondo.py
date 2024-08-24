#!/usr/bin/env python

"""

Usage:
    load-Mondo.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-Mondo.py -? | --help

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


def parse_mondo(fn):
  print(f"Parsing Mondo file {fn}")
  parser = obo.Parser(fn)
  raw_mondo = {}
  for stanza in parser:
    if stanza.name != 'Term':
      continue
    raw_mondo[stanza.tags['id'][0].value] = stanza.tags
  mondod = {}
  for mondoid,md in raw_mondo.items():
    if 'is_obsolete' in md:
      continue
    if 'name' not in md:
      continue
    init = {'mondoid': mondoid, 'name': md['name'][0].value}
    if 'def' in md:
      init['def'] = md['def'][0].value
    if 'comment' in md:
      init['comment'] = md['comment'][0].value
    if 'is_a' in md:
      init['parents'] = []
      for parent in md['is_a']:
        # for now, just ignore parent source info, if any.
        cp = parent.value.split(' ')[0]
        init['parents'].append(cp)
    if 'xref' in md:
      init['xrefs'] = []
      for xref in md['xref']:
        if xref.value.startswith('http') or xref.value.startswith('url'):
          continue
        if len(xref.value.split(' ')) == 1:
          (db, val) = xref.value.split(':')
          init['xrefs'].append({'db': db, 'value': val})
        else:
          (dbval, src) = xref.value.split(' ', 1)
          (db, val) = dbval.split(':')
          #init['xrefs'].append({'db': db, 'value': val})
          init['xrefs'].append({'db': db, 'value': val, 'source': src})
    mondod[mondoid] = init
  print("  Got {} Mondo terms".format(len(mondod)))
  return mondod



def load(args, dba, dataset_id, logger, logfile):
    mondod = parse_mondo(cfgd['DOWNLOAD_DIR']+cfgd['FILENAME'])
    mondo_ct = len(mondod)
    print(f"Loading {mondo_ct} MonDO terms")
    ct = 0
    ins_ct = 0
    dba_err_ct = 0
    for mondoid,md in mondod.items():
        #print(list(md.keys()))
        ct += 1
        md['mondoid'] = mondoid
        if 'xrefs' in md:
            for xref in md['xrefs']:
                if 'source' in xref and 'source="MONDO:equivalentTo"' in xref['source']:
                    xref['equiv_to'] = 1
                else:
                    xref['equiv_to'] = 0
        rv = dba.ins_mondo(md)
        if rv:
            ins_ct += 1
        else:
            dba_err_ct += 1
        slmf.update_progress(ct/mondo_ct)
            
    

            
                    
            
      
  
CONFIG = [{'name': 'Mondo', 'DOWNLOAD_DIR': '../data/mondo/', 
            'BASE_URL': 'https://github.com/monarch-initiative/mondo/releases/latest/download/mondo.obo', 'FILENAME': 'mondo.obo',
            'parse_function': parse_mondo, 'load_function': load}]
cfgd = CONFIG[0]

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

    # dataset_id = dba.ins_dataset( {'name': 'Mondo', 'source': 'Mondo file {}, version {}'.format(cfgd['BASE_URL']+cfgd['FILENAME'], MKDEV_VER), 'app': PROGRAM, 'app_version': __version__, 'url': 'https://mondo.monarchinitiative.org/'} )
    # assert dataset_id, f"Error inserting dataset See logfile {logfile} for details."
    # # Provenance
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'mondo'} ,
    #             {'dataset_id': dataset_id, 'table_name': 'mondo_parent'},
    #             {'dataset_id': dataset_id, 'table_name': 'mondo_xref'} ]
    # for prov in provs:
    #     rv = dba.ins_provenance(prov)
    #     assert rv, f"Error inserting provenance. See logfile {logfile} for details."
    
    #dataset_id = 60
    
    dataset_id=120

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    