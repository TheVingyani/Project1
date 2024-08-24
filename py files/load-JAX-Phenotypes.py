#!/usr/bin/env python

"""

Usage:
    load-JAX-Phenotypes.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-JAX-Phenotypes.py -? | --help

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
import json
import pandas as pd
import pronto


__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"

FILE ='../data/JAX/HMD_HumanPhenotype.rpt'
MPO_OWL_FILE = '../data/MPO/mp.json'


def parse_mp_owl(f):
    mapping = {}
    with open(f,'r') as ifh:
        data=json.load(ifh)
        for i in data['graphs']:
            print(i.keys())
            for edge in i['edges']:
                if 'pred' in edge and edge['pred']=='is_a':
                    if 'obj' in edge and 'sub' in edge:
                        first_value = edge['sub'].split('/')[-1].replace('_',':')
                        second_value = edge['obj'].split('/')[-1].replace('_',':')
                        mapping[first_value]=second_value
            for node in i['nodes']:
                if 'id' in node:
                    id=node['id'].split('/')[-1].replace('_',':')
                if not id[:3]=='MP:':
                    continue
                if 'lbl' in node:
                    name = node['lbl']
                else:
                    name=None
                mapping[id]={'name':name}
    return mapping
                
def load(args, dba, dataset_id, logger, logfile):
    mpo = parse_mp_owl(MPO_OWL_FILE)
    fn = FILE
    line_ct = slmf.wcl(fn)
    #print(mpo)
    with open(fn, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        ct = 0
        pt_ct = 0
        skip_ct = 0
        pmark = {}
        notfnd = set()
        dba_err_ct = 0
        for row in tsvreader:
            ct += 1
            #print(row)
            if not row[4] or row[4] == '':
                skip_ct += 1
                continue
            sym = row[0]
            geneid = row[1]
            k = "%s|%s"%(sym,geneid)
            if k in notfnd:
                continue
            targets = dba.find_protein_ids({'sym': sym})
            if not targets:
                targets = dba.find_protein_ids({'geneid': geneid})
            if not targets:
                notfnd.add(k)
                logger.warn("No target found for {}".format(k))
                continue
            for t in targets:
                pid = t
                pmark[pid] = True
                for mpid in row[4].split(','):
                    rv = dba.ins_phenotype({'protein_id': pid, 'ptype': 'JAX/MGI Human Ortholog Phenotype', 'term_id': mpid.strip(), 'term_name': mpo[mpid.strip()]['name']})
                    if rv:
                        pt_ct += 1
                    else:
                        dba_err_ct += 1
            slmf.update_progress(ct/line_ct) 
               
    

            
                    
            
      
  
  


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

    # dataset_id = dba.ins_dataset( {'name': 'JAX/MGI Mouse/Human Orthology Phenotypes', 'source': "File HMD_HumanPhenotype.rpt from ftp.informatics.jax.org", 'app': PROGRAM, 'app_version': __version__, 'url': 'http://www.informatics.jax.org/'} )
    # assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'phenotype','where_clause':"ptype = 'JAX/MGI Human Ortholog Phenotyp'"}]
    # for prov in provs:
    #     rv=dba.ins_provenance(prov)
    #     assert rv, f"Error inserting the data into prov for {prov}"
    
    dataset_id = 62
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    