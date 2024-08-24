#!/usr/bin/env python

"""

Usage:
    load-CTD-Diseases.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-CTD-Diseases.py -? | --help

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


__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"

FILE ='../data/CTD/CTD_genes_diseases.tsv'



def load(args, dba, dataset_id, logger, logfile):

    mesh2doid = dba.get_db2do_map('MESH')
    omim2doid = dba.get_db2do_map('OMIM')

    print(list(mesh2doid.keys())[0:5])
    print(list(mesh2doid.values())[0:5])
    print('\n')
    print(list(omim2doid.keys())[0:5])
    print(list(omim2doid.values())[0:5])


    # knowledge
    fn = FILE
    line_ct = slmf.wcl(fn)
    ct = 0
    k2pids = {}
    pmark = {}
    notfnd = set()
    skip_ct = 0
    dis_ct = 0
    dba_err_ct = 0
    with open(fn, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        for row in tsvreader:
            if len(row)==1:
                continue
            slmf.update_progress(ct/line_ct)
            # 0: GeneSymbol
            # 1: GeneID
            # 2: DiseaseName
            # 3: DiseaseID (MeSH or OMIM identifier)
            # 4: DirectEvidence ('|'-delimited list)
            # 5: InferenceChemicalName
            # 6: InferenceScore
            # 7: OmimIDs ('|'-delimited list)
            # 8: PubMedIDs ('|'-delimited list)
            #print(row)
            ct += 1
            if row[0].startswith('#'):
                continue
            if not row[4]: # only load associations with direct evidence
                skip_ct += 1
                continue
            sym = row[0]
            geneid = row[1]
            k = "%s|%s"%(sym,geneid)
            if k in k2pids:
                # we've already found it
                pids = k2pids[k]
            elif k in notfnd:
                # we've already not found it
                continue
            else:
                targets = dba.find_protein_ids({'sym': sym})
                if not targets:
                    targets = dba.find_protein_ids({'geneid': geneid})
                if not targets:
                    notfnd.add(geneid)
                    logger.warn("No target found for {}".format(k))
                    continue
                pids = []
                for t in targets:
                    p = t
                    pmark[p] = True
                    pids.append(p)
                k2pids[k] = pids # save this mapping so we only lookup each target once
            # Try to map MeSH and OMIM IDs to DOIDs
            if row[3].startswith('MESH:'):
                mesh = row[3].replace('MESH:', '')
                if mesh in mesh2doid:
                    dids = mesh2doid[mesh]
                else:
                    dids = [row[3]]
            elif row[3].startswith('OMIM:'):
                omim = row[3].replace('OMIM:', '')
                if omim in omim2doid:
                    dids = omim2doid[omim]
                else:
                    dids = [row[3]]
            else:
                dids = [row[3]]
            for pid in pids:
                for did in dids:
                    rv = dba.ins_disease( {'protein_id': pid, 'dtype': 'CTD', 'name': row[2],
                                        'did': did, 'evidence': row[4]} )
                    if not rv:
                        dba_err_ct += 1
                        continue
                    dis_ct += 1
    print("Loaded {} new disease rows for {} proteins.".format(dis_ct, len(pmark)))       
    

            
                    
            
      
  
  


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

    # dataset_id = dba.ins_dataset( {'name': 'CTD Disease Associations', 'source': "IFile CTD_genes_diseases.tsv.gz from http://ctdbase.org/reports/.", 'app': PROGRAM, 'app_version': __version__, 'url': 'http://ctdbase.org/','comments':'Only disease associations with direct evidence are loaded into Database.'} )
    # assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'disease','where_clause':"dtype = 'CTD'"}]
    # for prov in provs:
    #     rv=dba.ins_provenance(prov)
    #     assert rv, f"Error inserting the data into prov for {prov}"
    
    dataset_id = 59
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    