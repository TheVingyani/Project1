#!/usr/bin/env python

"""Load load-GOExptFuncLeafTDLIs.py.

Usage:
    load-GOExptFuncLeafTDLIs.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-GOExptFuncLeafTDLIs.py -? | --help

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
from goatools.obo_parser import GODag
import obo

__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"
DOWNLOAD_DIR = '../data/GO/'
BASE_URL = 'http://www.geneontology.org/ontology/'
FILENAME = 'go.obo'


def load(args, dba, dataset_id, logger, logfile):
    gofile=DOWNLOAD_DIR+FILENAME
    logger.info(f"Parsing GO OBO file:{gofile}")
    #godad = GODag(gofile)
    tct = int(dba.get_protein_counts()['total'])

    ct =0
    ti_ct = 0

    dba_err_ct=0
    notfnd ={}
    exp_codes=['EXP', 'IDA', 'IPI', 'IMP', 'IGI', 'IEP']
    fn = DOWNLOAD_DIR + FILENAME
    parser = obo.Parser(fn)
    eco_ids={}
    for stanza in parser:
        eco_ids[stanza.tags['id'][0].value] = stanza.tags
    go_ids_in_obo=list(eco_ids.keys())
    

    for pid in dba.get_protein_ids():
        ct +=1
        slmf.update_progress(ct/tct)
        goas = dba.get_goa_for_goexptfuncleaftdlis(pid)
        #print(pid)
        #print(goas)
        if goas:
            lfe_goa_strs=[]
            for d in goas:
                if d['go_term'][0]=='C': continue #only want mf/bp
                ev = d['evidence']
                if ev not in exp_codes: continue #only want experimental evidence goas

                if not d['go_id'] in go_ids_in_obo:
                    k = "%s:%s" % (d['go_id'], d['go_term'])
                    notfnd[k] = True
                    logger.error("GO term %s not found in GODag" % k)
                    continue
                
                lfe_goa_strs.append("%s|%s|%s"%(d['go_id'], d['go_term'], ev))
            if lfe_goa_strs:
                rv = dba.ins_tdl_info({'protein_id': pid, 'itype': 'Experimental MF/BP Leaf Term GOA', 'string_value': "; ".join(lfe_goa_strs)})
                if not rv:
                    dba_err_ct +=1
                    continue
                ti_ct +=1
    print("{} TCRD targets processed.".format(ct))
    print("  Inserted {} new tdl_info rows".format(ti_ct))
    if len(notfnd.keys()) > 0:
        print(f"WARNING: {len(notfnd.keys())} GO terms not found in GODag. See logfile {logfile} for details.")
    if dba_err_ct > 0:
        print("WARNING: {} DB errors occurred. See logfile {} for details.".format((dba_err_ct, logfile)))

            

        


    


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

    dataset_id = dba.ins_dataset( {'name': 'GO Experimental Leaf Term Flags', 'source': 'IDG-KMC generated data by Steve Mathias at UNM.', 'app': PROGRAM, 'app_version': __version__, 'comments': 'These values are calculated by the loader app and indicate that a protein is annotated with a GO leaf term in either the Molecular Function or Biological Process branch with an experimental evidenve code.'} )
    assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    provs = [ {'dataset_id': dataset_id, 'table_name': 'tdl_info', 'where_clause': "itype = 'Experimental MF/BP Leaf Term GOA'"} ]
    for prov in provs:
        rv=dba.ins_provenance(prov)
        assert rv, f"Error inserting the data into prov for {prov}"
    dataset_id=23
    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")
    







