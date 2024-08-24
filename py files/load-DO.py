#!/usr/bin/env python

"""Load load-DO.py.

Usage:
    load-DO.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-DO.py -? | --help

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

__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"
DO_OBO_FILE='../data/DO/disease_ontology.obo'

def load(args , dba, dataset_id, logger, logfile):
    # obo format
    #id: DOID:0040002
    # name: aspirin allergy
    # def: "A drug allergy that has_allergic_trigger acetylsalicylic acid." [url:https\://www.ncbi.nlm.nih.gov/pubmed/2468301] {comment="IEDB:RV"}
    # subset: DO_IEDB_slim
    # synonym: "acetylsalicylic acid allergy" EXACT []
    # synonym: "ASA allergy" EXACT []
    # xref: SNOMEDCT_US_2022_09_01:293586001
    # xref: UMLS_CUI:C0004058
    # is_a: DOID:0060500 ! drug allergy
    fn = DO_OBO_FILE
    eco = {}
    parser = obo.Parser(fn)
    for stanza in parser:
        eco[stanza.tags['id'][0].value] = stanza.tags
    line_ct = len(list(eco.keys()))
    ct =0
    total_doid=0
    db_err_ct = 0
    total_doid_parent =0
    total_do_xref = 0
    for e,d in eco.items():
        slmf.update_progress(total_doid/line_ct)
        doid = e
        name=None
        defination=None
        if 'name' in d:
            name = d['name'][0]
        if 'def' in d:
            defination = d['def'][0]

        rv = dba.ins_DO({'doid':str(doid),'name':str(name),'def':str(defination)})
        if rv:
            total_doid +=1
        else: 
            db_err_ct +=1

        if 'is_a' in d:
            parent = str(d['is_a'][0])
            rv = dba.ins_Do_parent({'doid':str(doid),'parent_id':str(parent)})
            if rv:
                total_doid_parent +=1
            else: 
                db_err_ct +=1

        #print(e,d)
        if 'xref' in d:
            #print(d['xref'][1])
            for x in d['xref']:
                db=str(x).split(':')[0]
                value=str(x).split(':')[1]
                #print(db,value)
                rv = dba.ins_Do_xref({'doid':str(doid),'db':db,'value':value})
                if rv :
                    total_do_xref +=1
                else :
                    db_err_ct+=1

                    





        
        
    print("Processed {} doid.".format(line_ct))
    print(f"  Inserted {total_doid} do")
    print(f"  Inserted {total_do_xref} do_xref")
    print(f"  Inserted {total_doid_parent} do_parent")
    if db_err_ct > 0:
        print(f"WARNING: {db_err_ct} DB errors occurred. See logfile {logfile} for details.")



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

    # dataset_id = dba.ins_dataset( {'name': 'Disease Ontology', 'source':"File http://purl.obolibrary.org/obo/doid.obo, version releases/2019-03-01/doid.obo",'url':"http://disease-ontology.org/"} )
    # assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    #dataset_id=42

    # provs = [ {'dataset_id': dataset_id, 'table_name': 'do'},
    #         {'dataset_id': dataset_id, 'table_name': 'do_xref'}]
    # for prov in provs:
    #     rv=dba.ins_provenance(prov)
    #     assert rv, f"Error inserting the data into prov for {prov}"
    dataset_id=42
    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")







