#!/usr/bin/env python

""" load-Uberon.

Usage:
    load-Uberon.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-Uberon.py -? | --help

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
uberon_obo_FILE='../data/uberon/uberon.obo'

def load(args, dba, dataset_id, logger, logfile):
    fn = uberon_obo_FILE
    eco = {}
    parser = obo.Parser(fn)
    for stanza in parser:
        eco[stanza.tags['id'][0].value] = stanza.tags
    line_ct = len(list(eco.keys()))
    i = 0
    for e,d in eco.items():
        slmf.update_progress(i/line_ct)
        uid = d['id'][0].value
        name = 'None'
        if 'name' in d:
            name =d['name'][0].value
        defi=None
        comment=None
        #print(d.keys())

        if 'def' in d:
            defi=d['def'][0].value
        if 'comment' in d:
            comment= d['comment'][0].value
        dba.ins_uberon(init={'uid':uid,'name':name,'def':defi,'comment':comment})
        if 'is_a' in d:
            for a in d['is_a']:
            
                parent_id=a.value
                #print('isa',uid,parent_id)
                dba.ins_uberon_parent(init={'uid':uid,'parent_id':parent_id})
        if 'xref' in d:
            
            for x in d['xref']:
                xr=x.value
                try:
                    print
                    db =xr.split(':')[0]
                    value=xr.split(':')[1]
                    #print(xr,db,value)
                    dba.ins_uberon_xref(init={'uid':uid,'db':db,'value':value})
                except:
                    do_nothing=1
        #print(type(uid[0].value),name[0].value,defi,comment)
        
        i+=1
        



        






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

    # dataset_id = dba.ins_dataset( {'name': 'Uberon Ontology', 'source': "file downloaded from https://github.com/obophenotype/uberon/", 'app': PROGRAM, 'app_version': __version__, 'url': 'https://github.com/obophenotype/uberon/'} )
    # assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    dataset_id=48
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'uberon'},
    #         {'dataset_id': dataset_id, 'table_name': 'uberon_parent'},
    #          {'dataset_id': dataset_id, 'table_name': 'uberon_xref'} ]
    # for prov in provs:
    #     rv=dba.ins_provenance(prov)
    #     assert rv, f"Error inserting the data into prov for {prov}"
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")