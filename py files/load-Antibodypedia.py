#!/usr/bin/env python

"""Load antibodypedia .

Usage:
    load-Antibodypedia.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-Antibodypedia.py -? | --help

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
import requests
import json

__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"
ABPC_API_URL = 'http://www.antibodypedia.com/tools/antibodies.php?uniprot='

def load(args , dba, dataset_id, logger, logfile):
    tct = int(dba.get_protein_counts()['total'])
    print(tct)
    logger.info(f"loading antiboydpedia annotation for {tct} proteins")
    ct =0
    tiab_ct=0
    timab_ct =0
    tiurl_ct=0
    dba_err_ct=0
    net_err_ct=0
    avail_proteins =dba.antibody_avail_proteins()
    #print(avail_proteins[-5:])
    
    for protein in dba.get_proteins():
        ct +=1
        slmf.update_progress(ct/tct)

        pid = protein['id']
        if pid in avail_proteins:
            #print(pid)
            continue
        url = ABPC_API_URL+protein['uniprot']
        r = None
        attempts =1
        while attempts <=5:
            try:
                logger.info(f"getting {url} [target {pid},attemt {attempts}]")
                r=requests.get(url)
                break
            except:
                attempts +=1
                time.sleep(1)
        if not r:
            net_err_ct +=1
            logger.error(f"no response for {url} [target {pid},attempts{attempts}]")
            continue
        if r.status_code !=200:
            net_err_ct +=1
            logger.error(f"Bad respinse:{r.status_code} for {url} [target {pid},attempts{attempts}]")
            continue
        abpd = json.loads(r.text)
        rv = dba.ins_tdl_info({'protein_id': pid, 'itype': 'Ab Count',
                           'integer_value': int(abpd['num_antibodies'])})
        if rv:
            tiab_ct +=1
        else:
            dba_err_ct +=1
        if 'ab_type_monoclonal' in abpd:
            mab_ct = int(abpd['ab_type_monoclonal'])
        else:
            mab_ct=0
        
        rv = dba.ins_tdl_info({'protein_id': pid, 'itype': 'MAb Count',
                           'integer_value': mab_ct})
        if rv:
            timab_ct +=1
        else:
            dba_err_ct +=1
        rv = dba.ins_tdl_info({'protein_id': pid, 'itype': 'Antibodypedia.com URL',
                           'string_value': abpd['url']})
        if rv:
            tiurl_ct += 1
        else:
            dba_err_ct += 1
        time.sleep(1)
        
    print("{} TCRD targets processed.".format(ct))
    print("  Inserted {} Ab Count tdl_info rows".format(tiab_ct))
    print("  Inserted {} MAb Count tdl_info rows".format(timab_ct))
    print("  Inserted {} Antibodypedia.com URL tdl_info rows".format(tiurl_ct))
    if net_err_ct > 0:
        print("WARNING: Network error for {} targets. See logfile {} for details.".format(net_err_ct, logfile))
    if dba_err_ct > 0:
        print("WARNING: {} DB errors occurred. See logfile {} for details.".format(dba_err_ct, logfile))


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

    # dataset_id = dba.ins_dataset( {'name': 'Aintibodypedia.com', 'source': "Web API at http://www.antibodypedia.com/tools/antibodies.php?uniprot=", 'app': PROGRAM, 'app_version': __version__, 'url': 'http://www.antibodypedia.com'} )
    # assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'tdl_info','where_clause':'itype == "Ab Count"'},
    #         {'dataset_id': dataset_id, 'table_name': 'tdl_info','where_clause':'itype == "MAb Count"'},
    #          {'dataset_id': dataset_id, 'table_name': 'tdl_info','where_clause':'itype == "Antibodypedia.com URL"'} ]
    # for prov in provs:
    #     rv=dba.ins_provenance(prov)
    #     assert rv, f"Error inserting the data into prov for {prov}"
    dataset_id=21
    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")







