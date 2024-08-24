#!/usr/bin/env python

"""Load HGNC annotations for TCRD targets from downloaded TSV file.

Usage:
    Load-IDGFams.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    Load-IDGFams.py -? | --help

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

__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"
INFILE = '../data/IDG_Families_UNM_UMiami_v2.csv'


def load(args , dba, dataset_id, logger, logfile):
    line_ct = slmf.wcl(INFILE)
    ct=0
    idg_ct = 0
    upd_ct1 = 0
    upd_ct2=0
    null_ct = 0
    notfnd = []
    mulfnd =[]
    dba_err_ct = 0
    with open(INFILE,'r') as csvfile:
        cscreader = csv.reader(csvfile)
        cscreader.__next__
        ct +=1
        slmf.update_progress(ct/line_ct)
        for row in cscreader:
            ct +=1
            up=row[2].strip()
            fam=row[3].strip()
            famext=row[4].strip()
            if not fam:
                null_ct +=1
                continue
            pids=dba.find_protein_ids({'uniprot',up})
            if not pids:
                notfnd.append(up)
                continue
            if len(pids)>1:
                mulfnd.append(up)
                continue
            t =pids[0]
            target_table_data=dba.fget_target(t)
            if target_table_data['fam']:
                idg_ct +=1
                continue
            #print(target_table_data)
            rv = dba.rv = dba.upd_target(target_table_data['id'], 'fam', fam)
            if not rv:
                print("ERROR updating target.fam: %d to %s" % (target_table_data['id'], fam))
            else:
                upd_ct1 += 1
            if famext and famext != '':
                rv = dba.upd_target(target_table_data['id'], 'famext', famext)
                if not rv:
                    print("ERROR updating target.famext: %d to %s" % (target_table_data['id'], famext))
                else:
                    upd_ct2 += 1
    print("{} rows processed.".format(ct))
    print("{} IDG family designations loaded into TCRD.".format(upd_ct1))
    print("{} IDG extended family designations loaded into TCRD.".format(upd_ct2))
    print("Skipped {} IDG2 targets.".format(idg_ct))
    if notfnd:
        print("[WARNING] No target found for {} UniProt accessions: {}".format(len(notfnd), ", ".join(notfnd)))
    if mulfnd:
        print("[WARNING] Multiple targets found for {} UniProt accessions: {}".format(len(mulfnd), ", ".join(mulfnd)))
    if dba_err_ct > 0:
        print("WARNING: {} database errors occured. See logfile {} for details.".format(dba_err_ct, logfile))
  
                






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

    # dataset_id = dba.ins_dataset( {'name': 'IDG Families', 'source': "IDG-KMC generated data from file IDG_Families_UNM_UMiami_v2.csv", 'app': PROGRAM, 'app_version': __version__, 'comments': 'http://www.genenames.org/'} )
    # assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'target', 'column_name': 'tiofam'} ]
    # for prov in provs:
    #     rv=dba.ins_provenance(prov)
    #     assert rv, f"Error inserting the data into prov for {prov}"
    dataset_id=20
    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")







