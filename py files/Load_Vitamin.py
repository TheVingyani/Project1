#!/usr/bin/env python

"""Load HGNC annotations for TCRD targets from downloaded TSV file.

Usage:
    load-UniProt.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-UniProt.py -? | --help

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
Vitamin_csv_file='../data/Vitamin/Vitamin.csv'

def load(args, dba, dataset_id, logger, logfile):
    line_ct=slmf.wcl(Vitamin_csv_file)
    if not args['--quiet']:
        print(f"\n processing {line_ct} lines in file {Vitamin_csv_file}")

    ct=0
    vitamin_ct=0
    notfnd=set()
    pmark={}
    db_err_ct=0
    vitamins ={}
    new_vitamin_id=1
    with open(Vitamin_csv_file,'r') as ifh:
        csvreader=csv.reader(ifh)
        
        for row in csvreader:
            #print("row",row)
            # 0: 'Vitamin_name'
            # 1: 'Uniprot'
            # 2:  'Reviewed'
            # 3: 'Entry Name'
            # 4: 'Protein names'
            # 5: 'Gene Names'
            # 6: 'Organism'
            # 7: 'Length'
            # 8: GeneID 
            if row[0] in vitamins:
                vitamin_id=vitamins[row[0]]
            else:
                vitamins[row[0]]=new_vitamin_id
                vitamin_id=vitamins[row[0]]
                new_vitamin_id +=1
            if ct==0:
                header=row #header line
                ct= ct+1
                continue
            ct =ct+1
            slmf.update_progress(ct/line_ct)
            if row[1] !='':
                up=row[1]
            else:
                up=None
            if row[8] !='':
                geneid=int(row[8])
            else:
                geneid =None
            Vitamin_name = row[0]
            Uniprot = row[1]
            Reviewed = row[2]
            Entry_Name = row[3]
            Protein_names = row[4]
            Gene_Names = row[5]
            Organism = row[6]
            Length = int(row[7])
            
            
            pids=dba.find_protein_ids({'uniprot':up})
            if not pids and geneid:
                pids=dba.find_protein_ids({'geneid':geneid})
            if not pids:
                notfnd.add(f"{up}")
                logger.warn(f"No protein found for {up}")
                rv = dba.ins_vitamin({'Vitamin_name':Vitamin_name,'Vitamin_id':vitamin_id, 'Uniprot': up,
                           'geneid':geneid,'Reviewed': Reviewed, 'Entry_Name': Entry_Name,'Protein_names': Protein_names, 'Gene_Names': Gene_Names,
                           'Organism': Organism, 'Length': Length})
                if rv:
                    vitamin_ct +=1
                else:
                    db_err_ct +=1

                continue
            
            for pid in pids:
                # Vitamins

                rv = dba.ins_vitamin({'Vitamin_name':Vitamin_name,'Vitamin_id':vitamin_id,'protein_id': pid, 'Uniprot': Uniprot,'geneid':geneid,
                           'Reviewed': Reviewed, 'Entry_Name': Entry_Name,'Protein_names': Protein_names, 'Gene_Names': Gene_Names,
                           'Organism': Organism, 'Length': Length})
                if rv:
                    vitamin_ct +=1
                else:
                    db_err_ct +=1

                pmark[pid]=True
    print("Processed {} lines - {} proteins annotated.".format(ct, len(pmark)))
    if notfnd:
        print("No protein found for {} lines (with UniProts).".format(len(notfnd)))
    if vitamin_ct > 0:
        print(f"  Inserted {vitamin_ct} new Vitamin")
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
    # logger.info(f"Connected to database {args['--dbname']} Schema_ver:{dbi['schema_ver']},data ver:{dbi['data_ver']}")
    # if not args['--quiet']:
    #     print(f"Connected to database {args['--dbname']} Schema_ver:{dbi['schema_ver']},data ver:{dbi['data_ver']}")

    # dataset_id = dba.ins_dataset( {'name': 'UniProt', 'source': "Custom download file from uniprot for a given vitamin", 'app': PROGRAM, 'app_version': __version__, 'url': 'https://www.uniprot.org/uniprotkb','comments':'Relation between protein and vitamin will be updated into vitamin table'} )
    # assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'vitamin'}]
    # for prov in provs:
    #     rv=dba.ins_provenance(prov)
    #     assert rv, f"Error inserting the data into prov for {prov}"
    dataset_id=200
    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")
