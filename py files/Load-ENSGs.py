#!/usr/bin/env python
#!/usr/bin/env python

"""Load ENSGs annotations for xref targets from downloaded TSV file.

Usage:
    load-ENSGs.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-ENSGs.py -? | --help

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
import os,sys,time,re
from docopt import docopt
from TCRD.DBAdaptor import DBAdaptor
import logging
from urllib.request import urlretrieve
import gzip
import csv
import slm_util_functions as slmf
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"

ENSGS_DATA_DIR='../data/ENSGs/'

TSV_HUMAN_FILE='Homo_sapiens.GRCh38.110.uniprot.tsv'
TSV_RAT_FILE='Rattus_norvegicus.mRatBN7.2.110.uniprot.tsv'
TSV_MUS_FILE='Mus_musculus.GRCm39.110.uniprot.tsv'

def load_human_tsv(args,dataset_id):
    fn = ENSGS_DATA_DIR + TSV_HUMAN_FILE
    line_ct=slmf.wcl(fn)
    if not args['--quiet']:
        print(f"\n processing {line_ct} lines in file {fn}")

    ct =0
    xref_ct=0
    dba_err_ct=0
    with open(fn,'r') as ifh:
        tsvreader=csv.reader(ifh,delimiter='\t')
        for row in tsvreader:
            slmf.update_progress(ct/line_ct)
            if ct==0:
                headers=row
                #print(headers)
                ct +=1
                continue
            #print(row)
            uniprot_id = row[3]
            ens_id = row[0]
            # print(uniprot_id)
            # print(ens_id)
            if uniprot_id !='':
                pids = dba.find_protein_ids({'uniprot':uniprot_id})
                for pid in pids:
                    rv = dba.ins_xref({'protein_id': pid, 'xtype': 'ENSG', 'dataset_id': dataset_id, 'value':ens_id})
                    if rv:
                        xref_ct += 1
                    else:
                        dba_err_ct += 1
                ct +=1
    print(f"total processed rows {line_ct}")
    print(f"total xref added {xref_ct}")
    print(f"DB error in {dba_err_ct} ")
    if dba_err_ct>0:
                print(f"WARNING: {dba_err_ct} DB errors occurred. See logfile {logfile} for details.")
            
                   

def load_rat_tsv(args,dataset_id):
    fn = ENSGS_DATA_DIR + TSV_RAT_FILE
    line_ct=slmf.wcl(fn)
    if not args['--quiet']:
        print(f"\n processing {line_ct} lines in file {fn}")

    ct =0
    xref_ct=0
    dba_err_ct=0
    with open(fn,'r') as ifh:
        tsvreader=csv.reader(ifh,delimiter='\t')
        for row in tsvreader:
            slmf.update_progress(ct/line_ct)
            if ct==0:
                headers=row
                #print(headers)
                ct +=1
                continue
            #print(row)
            uniprot_id = row[3]
            ens_id = row[0]
            # print(uniprot_id)
            # print(ens_id)
            if uniprot_id !='':
                pids = dba.find_nhprotein_ids({'uniprot':uniprot_id})
                for pid in pids:
                    rv = dba.ins_xref({'nhprotein_id': pid, 'xtype': 'ENSG', 'dataset_id': dataset_id, 'value':ens_id})
                    if rv:
                        xref_ct += 1
                    else:
                        dba_err_ct += 1
                ct +=1
    print(f"total processed rows {line_ct}")
    print(f"total xref added {xref_ct}")
    print(f"DB error in {dba_err_ct} ")
    if dba_err_ct>0:
                print(f"WARNING: {dba_err_ct} DB errors occurred. See logfile {logfile} for details.")   

def load_mus_tsv(args,dataset_id):
    fn = ENSGS_DATA_DIR + TSV_MUS_FILE
    line_ct=slmf.wcl(fn)
    if not args['--quiet']:
        print(f"\n processing {line_ct} lines in file {fn}")

    ct =0
    xref_ct=0
    dba_err_ct=0
    with open(fn,'r') as ifh:
        tsvreader=csv.reader(ifh,delimiter='\t')
        for row in tsvreader:
            slmf.update_progress(ct/line_ct)
            if ct==0:
                headers=row
                #print(headers)
                ct +=1
                continue
            #print(row)
            uniprot_id = row[3]
            ens_id = row[0]
            # print(uniprot_id)
            # print(ens_id)
            if uniprot_id !='':
                pids = dba.find_nhprotein_ids({'uniprot':uniprot_id})
                for pid in pids:
                    rv = dba.ins_xref({'nhprotein_id': pid, 'xtype': 'ENSG', 'dataset_id': dataset_id, 'value':ens_id})
                    if rv:
                        xref_ct += 1
                    else:
                        dba_err_ct += 1
                ct +=1
    print(f"total processed rows {line_ct}")
    print(f"total xref added {xref_ct}")
    print(f"DB error in {dba_err_ct} ")
    if dba_err_ct>0:
                print(f"WARNING: {dba_err_ct} DB errors occurred. See logfile {logfile} for details.")       





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

    dataset_id = dba.ins_dataset( {'name': 'Ensembl Gene IDs', 'source': "files Rattus_norvegicus.Rnor_6.0.94.uniprot.tsv, Mus_musculus.GRCm38.94.uniprot.tsv, Homo_sapiens.GRCh38.94.uniprot.tsv from ftp://ftp.ensembl.org/pub/current_tsv/", 'app': PROGRAM, 'app_version': __version__} )
    assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    #dataset_id=15
    provs = [ {'dataset_id': dataset_id, 'table_name': 'xref', 'where_clause': "dataset_id = %d"%dataset_id} ]
    for prov in provs:
        rv=dba.ins_provenance(prov)
        assert rv, f"Error inserting the data into prov for {prov}"
    #dataset_id=15
    # we will manually downlaod the file HUMAN_9606_idmapping_selected.tab.gz
    #download(args) 
    load_human_tsv(args,dataset_id)
    load_rat_tsv(args,dataset_id)
    load_mus_tsv(args,dataset_id)
    
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")