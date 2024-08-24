#!/usr/bin/env python

"""Load STRINGIDs annotations for mkdev.

Usage:
    Load-STRINGIDs.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    Load-STRINGIDs.py -? | --help

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
INFILE1 = '../data/STRINGIDs/human.uniprot_2_string.tsv'
INFILE2='../data/STRINGIDs/9606.protein.aliases.v11.5.txt'

def load(args,dba,dataset_id,logger,logfile):
    aliasmap ={}
    ct = 0
    skip_ct=0
    mult_ct = 0
    line_ct = slmf.wcl(INFILE1)
    with open(INFILE1,'r') as tsv:
        tsvreader = csv.reader(tsv,delimiter='\t')
        tsvreader.__next__()
        ct +=1
        for row in tsvreader:
            #taxid uniprot_ac|uniprot_id string_id identity bit_score
            ct +=1
            slmf.update_progress(ct/line_ct)
            if float(row[3]) !=100:
                skip_ct +=1
                continue
            [uniprot,name]=row[1].split('|')
            ensp = row[2].replace('9606.','')
            bitscore = float(row[4])
            if uniprot in aliasmap:
                #save mapping with highest bit score
                if bitscore>aliasmap[uniprot][1]:
                    aliasmap[uniprot]=(ensp,bitscore)
            else:
                aliasmap[uniprot]=(ensp,bitscore)
            if name in aliasmap:
                if bitscore>aliasmap[name][1]:
                    aliasmap[name]=(ensp,bitscore)
            else:
                aliasmap[name]=(ensp,bitscore)
    unmap_ct =len(aliasmap)
    print(f"{ct} input processed")
    print(f"skipped {skip_ct} non-identity lines")
    print(f"Got {unmap_ct} uniprot/name to string ID mapping")

    line_ct =slmf.wcl(INFILE2)

    ct = 0
    warn_ct = 0
    with open(INFILE2,'r') as tsv:
        tsvreader=csv.reader(tsv,delimiter='\t')
        tsvreader.__next__()
        ct +=1
        for row in tsvreader:
            alias = row[1]
            ct +=1
            slmf.update_progress(ct/line_ct)
            ensp=row[0].replace('9696.','')
            if alias in aliasmap and aliasmap[alias][0] !=ensp:
                logger.warning(f"Different ENSPs found for sam alias {alias}:{aliasmap[alias][0],ensp}")
                warn_ct +=1
                continue
            aliasmap[alias]=(ensp,None)
            

    amp_ct = len(aliasmap)-unmap_ct
    print(f"{ct} input lines processed")
    print(f"added {amp_ct} alias to string id mappings")
    if warn_ct>0:
        print(f"skipped {warn_ct} alisaes that would override uniprot mappings. see logfile {logfile} for details")
    tct = dba.get_protein_counts()['total']
    print('tct',tct)
    ct = 0
    upd_ct = 0
    nf_ct = 0
    dba_err_ct = 0
    for row in dba.get_proteins():
        ct += 1
        slmf.update_progress(ct/tct)
        p = row['id']
        geneid = 'hsa:'+str(row['geneid'])
        hgnc_values_from_xref = dba.get_hgnc_xref_for_stringids(p)
        #print(hgnc_values_from_xref)
        hgncid = None
        if len(hgnc_values_from_xref)>0:
            hgncid='HGNC:'+str(hgnc_values_from_xref[0]['value'])
            #print(hgncid)
        #print(row)
        ensp = None
        if row['uniprot'] in aliasmap:
            ensp=aliasmap[row['uniprot']][0]
        elif row['name'] in aliasmap:
            ensp=aliasmap[row['name']][0]
        elif geneid in aliasmap:
            ensp=aliasmap[geneid][0]
        elif hgncid and hgncid in aliasmap:
            ensp=aliasmap[hgncid][0]
        #print(ensp)
        if not ensp:
            nf_ct +=1
            logger.warning(f"No string for protein {p} ({row['uniprot']})")
            continue
        rv = dba.do_update({'table': 'protein', 'id': p, 'col': 'stringid', 'val': ensp.split('.')[-1]} )
        if rv:
            upd_ct += 1
        else:
            dba_err_ct += 1
        

    print("Updated {} STRING ID values".format(upd_ct))
    if nf_ct > 0:
        print("No stringid found for {} proteins. See logfile {} for details.".format(nf_ct, logfile))
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

    # dataset_id = dba.ins_dataset( {'name': 'STRING IDs', 'source': "Files human.uniprot_2_string.2018.tsv and 9606.protein.aliases.v11.0.txt from from http://string-db.org/", 'app': PROGRAM, 'app_version': __version__, 'url': 'http://string-db.org/'} )
    # assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'protein', 'column_name': 'stringid'}
    #          ]
    # for prov in provs:
    #     rv=dba.ins_provenance(prov)
    #     assert rv, f"Error inserting the data into prov for {prov}"
    #print(dataset_id)
    dataset_id =19
    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")







