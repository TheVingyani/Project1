#!/usr/bin/env python

"""Load HGNC annotations for TCRD targets from downloaded TSV file.

Usage:
    load-HGNC.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-HGNC.py -? | --help

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
HGNC_TSV_FILE='../data/HGNC/HGNC.tsv'

def load(args, dba, dataset_id, logger, logfile):
    line_ct=slmf.wcl(HGNC_TSV_FILE)
    if not args['--quiet']:
        print(f"\n processing {line_ct} lines in file {HGNC_TSV_FILE}")

    ct=0
    hgnc_ct=0
    mgi_ct=0
    chr_ct=0
    sym_ct=0
    symdiscr_ct=0
    geneid_ct=0
    geneiddiscr_ct=0
    notfnd=set()
    pmark={}
    db_err_ct=0
    with open(HGNC_TSV_FILE,'r') as ifh:
        tsvreader=csv.reader(ifh,delimiter='\t')
        
        for row in tsvreader:
            #print("row",row)
            # 0: HGNC ID
            # 1: Approved symbol
            # 2: Approved name
            # 3: Status
            # 4: Chromosome
            # 5: NCBI Gene ID
            # 6: Mouse genome database ID 
            # 7: UniProt ID
            if ct==0:
                header=row #header line
                ct= ct+1
                continue
            ct =ct+1
            slmf.update_progress(ct/line_ct)
            sym=row[1]
            if row[5] !='':
                geneid=int(row[5])
            else:
                geneid =None
            if row[7] !='':
                up=row[7]
            else:
                up=None
            pids=dba.find_protein_ids(q={'sym':sym})
            if not pids and geneid:
                pids=dba.find_protein_ids({'geneid':geneid})
            if not pids and up:
                pids:dba.find_protein_ids({'uniprot':up})
            if up and not pids:
                notfnd.add(f"{sym}|{geneid}|{up}")
                logger.warn(f"No protein found for {sym}|{geneid}|{up}")
                continue
            for pid in pids:
                # HGNC xref
                hgncid=row[0].replace('HGNC:', '')
                rv = dba.ins_xref({'protein_id': pid, 'xtype': 'HGNC ID',
                           'dataset_id': dataset_id, 'value': hgncid})
                if rv:
                    hgnc_ct +=1
                else:
                    db_err_ct +=1
                # MGI xref
                if row[6] !='':
                    mgiid=row[6].replace('MGI:','')
                    rv = dba.ins_xref({'protein_id': pid, 'xtype': 'MGI ID',
                             'dataset_id': dataset_id, 'value': mgiid})
                    if rv:
                        mgi_ct +=1
                    else:
                        db_err_ct +=1
                # add protein.chr values
                rv=dba.do_update({'table':'protein','col':'chr','id':pid,'val':row[4]})
                if rv:
                    chr_ct +=1
                else:
                    db_err_ct +=1

                p=dba.get_protein(pid)
                # add missing syms
                if p['sym']==None:
                    rv = dba.do_update({'table': 'protein', 'col': 'sym', 'id': pid, 'val': sym})
                    if rv:
                        logger.info("Inserted new sym {} for protein {}|{}".format(sym, pid, p['uniprot']))
                        sym_ct += 1
                    else:
                        db_err_ct += 1
                else:
                    logger.warn("Symbol discrepancy: UniProt's=%s, HGNC's=%s" % (p['sym'], sym))
                    symdiscr_ct += 1
                if geneid:
                    # add missing geneids
                    if p['geneid'] == None:
                        rv = dba.do_update({'table': 'protein', 'col': 'geneid', 'id': pid, 'val': geneid})
                        if rv:
                            logger.info("Inserted new geneid {} for protein {}, {}".format(geneid, pid, p['uniprot']))
                            geneid_ct += 1
                        else:
                            db_err_ct += 1
                    else:
                        # Check for geneid discrepancies
                        if p['geneid'] != geneid:
                            logger.warn("GeneID discrepancy: UniProt's={}, HGNC's={}".format(p['geneid'], geneid))
                            geneiddiscr_ct += 1
                pmark[pid]=True
    print("Processed {} lines - {} proteins annotated.".format(ct, len(pmark)))
    if notfnd:
        print("No protein found for {} lines (with UniProts).".format(len(notfnd)))
    print(f"  Updated {chr_ct} protein.chr values.")
    print(f"  Inserted {hgnc_ct} HGNC ID xrefs")
    print(f"  Inserted {mgi_ct} MGI ID xrefs")
    if sym_ct > 0:
        print(f"  Inserted {sym_ct} new HGNC symbols")
    if symdiscr_ct > 0:
        print(f"WARNING: Found {symdiscr_ct} discrepant HGNC symbols. See logfile {logfile} for details")
    if geneid_ct > 0:
        print(f"  Inserted {geneid_ct} new NCBI Gene IDs")
    if geneiddiscr_ct > 0:
        print(f"WARNING: Found {geneiddiscr_ct} discrepant NCBI Gene IDs. See logfile {logfile} for details")
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

    dataset_id = dba.ins_dataset( {'name': 'HGNC', 'source': "Custom download file from https://www.genenames.org/download/custom/", 'app': PROGRAM, 'app_version': __version__, 'url': 'http://www.genenames.org/','comments':'File downloaded with the following column data: HGNC ID Approved symbol Approved name   Status  UniProt ID NCBI Gene ID    Mouse genome database ID'} )
    assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    provs = [ {'dataset_id': dataset_id, 'table_name': 'protein', 'column_name': 'sym', 'comment': "This is only updated with HGNC data if data from UniProt is absent."},
            {'dataset_id': dataset_id, 'table_name': 'protein', 'column_name': 'geneid', 'comment': "This is only updated with HGNC data if data from UniProt is absent."},
            {'dataset_id': dataset_id, 'table_name': 'protein', 'column_name': 'chr'},
            {'dataset_id': dataset_id, 'table_name': 'xref', 'where_clause': f"dataset_id ={dataset_id}", 'comment': 'These are MGI xrefs only.'} ]
    for prov in provs:
        rv=dba.ins_provenance(prov)
        assert rv, f"Error inserting the data into prov for {prov}"
    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")







