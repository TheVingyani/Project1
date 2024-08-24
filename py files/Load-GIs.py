#!/usr/bin/env python
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

from urllib.request import urlretrieve
import gzip
import slm_util_functions as slmf

__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM = os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"

DOWNLOAD_DIR = '../data/GI/'
BASE_URL = 'ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/by_organism/'
FILENAME = 'HUMAN_9606_idmapping_selected.tab.gz'



def download(args):
    gzfn = DOWNLOAD_DIR+FILENAME
    if os.path.exists(gzfn):
        os.remove(gzfn)
    fn = gzfn.replace('.gz','')
    if os.path.exists(fn):
        os.remove(fn)
    urlretrieve(BASE_URL+FILENAME,gzfn)
    ifh = gzip.open(gzfn)
    ofh = open(fn,'wb')
    ofh.writable(ifh.read())
    ifh.close()
    ofh.close()


def load(args):
    
    infile = FILENAME.replace('.gz','')
    line_ct = slmf.wcl(DOWNLOAD_DIR+infile)
    # ID Mappiing fields
    # 1. UniProtKB-AC
    # 2. UniProtKB-ID
    # 3. GeneID (EntrezGene)
    # 4. RefSeq
    # 5. GI
    # 6. PDB
    # 7. GO
    # 8. UniRef100
    # 9. UniRef90
    # 10. UniParc
    # 12. PIRRef50
    # 11. Uni
    # 13. NCBI-taxon
    # 14. MIM
    # 15. UniGene
    # 16. PubMed
    # 17. EMBL
    # 18. EMBL-CDS
    # 19. Ensembl
    # 20. Ensembl_TRS
    # 21. Ensembl_PRO
    # 22. Additional PubMed
    # 
    
    ct=0

    with open(DOWNLOAD_DIR+infile, 'r') as tsv:
            ct = 0
            xref_ct = 0
            skip_ct_gi = 0
            skip_ct_pro = 0
            dba_err_ct = 0
            tmark = {}
            for line in tsv:
                slmf.update_progress(ct/line_ct)
                data = line.split('\t')
                if ct==0:
                    print('\n')
                    print('up',data[0])
                    print('\nGI',data[4].split('; '))
                ct=ct+1
                up = data[0].strip()
                if not data[4]:
                    skip_ct_gi +=1
                    continue
                protein_ids=dba.find_protein_ids({'uniprot':up})
                if not protein_ids:
                    skip_ct_pro +=1
                    continue
                    
                for pid in protein_ids:


                    tmark[pid]=True

                    for gi in data[4].split('; '):
                        rv =dba.ins_xref({'protein_id': pid, 'xtype': 'NCBI GI', 'dataset_id': dataset_id, 'value': gi})

                        if rv:
                            xref_ct += 1
                        else:
                            dba_err_ct += 1
            print(f"\n{ct} row processed")
            print(f"\ntotal {xref_ct} new GI xref for targets {len(tmark)}")
            print(f"\n skipped {skip_ct_gi} with no GI id")
            print(f"\n skipped {skip_ct_pro} with no protein_id")
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

    dataset_id = dba.ins_dataset( {'name': 'NCBI GI Numbers', 'source': "UniProt ID Mapping file ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/by_organism/HUMAN_9606_idmapping_selected.tab.gz", 'app': PROGRAM, 'app_version': __version__, 'url': 'http://www.uniprot.org/'} )
    assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    provs = [ {'dataset_id': dataset_id, 'table_name': 'xref', 'where_clause': "dataset_id = %d"%dataset_id} ]
    for prov in provs:
        rv=dba.ins_provenance(prov)
        assert rv, f"Error inserting the data into prov for {prov}"
    print(dataset_id)
    # we will manually downlaod the file HUMAN_9606_idmapping_selected.tab.gz
    #download(args) 
    load(args)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")