#!/usr/bin/env python

""" load-Orthologs.py

Usage:
    load-Orthologs.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-Orthologs.py -? | --help

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
import pandas as pd


__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"
FILE='../data/Orthologs/human_all_hcop_sixteen_column.txt'

TAXID2SP = {'9598': 'Chimp', 
            '9544': 'Macaque',
            '10090': 'Mouse',
            '10116': 'Rat',
            '9615': 'Dog',
            '9796': 'Horse',
            '9913': 'Cow',
            '9823': 'Pig',
            '13616': 'Opossum',
            '9258': 'Platypus',
            '9031': 'Chicken',
            '28377': 'Anole lizard',
            '8364': 'Xenopus',
            '7955': 'Zebrafish',
            '6239': 'C. elegans',
            '7227': 'Fruitfly',
            '4932': 'S.cerevisiae'}

def parse_hcop16(args):
  fn = FILE
  orthos = list()
  line_ct = slmf.wcl(fn)
  with open(fn, 'r') as tsv:
    tsvreader = csv.DictReader(tsv, delimiter='\t')
    for d in tsvreader:

      # ortholog_species	
      # human_entrez_gene	
      # human_ensembl_gene	
      # hgnc_id	
      # human_name	
      # human_symbol	
      # human_chr	
      # human_assert_ids	
      # ortholog_species_entrez_gene	
      # ortholog_species_ensembl_gene	
      # ortholog_species_db_id	
      # ortholog_species_name	
      # ortholog_species_symbol	
      # ortholog_species_chr	
      # ortholog_species_assert_ids	
      # support
      
        d['sources'] = d['support']
        orthos.append(d)
  if not args['--quiet']:
    print(" Generated ortholog dataframe with {} entries".format(len(orthos)))
  ortho_df = pd.DataFrame(orthos)
  return ortho_df

def load(args, dba, dataset_id, logger, logfile):
    ortho_df=parse_hcop16(args)
    proteins = dba.get_proteins()
    line_ct =len(proteins)
    ct = 0
    for prow in proteins:
        ct += 1
        slmf.update_progress(ct/line_ct)
        if prow['sym']: # try first by symbol
            to_df = ortho_df.loc[ortho_df['human_symbol'] == prow['sym']]
        elif prow['geneid']: # then try by GeneID
            to_df = ortho_df.loc[ortho_df['human_entrez_gene'] == prow['geneid']]
        else:      
            continue
        if len(to_df) == 0:
            continue
        for idx, row in to_df.iterrows():
            if row['ortholog_species_symbol'] == '-' and row['ortholog_species_name'] == '-':
                continue
            os = row['ortholog_species']
            if os not in TAXID2SP:
                continue
            sp = TAXID2SP[os]
            init = {'protein_id': prow['id'], 'taxid': os, 'species': sp, 'sources': row['sources'],
                    'symbol': row['ortholog_species_symbol'], 'name': row['ortholog_species_name']}
            # Add MOD DB ID if it's there
            if row['ortholog_species_db_id'] != '-':
                init['db_id'] = row['ortholog_species_db_id']
            # Add NCBI Gene ID if it's there
            if row['ortholog_species_entrez_gene'] != '-':
                init['geneid'] = row['ortholog_species_entrez_gene']
            # Construct MOD URLs for mouse, rat, zebrafish, fly, worm and yeast
            if sp == 'Mouse':
                init['mod_url'] = 'http://www.informatics.jax.org/marker/' + row['ortholog_species_db_id']
            elif sp == 'Rat':
                rgdid = row['ortholog_species_db_id'].replace('RGD:', '')
                init['mod_url'] = 'http://rgd.mcw.edu/rgdweb/report/gene/main.html?id=' + rgdid
            elif sp == 'Zebrafish':
                init['mod_url'] = 'http://zfin.org/' + row['ortholog_species_db_id']
            elif sp == 'Fruitfly':
                init['mod_url'] = "http://flybase.org/reports/%s.html" % row['ortholog_species_db_id']
            elif sp == 'C. elegans':
                init['mod_url'] = 'http://www.wormbase.org/search/gene/' + row['ortholog_species_symbol']
            elif sp == 'S.cerevisiae':
                init['mod_url'] = 'https://www.yeastgenome.org/locus/' + row['ortholog_species_db_id']
            rv = dba.ins_ortholog(init)
        
       
    


        






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

    # dataset_id = dba.ins_dataset( {'name': 'Orthologs', 'source': "File ftp://ftp.ebi.ac.uk/pub/databases/genenames/hcop/human_all_hcop_sixteen_column.txt.gz", 'app': PROGRAM, 'app_version': __version__, 'url': 'https://ftp.ebi.ac.uk/pub/databases/genenames/hcop/'} )
    # assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    dataset_id=49
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'ortholog','comment':'Orthologs are majority vote from the OMA, EggNOG and InParanoid resources as per HGNC.'} ]
    # for prov in provs:
    #     rv=dba.ins_provenance(prov)
    #     assert rv, f"Error inserting the data into prov for {prov}"
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")