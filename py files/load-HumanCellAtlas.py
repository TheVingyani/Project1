#!/usr/bin/env python

"""

Usage:
    load-HumanCellAtlas.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-HumanCellAtlas.py -? | --help

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
import numpy as np
from collections import defaultdict


__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"

RNA_FILE  ='../data/HCA/s1.csv'
LOC_FILE  ='../data/HCA/s6.csv'

COMPARTMENTS = {'Nucleus': ('Nucleus', 'GO:0005634'),
                'Nucleoplasm': ('Nucleoplasm', 'GO:0005654'),
                'Nuclear bodies': ('Nuclear bodies', 'GO:0016604'),
                'Nuclear speckles': ('Nuclear speckles', 'GO:0016607'),
                'Nuclear membrane': ('Nuclear membrane', 'GO:0031965'),
                'Nucleoli': ('Nucleoli', 'GO:0005730'),
                'Nucleoli (Fibrillar center)': ('Nucleoli fibrillar center', 'GO:0001650'),
                'Cytosol': ('Cytosol', 'GO:0005829'),
                'Cytoplasmic bodies': ('Cytoplasmic bodies', 'GO:0000932'),
                'Rods and Rings': ('Rods & Rings', ''),
                'Lipid droplets': ('Lipid droplets', 'GO:0005811'),
                'Aggresome': ('Aggresome', 'GO:0016235'),
                'Mitochondria': ('Mitochondria', 'GO:0005739'),
                'Microtubules': ('Microtubules', 'GO:0015630'),
                'Microtubule ends': ('Microtubule ends', 'GO:1990752'),
                'Microtubule organizing center': ('Microtubule organizing center', 'GO:0005815'),
                'Centrosome': ('Centrosome', 'GO:0005813'),
                'Mitotic spindle': ('Mitotic spindle', 'GO:0072686'),
                'Cytokinetic bridge': ('Cytokinetic bridge ', 'GO:0045171'),
                'Midbody': ('Midbody', 'GO:0030496'),
                'Midbody ring': ('Midbody ring', 'GO:0070938'),
                'Intermediate filaments': ('Intermediate filaments', 'GO:0045111'),
                'Actin filaments': ('Actin filaments', 'GO:0015629'),
                'Focal Adhesions': ('Focal adhesion sites', 'GO:0005925'),
                'Endoplasmic reticulum': ('Endoplasmic reticulum', 'GO:0005783'),
                'Golgi apparatus': ('Golgi apparatus', 'GO:0005794'),
                'Vesicles': ('Vesicles', 'GO:0043231'),
                'Plasma membrane': ('Plasma membrane', 'GO:0005886'),
                'Cell Junctions': ('Cell Junctions', 'GO:0030054')}

def calc_pctiles():
  pctiles = {}
  df = pd.read_csv(RNA_FILE)
  cell_lines = [c for c in df.columns[2:]]
  for cl in cell_lines:
    a = df[cl].values
    a = np.delete(a, np.where(a == 0))
    cl = cl.replace(' (TPM)', '') 
    pctiles[cl] = (np.percentile(a, 33), np.percentile(a, 66))
  return pctiles

def calc_qual_value(tpm, pctiles):
  # pctiles here is a tuple of 33rd and 66th percentiles
  if tpm == 0:
    qv = 'Not detected'
  elif tpm <= pctiles[0]:
    qv = 'Low'
  elif tpm <= pctiles[1]:
    qv = 'Medium'
  else:
    qv = 'High'
  return qv


def load(args, dba, exp_dataset_id,cpt_dataset_id, logger, logfile):
    pctiles = calc_pctiles()
    line_ct = slmf.wcl(RNA_FILE)
    ct = 0
    k2pids = defaultdict(list)
    notfnd = set()
    dba_err_ct = 0
    pmark = {}
    exp_ct = 0
    with open(RNA_FILE, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        header = csvreader.__next__()
        for row in csvreader:
            ct += 1
            slmf.update_progress(ct/line_ct)
            sym = row[1]
            ensg = row[0]
            k = "%s|%s"%(sym,ensg)
            if k in k2pids:
                # we've already found it
                pids = k2pids[k]
            elif k in notfnd:
                # we've already not found it
                continue
            else:
                # look it up
                targets = dba.find_protein_ids({'sym': sym}, False)
                if not targets:
                    targets = dba.find_protein_ids_by_xref({'xtype': 'Ensembl', 'value': ensg})
                if not targets:
                    notfnd.add(k)
                    continue
                pids = []
                for t in targets:
                    pids.append(t)
                k2pids[k] = pids
            for pid in pids:
                cell_lines = [c.replace(' (TPM)', '') for c in header[2:]]
                for (i,cl) in enumerate(cell_lines):
                    tpm_idx = i + 2 # add two because row has ENSG and Gene at beginning
                    tpm = float(row[tpm_idx])
                    qv = calc_qual_value( tpm, pctiles[cl] )
                    rv = dba.ins_expression( {'protein_id': pid, 'etype': 'HCA RNA',
                                            'tissue': 'Cell Line '+cl, 
                                            'qual_value': qv, 'number_value': tpm} )
                    if not rv:
                        dba_err_ct += 1
                        continue
                    exp_ct += 1
                pmark[pid] = True

            
    print("\n Inserted {} new expression rows for {} proteins.".format(exp_ct, len(pmark)))
    line_ct = slmf.wcl(LOC_FILE)
    ct = 0
    k2pids = defaultdict(list)
    notfnd = set()
    dba_err_ct = 0
    pmark = {}
    cpt_ct = 0
    with open(LOC_FILE, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        header = csvreader.__next__()
        for row in csvreader:
            ct += 1
            slmf.update_progress(ct/line_ct)
            uniprot = row[2]
            sym = row[1]
            k = "%s|%s"%(uniprot,sym)
            if k in k2pids:
                # we've already found it
                pids = k2pids[k]
            elif k in notfnd:
                # we've already not found it
                continue
            else:
                # look it up
                targets = dba.find_protein_ids({'uniprot': uniprot}, False)
                if not targets:
                    targets = dba.find_protein_ids({'sym': sym}, False)
                if not targets:
                    notfnd.add(k)
                    continue
                pids = []
                for t in targets:
                    pids.append(t)
                k2pids[k] = pids
            for pid in pids:
                compartments = [c for c in header[3:-5]]
                for (i,c) in enumerate(compartments):
                    val_idx = i + 3 # add three because row has ENSG,Gene,Uniprot at beginning
                    val = int(row[val_idx])
                    if val == 0:
                        continue
                    rel = row[-5]
                    if rel == 'Uncertain':
                        continue
                    rv = dba.ins_compartment( {'protein_id': pid, 'ctype': 'Human Cell Atlas',
                                            'go_id': COMPARTMENTS[c][1], 
                                            'go_term': COMPARTMENTS[c][0], 'reliability': rel} )
                    if not rv:
                        dba_err_ct += 1
                        continue
                    cpt_ct += 1
                pmark[pid] = True
            
    print(" \n Inserted {} new compartment rows for {} protein.s".format(cpt_ct, len(pmark)))


         
    

            
                    
            
      
  
  


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

    # # Dataset
    # exp_dataset_id = dba.ins_dataset( {'name': 'Human Cell Atlas Expression', 'source': 'File Table S1 from http://science.sciencemag.org/content/suppl/2017/05/10/science.aal3321.DC1', 'app': PROGRAM, 'app_version': __version__, 'url': 'http://science.sciencemag.org/content/356/6340/eaal3321.full', 'comments': 'Qualitative expression values are generated by the loading app.'} )
    # assert exp_dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # cpt_dataset_id = dba.ins_dataset( {'name': 'Human Cell Atlas Compartments', 'source': 'File Table S6 from http://science.sciencemag.org/content/suppl/2017/05/10/science.aal3321.DC1', 'app': PROGRAM, 'app_version': __version__, 'url': 'http://science.sciencemag.org/content/356/6340/eaal3321.full'} )
    # assert cpt_dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # # Provenance
    # provs = [ {'dataset_id': exp_dataset_id, 'table_name': 'expression', 'where_clause': "etype = 'HCA RNA'", 'comment': 'TPM and qualitative expression values are derived from file Table S1 from http://science.sciencemag.org/content/suppl/2017/05/10/science.aal3321.DC1'},
    #             {'dataset_id': cpt_dataset_id, 'table_name': 'compartment', 'where_clause': "ctype = 'Human Cell Atlas'"} ]
    # for prov in provs:
    #     rv = dba.ins_provenance(prov)
    #     assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)
    
    exp_dataset_id=67
    cpt_dataset_id=68
    
    #print(dataset_id)

    load(args , dba,exp_dataset_id,cpt_dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    