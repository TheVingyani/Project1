#!/usr/bin/env python

"""

Usage:
    load-ClinVar.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-ClinVar.py -? | --help

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
from collections import defaultdict
from dateutil.parser import parse as parse_date

__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"
DOWNLOAD_DIR = '../data/ClinVar/'
BASE_URL = 'ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/'
FILENAME = 'variant_summary.txt.gz'
OUTFILE = 'ClinVar.csv'


def extract_phenotypes(args, logfile, logger):
    fn = DOWNLOAD_DIR + FILENAME
    fn = fn.replace('.gz', '')
    line_ct = slmf.wcl(fn)
    ct = 0
    mm_ct = 0

    phenotypes = defaultdict(set)
    with open(fn, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        header = tsvreader.__next__()
        ct += 1
        for row in tsvreader:
            # 0: '#AlleleID'
            # 1: 'Type'
            # 2: 'Name'
            # 3: 'GeneID'
            # 4: 'GeneSymbol'
            # 5: 'HGNC_ID'
            # 6: 'ClinicalSignificance'
            # 7: 'ClinSigSimple'
            # 8: 'LastEvaluated'
            # 9: 'RS# (dbSNP)'
            # 10: 'nsv/esv (dbVar)'
            # 11: 'RCVaccession'
            # 12: 'PhenotypeIDS'
            # 13: 'PhenotypeList'
            # 14: 'Origin'
            # 15: 'OriginSimple'
            # 16: 'Assembly'
            # 17: 'ChromosomeAccession'
            # 18: 'Chromosome'
            # 19: 'Start'
            # 20: 'Stop'
            # 21: 'ReferenceAllele'
            # 22: 'AlternateAllele'
            # 23: 'Cytogenetic'
            # 24: 'ReviewStatus'
            # 25: 'NumberSubmitters'
            # 26: 'Guidelines'
            # 27: 'TestedInGTR'
            # 28: 'OtherIDs'
            # 29: 'SubmitterCategories'
            # 30: 'VariationID'
            ct += 1
            slmf.update_progress(ct/line_ct)
            assert len(header) == len(row), "Parsing error at line {}".format(ct)
            pts = row[13].split(';')
            ids = row[12].split(';')
            #assert len(pts) == len(ids), "Phenotypes:IDs mismatch at line {}: {} vs {}".format(ct,pts,ids)
            if len(pts) != len(ids):
                mm_ct += 1
                logger.warning("PhenotypeIDS vs PhenotypeList mismatch on line {}. Skipping.".format(ct))
                continue
            for i,pt in enumerate(pts):
                for sv in ids[i].split(','):
                    phenotypes[pt].add(sv)
    if mm_ct > 0:
        print("WARNING: Skipped {} lines with mismatched PhenotypeIDS vs. PhenotypeList. See logfile {} for details.".format(mm_ct, logfile))
    return phenotypes


def load_phenotypes(args, dba, logfile, phenotypes):
    ct = 0
    cvpt_ct = 0
    xref_ct = 0
    pt2id = {}
    dba_err_ct = 0
    line_ct = len(list(phenotypes.keys()))
    for pt,xrefs in phenotypes.items():
        ct += 1
        slmf.update_progress(ct/line_ct)
        cvpt_id = dba.ins_clinvar_phenotype({'name': pt})
        #print(cvpt_id)
        if not cvpt_id:
            dba_err_ct += 1
            continue
        cvpt_ct += 1
        pt2id[pt] = cvpt_id
        for xr in xrefs:
            xr = xr.replace('Human Phenotype Ontology:HP:', 'HPO:')
            if ':' not in xr:
                continue
            #print "DEBUG: Pt: {}\n       Xrefs: {}".format(pt,xrefs)
            #print(xr)
            if '|' in xr:
                for a in xr.split('|'):
                    src=xr.split(':')[-2]
                    val = xr.split(':')[-1]
                    #[src,val] = a.split(':')
                    rv = dba.ins_clinvar_phenotype_xref({'clinvar_phenotype_id': cvpt_id,
                                                        'source': src, 'value': val})
                    if not rv:
                        dba_err_ct += 1
                        continue
                    xref_ct += 1
            else:
                src=xr.split(':')[-2]
                val = xr.split(':')[-1]
                rv = dba.ins_clinvar_phenotype_xref({'clinvar_phenotype_id': cvpt_id,
                                                        'source': src, 'value': val})
                if not rv:
                    dba_err_ct += 1
                    continue
                xref_ct += 1

    print(" \n Inserted {} new clinvar_phenotype rows".format(cvpt_ct))
    print(" \n Inserted {} new clinvar_phenotype_xref rows".format(xref_ct))
    if dba_err_ct > 0:
        print("WARNNING: {} DB errors occurred. See logfile {} for details.".format(dba_err_ct, logfile))

    return pt2id


def load_associations(args, dba, logfile, logger, ptname2id):
    fn = DOWNLOAD_DIR + FILENAME
    fn = fn.replace('.gz', '')
    line_ct = slmf.wcl(fn)

    ct = 0
    cv_ct = 0
    skip_ct = 0
    notfnd = set()
    pmark = {}
    dba_err_ct = 0
    want_status = ['reviewed by expert panel', 'criteria provided, multiple submitters, no conflicts']
    with open(fn, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        header = tsvreader.__next__()
        ct += 1
        for row in tsvreader:
            # 0: 'AlleleID'
            # 1: 'Type'
            # 2: 'Name'
            # 3: 'GeneID'
            # 4: 'GeneSymbol'
            # 5: 'HGNC_ID'
            # 6: 'ClinicalSignificance'
            # 7: 'ClinSigSimple'
            # 8: 'LastEvaluated'
            # 9: 'RS# (dbSNP)'
            # 10: 'nsv/esv (dbVar)'
            # 11: 'RCVaccession'
            # 12: 'PhenotypeIDS'
            # 13: 'PhenotypeList'
            # 14: 'Origin'
            # 15: 'OriginSimple'
            # 16: 'Assembly'
            # 17: 'ChromosomeAccession'
            # 18: 'Chromosome'
            # 19: 'Start'
            # 20: 'Stop'
            # 24: 'ReviewStatus'
            # 25: 'NumberSubmitters'
            # 27: 'TestedInGTR'
            # 29: 'SubmitterCategories'
            ct += 1
            slmf.update_progress(ct/line_ct)
            if row[24] not in want_status:
                skip_ct += 1
                continue
            targets = dba.find_protein_ids({'sym': row[4]})
            if not targets:
                targets = dba.find_protein_ids({'geneid': row[3]})
            if not targets:
                notfnd.add("%s|%s"%(row[4],row[3]))
                continue
            if row[27] == 'Y':
                tig = 1
            else:
                tig = 0
            for t in targets:
                pid = t
            for pt in row[13].split(';'):
                if pt not in ptname2id:
                    continue
                try:
                    date_data=parse_date(row[8])
                    
                except:
                    date_data=None
                rv = dba.ins_clinvar({'protein_id': pid, 'clinvar_phenotype_id': ptname2id[pt], 'alleleid': int(row[0]), 'type': row[1], 'name': row[2], 'review_status': row[24], 'clinical_significance': row[6], 'clin_sig_simple': int(row[7]), 'last_evaluated': date_data, 'dbsnp_rs': int(row[9]), 'dbvarid': row[10], 'origin': row[14], 'origin_simple': row[15], 'assembly': row[16], 'chr': row[18], 'chr_acc': row[17], 'start': int(row[19]), 'stop': int(row[20]), 'number_submitters': int(row[25]), 'tested_in_gtr': tig, 'submitter_categories': int(row[29])})
                if not rv:
                    dba_err_ct += 1
                    continue
                cv_ct += 1
                pmark[pid] = True
    for k in notfnd:
        logger.warning("No target found for {}".format(k))
    print("{} lines processed.".format(ct))
    print("  Inserted {} new clinvar rows".format(cv_ct))
    if notfnd:
        print("No target found for {} symbols/geneids. See logfile {} for details.".format(len(notfnd), logfile))
    if dba_err_ct > 0:
        print("WARNNING: {} DB errors occurred. See logfile {} for details.".format(dba_err_ct, logfile))

    return True     
    

            
                    
            
      
  
  


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

    # Dataset
    # dataset_id = dba.ins_dataset( {'name': 'ClinVar', 'source': 'Download file: ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/variant_summary.txt.gz', 'app': PROGRAM, 'app_version': __version__, 'url': 'https://www.ncbi.nlm.nih.gov/clinvar/', 'comments': 'Only phenotype associations with review status of "criteria provided, multiple submitters, no conflicts" or "reviewed by expert panel" are loaded into TCRD.'} )
    # assert dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # # Provenance
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'clinvar'},
    #             {'dataset_id': dataset_id, 'table_name': 'clinvar_phenotype'},
    #             {'dataset_id': dataset_id, 'table_name': 'clinvar_phenotype_xref'} ]
    # for prov in provs:
    #     rv = dba.ins_provenance(prov)
    #     assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)
    
    dataset_id = 115
    
    #print(dataset_id)

    # phenotypes = extract_phenotypes(args, logfile, logger)
    # ptname2id = load_phenotypes(args, dba, logfile, phenotypes)
    ptname2id = dba.clinvar_phenotype()
    load_associations(args, dba, logfile, logger, ptname2id)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    