#!/usr/bin/env python

"""

Usage:
    load-eRAM.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-eRAM.py -? | --help

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


__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"

DOWNLOAD_DIR = '../data/IMPC/'
BASE_URL = 'ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/results/'
GENO_PHENO_FILE = 'genotype-phenotype-assertions-IMPC.csv.gz'
STAT_RES_FILE = 'statistical-results-ALL.csv.gz'



def load(args, dba, dataset_id, logger, logfile):
    fn = DOWNLOAD_DIR + GENO_PHENO_FILE.replace('.gz', '')
    line_ct = slmf.wcl(fn)
    with open(fn, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        header = csvreader.__next__() # skip header line
        ct = 0
        pt_ct = 0
        pmark = {}
        sym2nhps = {}
        notfnd = set()
        skip_ct = 0
        dba_err_ct = 0
        # 0: marker_accession_id
        # 1: marker_symbol
        # 2: phenotyping_center
        # 3: colony_id
        # 4: sex
        # 5: zygosity
        # 6: allele_accession_id
        # 7: allele_symbol
        # 8: allele_name
        # 9: strain_accession_id
        # 10: strain_name
        # 11: project_name
        # 12: project_fullname
        # 13: pipeline_name
        # 14: pipeline_stable_id
        # 15: procedure_stable_id
        # 16: procedure_name
        # 17: parameter_stable_id
        # 18: parameter_name
        # 19: top_level_mp_term_id
        # 20: top_level_mp_term_name
        # 21: mp_term_id
        # 22: mp_term_name
        # 23: p_value
        # 24: percentage_change
        # 25: effect_size
        # 26: statistical_method
        # 27: resource_name
        for row in csvreader:
            ct += 1
            slmf.update_progress(ct/line_ct)

            sym = row[1]
            if not row[21] and not row[22]:
                # skip data with neither a term_id or term_name (IMPC has some of these)
                skip_ct += 1
                continue
            if sym in sym2nhps:
                # we've already found it
                nhpids = sym2nhps[sym]
            elif sym in notfnd:
                # we've already not found it
                continue
            else:
                nhps = dba.find_nhprotein_ids({'sym': sym}, species = 'Mus musculus')
                if not nhps:
                    notfnd.add(sym)
                    logger.warning("No nhprotein found for symbol {}".format(sym))
                    continue
                nhpids = []
                for nhp in nhps:
                    nhpids.append(nhp)
                sym2nhps[sym] = nhpids # save this mapping so we only lookup each nhprotein once
            pval = None
            if row[23] and row[23] != '':
                try:
                    pval = float(row[23])
                except:
                    logger.warning("Problem converting p_value {} for row {}".format(row[23], ct))
            if row[4]=='not_considered':
                sex=None
            else:
                sex=row[4]
            for nhpid in nhpids:
                rv = dba.ins_phenotype({'nhprotein_id': nhpid, 'ptype': 'IMPC', 'top_level_term_id': row[19], 'top_level_term_name': row[20], 'term_id': row[21], 'term_name': row[22], 'p_value': pval, 'percentage_change': row[24], 'effect_size': row[25], 'procedure_name': row[16], 'parameter_name': row[18], 'statistical_method': row[26], 'sex': sex, 'gp_assoc': 1})
                if rv:
                    pmark[nhpid] = True
                    pt_ct += 1
                else:
                    dba_err_ct += 1
             
    print("\n{} lines processed.".format(ct))
    print("Loaded {} IMPC phenotypes for {} nhproteins".format(pt_ct, len(pmark.keys())))
    if notfnd:
        print("No nhprotein found for {} gene symbols. See logfile {} for details.".format(len(notfnd), logfile))
    if skip_ct > 0:
        print("Skipped {} lines with no term_id or term_name.".format(skip_ct))
    if dba_err_ct > 0:
        print("WARNING: {} DB errors occurred. See logfile {} for details.".format(dba_err_ct, logfile))

    fn = DOWNLOAD_DIR + STAT_RES_FILE.replace('.gz', '')
    line_ct = slmf.wcl(fn)
    with open(fn, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        header = csvreader.__next__() # skip header line
        #print(header)
        ct = 0
        pt_ct = 0
        pmark = {}
        sym2nhps = {}
        notfnd = set()
        skip_ct = 0
        pv_ct = 0
        dba_err_ct = 0
        # 0: phenotyping_center
        # 1: intercept_estimate
        # 2: procedure_id
        # 3: mutant_biological_model_id
        # 4: rotated_residuals_test
        # 5: weight_effect_p_value
        # 6: male_mutant_count
        # 7: pipeline_stable_key
        # 8: female_ko_effect_p_value
        # 9: pipeline_stable_id
        # 10: parameter_stable_key
        # 11: data_type
        # 12: parameter_stable_id
        # 13: interaction_significant
        # 14: strain_accession_id
        # 15: control_selection_method
        # 16: parameter_name
        # 17: allele_name
        # 18: phenotyping_center_id
        # 19: weight_effect_stderr_estimate
        # 20: weight_effect_parameter_estimate
        # 21: procedure_stable_id
        # 22: status
        # 23: sex_effect_parameter_estimate
        # 24: female_ko_effect_stderr_estimate
        # 25: female_percentage_change
        # 26: group_2_residuals_normality_test
        # 27: marker_accession_id
        # 28: mp_term_name
        # 29: group_1_residuals_normality_test
        # 30: genotype_effect_p_value
        # 31: dependent_variable
        # 32: resource_name
        # 33: project_id
        # 34: procedure_name
        # 35: doc_id
        # 36: top_level_mp_term_id
        # 37: allele_accession_id
        # 38: blups_test
        # 39: null_test_p_value
        # 40: p_value
        # 41: marker_symbol
        # 42: control_biological_model_id
        # 43: pipeline_name
        # 44: sex
        # 45: interaction_effect_p_value
        # 46: colony_id
        # 47: project_name
        # 48: female_ko_parameter_estimate
        # 49: female_mutant_count
        # 50: organisation_id
        # 51: external_db_id
        # 52: female_control_count
        # 53: intermediate_mp_term_id
        # 54: db_id
        # 55: male_ko_effect_p_value
        # 56: top_level_mp_term_name
        # 57: metadata_group
        # 58: sex_effect_stderr_estimate
        # 59: zygosity
        # 60: male_percentage_change
        # 61: sex_effect_p_value
        # 62: mp_term_id
        # 63: male_ko_effect_stderr_estimate
        # 64: additional_information
        # 65: statistical_method
        # 66: _version_
        # 67: intercept_estimate_stderr_estimate
        # 68: male_control_count
        # 69: intermediate_mp_term_name
        # 70: strain_name
        # 71: classification_tag
        # 72: effect_size
        # 73: procedure_stable_key
        # 74: allele_symbol
        # 75: resource_id
        # 76: group_2_genotype
        # 77: variance_significant
        # 78: pipeline_id
        # 79: group_1_genotype
        # 80: male_ko_parameter_estimate
        # 81: genotype_effect_parameter_estimate
        # 82: categories
        # 83: parameter_id
        # 84: batch_significant
        # 85: genotype_effect_stderr_estimate
        # 86: resource_fullname
        for row in csvreader:
            #print(row)
            ct += 1
            slmf.update_progress(ct/line_ct)
            sym = row[41]
            if not row[62] and not row[28]:
            # skip lines with neither a term_id or term_name
                skip_ct += 1
                continue
            if sym in sym2nhps:
            # we've already found it
                nhpids = sym2nhps[sym]
            elif sym in notfnd:
            # we've already not found it
                continue
            else:
                nhps = dba.find_nhprotein_ids({'sym': sym}, species = 'Mus musculus')
            if not nhps:
                notfnd.add(sym)
                logger.warn("No nhprotein found for symbol {}".format(sym))
                continue
            nhpids = []
            for nhp in nhps:
                nhpids.append(nhp)
            sym2nhps[sym] = nhpids # save this mapping so we only lookup each nhprotein once
            pval = None
            if row[40] and row[40] != '':
                try:
                    pval = float(row[40])
                except:
                    logger.warn("Problem converting p_value {} for row {}".format(row[40], ct))
            #print(row[44])
            if row[44]=='not_considered':
                sex=None
            else:
                sex=row[44]
            #print(sex)
            for nhpid in nhpids:
                rv = dba.ins_phenotype({'nhprotein_id': nhpid, 'ptype': 'IMPC', 'top_level_term_id': row[36], 'top_level_term_name': row[56], 'term_id': row[62], 'term_name': row[28], 'p_value': pval, 'effect_size': row[72], 'procedure_name': row[34], 'parameter_name': row[16], 'statistical_method': row[65], 'sex': sex, 'gp_assoc': 0})
                if rv:
                    pmark[nhpid] = True
                    pt_ct += 1
                else:
                    dba_err_ct += 1
            

    print("\n{} lines processed.".format(ct))
    print("Loaded {} IMPC phenotypes for {} nhproteins".format(pt_ct, len(pmark)))
    if notfnd:
        print("No nhprotein found for {} gene symbols. See logfile {} for details.".format(len(notfnd), logfile))
    if skip_ct > 0:
        print("Skipped {} lines with no term_id/term_name or no p-value.".format(skip_ct))
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

    # # Dataset
    # dataset_id = dba.ins_dataset( {'name': 'IMPC Phenotypes', 'source': "Files %s and %s from ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/results/"%(os.path.basename(GENO_PHENO_FILE), os.path.basename(STAT_RES_FILE)), 'app': PROGRAM, 'app_version': __version__} )
    # assert dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # # Provenance
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'phenotype', 'where_clause': "ptype = 'IMPC'"} ]
    # for prov in provs:
    #     rv = dba.ins_provenance(prov)
    #     assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)
    
    dataset_id = 118
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    