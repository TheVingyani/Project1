#!/usr/bin/env python

"""

Usage:
    TIN-X.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    TIN-X.py -? | --help

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

DO_BASE_URL = 'https://raw.githubusercontent.com/DiseaseOntology/HumanDiseaseOntology/master/src/ontology/'
DO_DOWNLOAD_DIR = '../data/DiseaseOntology/'
DO_OBO = 'doid.obo'
JL_BASE_URL = 'http://download.jensenlab.org/'
JL_DOWNLOAD_DIR = '../data/JensenLab/'
DISEASE_FILE = 'disease_textmining_mentions.tsv'
PROTEIN_FILE = 'human_textmining_mentions.tsv'
# Output CSV files:
PROTEIN_NOVELTY_FILE = '../data/TIN-X/TCRDv%s/ProteinNovelty.csv'%MKDEV_VER
DISEASE_NOVELTY_FILE = '../data/TIN-X/TCRDv%s/DiseaseNovelty.csv'%MKDEV_VER
PMID_RANKING_FILE = '../data/TIN-X/TCRDv%s/PMIDRanking.csv'%MKDEV_VER
IMPORTANCE_FILE = '../data/TIN-X/TCRDv%s/Importance.csv'%MKDEV_VER







def load(args,dba):
    # The results of parsing the input mentions files will be the following dictionaries:
    pid2pmids = {}  # 'TCRD.protein.id,UniProt' => set of all PMIDs that mention the protein
                    # Including the UniProt accession in the key is just for convenience when
                    # checking the output. It is not used for anything.
    doid2pmids = {} # DOID => set of all PMIDs that mention the disease
    pmid_disease_ct = {} # PMID => count of diseases mentioned in a given paper 
    pmid_protein_ct = {} # PMID => count of proteins mentioned in a given paper 

    # First parse the Disease Ontology OBO file to get DO names and defs
    dofile = DO_DOWNLOAD_DIR + DO_OBO
    print("\nParsing Disease Ontology file {}".format(dofile))
    do_parser = obo.Parser(dofile)
    do = {}
    for stanza in do_parser:
        do[stanza.tags['id'][0].value] = stanza.tags
    print("  Got {} Disease Ontology terms".format(len(do)))

    print(list(do.keys())[:1])
    print(list(do.values())[:1])

    fn = JL_DOWNLOAD_DIR+PROTEIN_FILE
    line_ct = slmf.wcl(fn)
    with open(fn, 'r') as tsvf:
        ct = 0
        skip_ct = 0
        notfnd = set()
        for line in tsvf:
            ct += 1
            slmf.update_progress(ct/line_ct)
            if not line.startswith('ENSP'):
                skip_ct += 1
                continue
            data = line.rstrip().split('\t')
            ensp = data[0]
            pmids = set([int(pmid) for pmid in data[1].split()])
            targets = dba.find_proteins({'stringid': ensp})
            if not targets:
                # if we don't find a target by stringid, which is the more reliable and
                # prefered way, try by Ensembl xref
                targets = dba.find_proteins_by_xref({'xtype': 'Ensembl', 'value': ensp})
            if not targets:
                notfnd.add(ensp)
                continue
            for t in targets:
                p = t
                k = "%s,%s" % (p['id'], p['uniprot'])
                if k in pid2pmids:
                    pid2pmids[k] = pid2pmids[k].union(pmids)
                else:
                    pid2pmids[k] = set(pmids)
                for pmid in pmids:
                    if pmid in pmid_protein_ct:
                        pmid_protein_ct[pmid] += 1.0
                    else:
                        pmid_protein_ct[pmid] = 1.0
    for ensp in notfnd:
        logger.warning("No target found for {}".format(ensp))
    print("\n{} lines processed.".format(ct))
    print("  Skipped {} non-ENSP lines".format(skip_ct))
    print("  Saved {} protein to PMIDs mappings".format(len(pid2pmids)))
    print("  Saved {} PMID to protein count mappings".format(len(pmid_protein_ct)))
    if notfnd:
        print("  No target found for {} ENSPs. See logfile {} for details.".format(len(notfnd), logfile))
    
    fn = JL_DOWNLOAD_DIR+DISEASE_FILE
    line_ct = slmf.wcl(fn)

    with open(fn, 'r') as tsvf:
        ct = 0
        skip_ct = 0
        notfnd = set()
        for line in tsvf:
            ct += 1
            slmf.update_progress(ct/line_ct)
            if not line.startswith('DOID:'):
                skip_ct += 1
                continue
            data = line.rstrip().split('\t')
            doid = data[0]
            pmids = set([int(pmid) for pmid in data[1].split()])
            if doid not in do:
                logger.warn("%s not found in DO" % doid)
                notfnd.add(doid)
                continue
            if doid in doid2pmids:
                doid2pmids[doid] = doid2pmids[doid].union(pmids)
            else:
                doid2pmids[doid] = set(pmids)
            for pmid in pmids:
                if pmid in pmid_disease_ct:
                    pmid_disease_ct[pmid] += 1.0
                else:
                    pmid_disease_ct[pmid] = 1.0
    
    print("\n {} lines processed.".format(ct))
    print("  Skipped {} non-DOID lines".format(skip_ct))
    print("  Saved {} DOID to PMIDs mappings".format(len(doid2pmids)))
    print("  Saved {} PMID to disease count mappings".format(len(pmid_disease_ct)))
    if notfnd:
        print("WARNNING: No entry found in DO map for {} DOIDs. See logfile {} for details.".format(len(notfnd), logfile))

    # To calculate novelty scores, each paper (PMID) is assigned a
    # fractional target (FT) score of one divided by the number of targets
    # mentioned in it. The novelty score of a given protein is one divided
    # by the sum of the FT scores for all the papers mentioning that
    # protein.
    print("\nComputing protein novely scores")
    ct = 0
    with open(PROTEIN_NOVELTY_FILE, 'wb') as pnovf:
        pnovf.write(b"Protein ID,UniProt,Novelty\n")
        for k in pid2pmids.keys():
            ct += 1
            ft_score_sum = 0.0
            for pmid in pid2pmids[k]:
                ft_score_sum += 1.0 / pmid_protein_ct[pmid]
                novelty = 1.0 / ft_score_sum
                #pnovf.write(b"%s,%.8f\n" % (k, novelty) )
            pnovf.write(b"%s,%.8f\n" % (k.encode('utf-8'), novelty))
    print("\nWrote {} novelty scores to file {}".format(ct, PROTEIN_NOVELTY_FILE))

    print("\nComputing disease novely scores")
    # Exactly as for proteins, but using disease mentions
    ct = 0
    with open(DISEASE_NOVELTY_FILE, 'wb') as dnovf:
        dnovf.write(b"DOID,Novelty\n")
        for doid in doid2pmids.keys():
            ct += 1
            ft_score_sum = 0.0
            for pmid in doid2pmids[doid]:
                ft_score_sum += 1.0 / pmid_disease_ct[pmid]
                novelty = 1.0 / ft_score_sum
            dnovf.write(b"%s,%.8f\n" % (doid.encode('utf-8'), novelty))
    print("  Wrote {} novelty scores to file {}".format(ct, DISEASE_NOVELTY_FILE))

    print("\nComputing importance scores")
    # To calculate importance scores, each paper is assigned a fractional
    # disease-target (FDT) score of one divided by the product of the
    # number of targets mentioned and the number of diseases
    # mentioned. The importance score for a given disease-target pair is
    # the sum of the FDT scores for all papers mentioning that disease and
    # protein.
    ct = 0
    with open(IMPORTANCE_FILE, 'wb') as impf:
        impf.write(b"DOID,Protein ID,UniProt,Score\n")
        for k,ppmids in pid2pmids.items():
            for doid,dpmids in doid2pmids.items():
                pd_pmids = ppmids.intersection(dpmids)
                fdt_score_sum = 0.0
                for pmid in pd_pmids:
                    fdt_score_sum += 1.0 / ( pmid_protein_ct[pmid] * pmid_disease_ct[pmid] )
                if fdt_score_sum > 0:
                    ct += 1
                    impf.write(b"%s,%s,%.8f\n" % (doid.encode('utf-8'), k.encode('utf-8'), fdt_score_sum))
    print("  Wrote {} importance scores to file {}".format(ct, IMPORTANCE_FILE))

    print("\nComputing PubMed rankings")
    # PMIDs are ranked for a given disease-target pair based on a score
    # calculated by multiplying the number of targets mentioned and the
    # number of diseases mentioned in that paper. Lower scores have a lower
    # rank (higher priority). If the scores do not discriminate, PMIDs are
    # reverse sorted by value with the assumption that larger PMIDs are
    # newer and of higher priority.
    ct = 0
    with open(PMID_RANKING_FILE, 'wb') as pmrf:
        pmrf.write(b"DOID,Protein ID,UniProt,PubMed ID,Rank\n")
        for k,ppmids in pid2pmids.items():
            for doid,dpmids in doid2pmids.items():
                #print(ppmids,dpmids)
                pd_pmids = ppmids.intersection(dpmids)
                #print(pd_pmids)
                scores = [] # scores are tuples of (PMID, protein_mentions*disease_mentions)
                for pmid in pd_pmids:
                    scores.append( (pmid, pmid_protein_ct[pmid] * pmid_disease_ct[pmid]) )
                #print(scores)
                if len(scores) > 0:
                    #print(scores)
                    scores = sorted(scores, key=lambda x: (x[1], -x[0]))
                    # #scores.sort(cmp_pmids_scores)
                    # print(scores)
                    for i,t in enumerate(scores):
                        ct += 1
                        pmrf.write(b"%s,%s,%d,%d\n" % (doid.encode('utf-8'), k.encode('utf-8'), t[0], i))
    print("  Wrote {} PubMed rankings to file {}".format(ct, PMID_RANKING_FILE))

def cmp_pmids_scores(a, b):
  '''
  a and b are tuples: (PMID, score)
  This sorts first by score ascending and then by PMID descending.
  '''
  if a[1] > b[1]:
    return 1
  elif a[1] < b[1]:
    return -1
  elif a[0] > b[0]:
    return -1
  elif a[1] < b[0]:
    return 1
  else:
    return 0

    


















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

    load(args,dba)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")
  
