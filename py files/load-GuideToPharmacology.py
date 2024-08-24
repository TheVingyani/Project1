#!/usr/bin/env python

"""Load HGNC annotations for TCRD targets from downloaded TSV file.

Usage:
    load-GuideToPharmacology.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-GuideToPharmacology.py -? | --help

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
from collections import defaultdict

__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"
DOWNLOAD_DIR = '../data/GuideToPharmacology/'
L_FILE = 'ligands.csv'
I_FILE = 'interactions.csv'

def load(args , dba, dataset_id, logger, logfile):
    fn=DOWNLOAD_DIR+L_FILE
    #line_ct = slmf.wcl(fn)
    line_ct=12164
    ligands={}
    skip_ct = 0 
    with open(fn,'r', encoding='utf-8') as lfh:
        csvreader = csv.reader(lfh)
        csvreader.__next__()
        ct =1
        for row in csvreader:
            # 0 Ligand ID
            # 1 Name	
            # 2 Species	
            # 3 Type	
            # 4 Approved	
            # 5 Withdrawn	
            # 6 Labelled	
            # 7 Radioactive	
            # 8 PubChem SID	
            # 9 PubChem CID	
            # 10 UniProt ID	
            # 11 Ensembl ID	
            # 12 Ligand Subunit IDs	
            # 13 Ligand Subunit Name	
            # 14 Ligand Subunit UniProt IDs	
            # 15 Ligand Subunit Ensembl IDs	
            # 16 IUPAC name	
            # 17 INN	
            # 18 Synonyms	
            # 19 SMILES	
            # 20 InChIKey	
            # 21 InChI	
            # 22 GtoImmuPdb	
            # 23 GtoMPdb	
            # 24 Antibacterial
            ct +=1
            slmf.update_progress(ct/line_ct)
            #print(row)
            ligand_id = int(row[0])
            ligand_type = row[3]
            if ligand_type=='Antibody' or ligand_type=='Peptide':
                skip_ct +=1
                continue
            ligands[ligand_id]={'name':row[1],'pubchem_cid':row[9],'smiles':row[19]}
            #print(ligands[ligand_id])

    # this dict will map uniprot|sym from interactions file to TCRD target(s)
    # so we only have to find target(s) once for each pair.
    k2ts=defaultdict(list)
    fn =DOWNLOAD_DIR+I_FILE
    #line_ct = slmf.wcl(fn)
    line_ct = 22431
    with open(fn,'r',encoding='utf-8') as ifh:
        csvreader=csv.reader(ifh)
        csvreader.__next__()
        ct =1
        tmark = {}
        ca_ct = 0
        ap_ct = 0
        md_ct = 0
        ba_ct = 0
        notfnd = set()
        dba_err_ct =0
        for row in csvreader:
            # 0 Target	
            # 1 Target ID	
            # 2 Target Subunit IDs	
            # 3 Target Gene Symbol	
            # 4 Target UniProt ID	
            # 5 Target Ensembl Gene ID	
            # 6 Target Ligand	
            # 7 Target Ligand ID	
            # 8 Target Ligand Subunit IDs	
            # 9 Target Ligand Gene Symbol	
            # 10 Target Ligand UniProt ID	
            # 11 Target Ligand Ensembl Gene ID	
            # 12 Target Ligand PubChem SID	
            # 13 Target Species	
            # 14 Ligand ID	
            # 15 Ligand	
            # 16 Ligand Type	
            # 17 Ligand Subunit IDs	
            # 18 Ligand Gene Symbol	
            # 19 Ligand Species	
            # 20 Ligand PubChem SID	
            # 21 Approved	
            # 22 Type	
            # 23 Action	
            # 24 Action comment	
            # 25 Selectivity	
            # 26 Endogenous	
            # 27 Primary Target	
            # 28 concentration Range	
            # 29 Affinity Units	
            # 30 Affinity High	
            # 31 Affinity Median	
            # 32 Affinity Low	
            # 33 Original Affinity Units	
            # 34 Original Affinity Low nm	
            # 35 Original Affinity Median nm	
            # 36 Original Affinity High nm	
            # 37 Original Affinity Relation	
            # 38 Assay Description	
            # 39 Receptor Site	
            # 40 Ligand Context	
            # 41 PubMed ID
            #print(row)
            ct +=1 
            slmf.update_progress(ct/line_ct)
            lid = int(row[14])
            if lid not in ligands:
                ap_ct +=1
                continue
            if row[31]=='':
                md_ct +=1
                continue
            
            if '|' in row[4]:
                skip_ct +=1
                continue
            val = "%.8f"%float(row[31])
            act_type = row[33]
            up = row[4]
            sym = row[3]
            k = "%s|%s"%(up,sym)
            if k == '|':
                md_ct += 1
                continue
            if k in k2ts:
                # already found target(s)
                ts = k2ts[k]
            elif k in notfnd:
                #already didn't find target
                continue
            else:
                #lookup proteins
                pids = dba.find_protein_ids({'uniprot':up})
                if not pids:
                    pids = dba.find_protein_ids({'sym':sym})
                    if not pids:
                        notfnd.add(k)
                        logger.warn(f"No target found for {k}")
                        continue
                ts =[]
                for pid in pids:
                    targets = dba.get_target(pid)
                    #print(targets)
                    ts.append({'id':pid,'fam':targets['fam']})
                    k2ts[k]=ts
            if row[41] and row[41] !='':
                pmids =row[41]
            else :
                pmids=None
            if ligands[lid]['pubchem_cid']=='':
                pccid=None
            else:
                pccid=ligands[lid]['pubchem_cid']
            for t in ts:
                if t['fam']=='GPCR':
                    cutoff=7.0 #110nm
                if t['fam'] == 'GPCR':
                    cutoff =  7.0 # 100nM
                elif t['fam'] == 'IC':
                    cutoff = 5.0 # 10uM
                elif t['fam'] == 'Kinase':
                    cutoff = 7.52288 # 30nM
                elif t['fam'] == 'NR':
                    cutoff =  7.0 # 100nM
                else:
                    cutoff = 6.0 # 1uM for non-IDG Family targets
                #print(val)
                if float(val) >= cutoff:
                    # target is Tchem, save activity
                    tmark[t['id']] = True
                    rv = dba.ins_cmpd_activity( {'target_id': t['id'], 'catype': 'Guide to Pharmacology',
                                       'cmpd_id_in_src': lid,
                                       'cmpd_name_in_src': ligands[lid]['name'],
                                       'smiles': ligands[lid]['smiles'], 'act_value': val,
                                       'act_type': act_type, 'pubmed_ids': pmids,
                                       'cmpd_pubchem_cid': pccid} )    
                    if not rv:
                        dba_err_ct += 1
                        continue
                    ca_ct += 1
                else:
                    ba_ct += 1
            

    print("{} rows processed.".format(ct))
    print("  Inserted {} new cmpd_activity rows for {} targets".format(ca_ct, len(tmark)))
    print("  Skipped {} with below cutoff activity values".format(ba_ct))
    print("  Skipped {} activities with multiple targets".format(skip_ct))
    print("  Skipped {} antibody/peptide activities".format(ap_ct))
    print("  Skipped {} activities with missing data".format(md_ct))
    if notfnd:
        print("No target found for {} uniprots/symbols. See logfile {} for details.".format(len(notfnd), logfile))
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

    # dataset_id = dba.ins_dataset( {'name': 'Guide to Pharmacology', 'source': "Files ligands.csv, interactions.csv from http://www.guidetopharmacology.org/DATA/", 'app': PROGRAM, 'app_version': __version__, 'url': 'http://www.guidetopharmacology.org/'} )
    # assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'cmpd_activity','where_clause':'ctype = "Guide to Pharmacology"'}]
    # for prov in provs:
    #     rv=dba.ins_provenance(prov)
    #     assert rv, f"Error inserting the data into prov for {prov}"
    dataset_id=22
    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")







