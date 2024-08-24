#!/usr/bin/env python

"""Load NCBI annotations for TCRD from NCBI api.

Usage:
    load-NCBIGene.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-NCBIGene.py -? | --help

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
import shelve
import requests
from bs4 import BeautifulSoup

__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"
EFETCH_GENE_URL='http://www.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=Gene&rettype=xml&id='
SHELF_FILE = '%s/load-NCBIGene.db'%LOGDIR



def load(args , dba, dataset_id, logger, logfile):
    s = shelve.open(SHELF_FILE,writeback=True)
    s['loaded']=[]
    s['retries']={}
    s['counts']=defaultdict(int)

    ct = 0
    skip_ct = 0
    past_id = None
    pct =len(dba.get_protein_ids())
    logger.info(f'loading NCBI Gene annotation for {pct} TCRD proteins')
    avails_proteins = dba.ncbigene_avail_proteins()
    for t in dba.get_proteins():
        
        ct +=1
        slmf.update_progress(ct/pct)
        #print(t)
        pid = t['id']
        # we skip the t if the data is already inserted into database
        if pid in avails_proteins:
            continue
        if t['geneid']==None:
            skip_ct +=1
            continue
        
        
        geneid =str(t['geneid'])
        logger.info(f"precessing protein with protein is {pid} and geneid {geneid}")
        (status,headers,xml)=get_ncbigene(geneid)
        if not status:
            logger.warn(f"Failed getting gene ID:{geneid}")
            s['retries'][pid]=True
            continue
        if status !=200:
            logger.warn(f"Bad APT response for Gene ID {geneid}: status:{status}")
            s['retries'][pid]=True
        gene_annotations =parse_genexml(xml)
        if not gene_annotations:
            s['counts']['xml_err'] +=1
            logger.error(f"XML error for geneid {geneid}")
            s['retries'][pid]=True
            continue
        load_annotations(dba,t,dataset_id,gene_annotations,s)
        time.sleep(0.5)
        
    print(f"processed {ct} proteins")
    if skip_ct>0:
        print(f"Skipped {skip_ct}  proteins with no geneid")
    print(f"loaded NCBI annotations for {len(s['loaded'])}")
    if len(s['retries'])>0:
        print(f"Total targets remaining for retries {len(s['retries'])}")

    loop =1
    while len(s['retries'])>0:
        print(f"Retry loop {loop} loading NCBI gene annotations for {len(s['retries'])} MKDEV proteins")
        logger.info(f"Retry loop {loop} loading NCBI gene annotations for {len(s['retries'])} MKDEV proteins")
        ct = 0
        act = 0
        for pid,_ in s['retries'].items():
            slmf.update_progress(ct/len(s['retries']))
            ct +=1
            p=dba.get_protein(pid)
            geneid=p['geneid']
            logger.info(f"processing protein {pid}: geneid {geneid}")
            (status,headers,xml)=get_ncbigene(geneid)
            if not status:
                logger.warn(f"Failed getting gene id {geneid}")
                continue
            if status !=200:
                logger.warn(f"Bad API response for gene ID {geneid} ")
                continue
            gene_annotations=parse_genexml(xml)
            if not gene_annotations:
                s['counts']['xml_err'] +=1
                logger.error(f'XML Error for gene ID f{geneid}')
                continue
            load_annotations(dba,p,dataset_id,gene_annotations,s)
            act +=1
            del s['retries'][pid]
            time.sleep(0.5)
        loop +=1
        if loop ==5:
            print(f"Completed 5 retry loops.Aborting")
            break
        print(f"prcessed {ct} proteins")
        print(f"Total annoted proteins {act}")
        print(f"total annotated protein {len(s['loaded'])}")
        if len(s['retries'])>0:
            print(f"Total proteins remaning for retries {len(s['retries'])}")
    print(f"inserted {s['counts']['alias']} alias")
    print(f"inserted {s['counts']['summary']} NCBI Gene Summary tdl_infos")
    print(f"inserted {s['counts']['summary']} NCBI Gene PubMed Count tdl_infos")
    print(f"Inserted {s['counts']['generif']} GeneRIFs")
    print(f"Inserted {s['counts']['pmxr']} PubMed xrefs")

    if s['counts']['xml_err']>0:
        print(f"WARNING:{s['counts']['xml_err']} XML parsing errors occured. See logfile {logfile}")
    if s['counts']['dba_err']>0:
        print(f"WARNING:{s['counts']['dba_err']}. See logfile {logfile} for details.")

def get_ncbigene(id):
    url = EFETCH_GENE_URL+str(id)+".xml"
    r =None
    attempts = 0
    while attempts <=5:
        try:
            r = requests.get(url)
            break
        except:
            attempts +=1
            time.sleep(2)
    if r:
        return (r.status_code,r.headers,r.text)
    else:
        return (False,False,False)
    
def parse_genexml(xml):
    annotations = {}
    soup=BeautifulSoup(xml,"xml")
    if not soup:
        return False
    try:
        g=soup.find('Entrezgene')
    except:
        return False
    if not g:
        return False
    comments = g.find('Entrezgene_comments')
    # Aliases
    annotations['aliases']=[]
    if g.find('Gene-ref_syn'):
        for grse in g.find('Gene-ref_syn').findAll('Gene-ref_syn_E'):
            annotations['aliases'].append(grse.text)
    
    # Gene Summary
    if g.find('Entrezgene_summary'):
        annotations['summary'] = g.find('Entrezgene_summary').text

    # PubMed IDs
    annotations['pmids']=[]
    gcrefs = comments.find('Gene-commentary_refs')
    if gcrefs:
        annotations['pmids']=set([pmid.text for pmid in gcrefs.findAll('PubMedId')])
    
    # generifs
    annotations['generifs'] = []
    for gc in comments.findAll('Gene-commentary', recursive=False):
        if gc.findChild('Gene-commentary_heading') and gc.find('Gene-commentary_heading').text == 'Interactions':
            continue
        gctype = gc.findChild('Gene-commentary_type')
        if gctype.attrs['value'] == 'generif':
            gctext = gc.find('Gene-commentary_text')
            if gctext:
                annotations['generifs'].append( {'pubmed_ids': "|".join([pmid.text for pmid in gc.findAll('PubMedId')]), 'text': gctext.text} )

    return annotations

def load_annotations(dba,p,dataset_id,gene_annotations,shelf):
    pid = p['id']
    for a in gene_annotations['aliases']:
        rv= dba.ins_alias({'protein_id': pid, 'type': 'symbol', 'dataset_id': dataset_id, 'value': a})
        if rv:
            shelf['counts']['alias'] +=1
        else:
            shelf['counts']['dba_err'] +=1

    if 'summary' in gene_annotations:
        rv = dba.ins_tdl_info({'protein_id': pid, 'itype': 'NCBI Gene Summary',
                           'string_value': gene_annotations['summary']})
        if rv:
            shelf['counts']['summary'] += 1
        else:
            shelf['counts']['dba_err'] += 1

    if 'pmids' in gene_annotations:
        pmct=len(gene_annotations['pmids'])
    else:
        pmct=0
    rv = dba.ins_tdl_info({'protein_id': pid, 'itype': 'NCBI Gene PubMed Count',
                         'integer_value': pmct})  
    if rv:
        shelf['counts']['pmc'] += 1
    else:
        shelf['counts']['dba_err'] += 1 

    for pmid in gene_annotations['pmids']:
        rv = dba.ins_xref({'protein_id': pid, 'xtype': 'PubMed', 'dataset_id': dataset_id, 'value': pmid})
        if rv:
            shelf['counts']['pmxr'] += 1
        else:
            shelf['counts']['dba_err'] += 1
    if 'generifs' in gene_annotations:
        for grd in gene_annotations['generifs']:
            grd['protein_id'] = pid
            rv = dba.ins_generif(grd)
            if rv:
                shelf['counts']['generif'] += 1
            else:
                shelf['counts']['dba_err'] += 1

    shelf['loaded'].append(pid)
    





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

    # dataset_id = dba.ins_dataset( {'name': 'NCBI Gene', 'source': "EUtils web API at http://www.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=Gene&rettype=xml&id=", 'app': PROGRAM, 'app_version': __version__, 'url': 'http://www.ncbi.nlm.nih.gov/gene'} )
    # assert dataset_id ,f"Error inserting data, for more infor see logfile:{logfile}"
    # provs = [ {'dataset_id': dataset_id, 'table_name': 'tdl_info','where_clause':"itype = 'NCBI Gene Summary'"},
    #         {'dataset_id': dataset_id, 'table_name': 'tdl_info','where_clause':"itype = 'NCBI Gene PubMed Count'"},
    #         {'dataset_id': dataset_id, 'table_name': 'generif'},
    #         {'dataset_id': dataset_id, 'table_name': 'xref', 'where_clause': f"dataset_id ={dataset_id}"},
    #          {'dataset_id': dataset_id, 'table_name': 'alias', 'where_clause': f"dataset_id ={dataset_id}"} ]
    # for prov in provs:
    #     rv=dba.ins_provenance(prov)
    #     assert rv, f"Error inserting the data into prov for {prov}"
    dataset_id=18
    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")







