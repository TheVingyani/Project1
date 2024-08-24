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
import urllib
import shelve
from bs4 import BeautifulSoup
import requests
import re
import calendar

__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"

EMAIL = 'manoj19@iiserb.ac.in'
EFETCHURL = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?&db=pubmed&retmode=xml&email=%s&tool=%s&id=" % (urllib.parse.quote(EMAIL), urllib.parse.quote(PROGRAM))
SHELF_FILE = "%s/load-PubMed.db" % LOGDIR

# map abbreviated month names to ints
months_rdict = {v: str(i) for i,v in enumerate(calendar.month_abbr)}
mld_regex = re.compile(r'(\d{4}) (\w{3}) (\d\d?)-')

def chunker(l, size):
  return (l[pos:pos + size] for pos in range(0, len(l), size))

def get_pubmed(pmids):
  #print(EFETCHURL)
  url = EFETCHURL + ','.join(pmids)
  #print(url)
  attempts = 0
  r = None
  while attempts <= 5:
    try:
      r = requests.get(url)
      break
    except:
      attempts += 1
      time.sleep(1)
  if r:
    return r
  else:
    return False
  
def pubdate2isostr(pubdate):
  """Turn a PubDate XML element into an ISO-type string (ie. YYYY-MM-DD)."""
  if pubdate.find('MedlineDate'):
    mld = pubdate.find('MedlineDate').text
    m = mld_regex.search(mld)
    if not m:
      return None
    month = months_rdict.get(m.groups(1), None)
    if not month:
      return m.groups()[0]
    return "%s-%s-%s" % (m.groups()[0], month, m.groups()[2])
  else:
    year = pubdate.find('Year').text
    if not pubdate.find('Month'):
      return year
    month = pubdate.find('Month').text
    if not month.isdigit():
      month = months_rdict.get(month, None)
      if not month:
        return year
    if pubdate.find('Day'):
      day = pubdate.find('Day').text
      return "%s-%s-%s" % (year, month.zfill(2), day.zfill(2))
    else:
      return "%s-%s" % (year, month.zfill(2))
  
def parse_pubmed_article(pma):
  """
  Parse a BeautifulSoup PubmedArticle into a dict suitable to use as an argument
  to TCRC.DBAdaptor.ins_pubmed().
  """
  pmid = pma.find('PMID').text
  article = pma.find('Article')
  title = article.find('ArticleTitle').text
  init = {'id': pmid, 'title': title }
  journal = article.find('Journal')
  pd = journal.find('PubDate')
  if pd:
    init['date'] = pubdate2isostr(pd)
  jt = journal.find('Title')
  if jt:
    init['journal'] = jt.text
  authors = pma.findAll('Author')
  if len(authors) > 0:
    if len(authors) > 5:
      # For papers with more than five authors, the authors field will be
      # formated as: "Mathias SL and 42 more authors."
      a = authors[0]
      # if the first author has no last name, we skip populating the authors field
      if a.find('LastName'):
        astr = "%s" % a.find('LastName').text
        if a.find('ForeName'):
          astr += ", %s" % a.find('ForeName').text
        if a.find('Initials'):
          astr += " %s" % a.find('Initials').text
        init['authors'] = "%s and %d more authors." % (astr, len(authors)-1)
    else:
      # For papers with five or fewer authors, the authors field will have all their names
      auth_strings = []
      last_auth = authors.pop()
      # if the last author has no last name, we skip populating the authors field
      if last_auth.find('LastName'):
        last_auth_str = "%s" % last_auth.find('LastName').text
        if last_auth.find('ForeName'):
          last_auth_str += ", %s" % last_auth.find('ForeName').text
        if last_auth.find('Initials'):
          last_auth_str += " %s" % last_auth.find('Initials').text
        for a in authors:
          if a.find('LastName'): # if authors have no last name, we skip them
            astr = "%s" % a.find('LastName').text
            if a.find('ForeName'):
              astr += ", %s" % a.find('ForeName').text
            if a.find('Initials'):
              astr += " %s" % a.find('Initials').text
            auth_strings.append(astr)
        init['authors'] = "%s and %s." % (", ".join(auth_strings), last_auth_str)
  abstract = article.find('AbstractText')
  if abstract:
    init['abstract'] = abstract.text
  return init

def load(args, dba, dataset_id, logger, logfile):
    # s = shelve.open(SHELF_FILE, writeback=True)
    # s['loaded'] = [] # list of target IDs that have been successfully processed
    # s['pmids'] = [] # list of stored pubmed ids
    # s['p2p_ct'] = 0
    # s['errors'] = defaultdict(list)

        
    # tct = dba.get_protein_counts()['total']
    # ct = 0
    # dba_err_ct = 0
    # past_id = 0
    # already_process_proteins = dba.get_protein2pubmed()
    # already_process_pubmend = dba.get_pmids()
    # counts  = dba.get_proteinpubmed_count()
    # for target in dba.get_protein_ids():
    #     #print("target",target)
    #     ct += 1
    #     slmf.update_progress(ct/tct)
        
    #     p = target
    #     # #print(already_process_proteins,target)
    #     # if p in already_process_proteins:
    #     #    continue

    #     xrefs = dba.get_pubmed_xref(p)
        
    #     pmids = [d for d in xrefs]
    #     if not pmids:
    #        continue
    #     count = -1
    #     if p in counts:
    #       count = counts[p]
    #       #print(p,len(pmids),count)
        
        
        

    #     if count >= len(pmids):
    #        continue
           
        
    #     #print("pmids",pmids)
    #     chunk_ct = 0
    #     err_ct = 0
    #     for chunk in chunker(pmids, 200):
    #         chunk_ct += 1
    #         #print("chunk",chunk)
    #         r = get_pubmed(chunk)
    #         if not r or r.status_code != 200:
    #             # try again...
    #             r = get_pubmed(chunk)
    #             if not r or r.status_code != 200:
    #                 logger.error("Bad E-Utils response for target {}, chunk {}".format(target, chunk_ct))
    #                 s['errors'][target].append(chunk_ct)
    #                 err_ct += 1
    #                 continue
    #         soup = BeautifulSoup(r.text, "xml")
    #         pmas = soup.find('PubmedArticleSet')
    #         for pma in pmas.findAll('PubmedArticle'):
    #             pmid = pma.find('PMID').text
    #             if int(pmid) not in already_process_pubmend: # only store each pubmed once
    #                 logger.debug("  parsing XML for PMID: %s" % pmid)
    #                 init = parse_pubmed_article(pma)
    #                 rv = dba.ins_pubmed(init)
    #                 if not rv:
    #                     dba_err_ct += 1
    #                     continue
    #                 already_process_pubmend.append(int(pmid))
    #             s['pmids'].append(pmid) # add pubmed id to list of saved ones
    #             rv = dba.ins_protein2pubmed({'protein_id': p, 'pubmed_id': pmid})
    #             if not rv:
    #                 dba_err_ct += 1
    #                 continue
    #             s['p2p_ct'] += 1
    #         time.sleep(0.5)
    #         if err_ct == 0:
    #             s['loaded'].append(target)

    
    # loop = 1
    # while len(s['errors']) > 0:
    #     print("\nRetry loop {}: Trying to load PubMeds for {} proteins".format(loop, len(s['errors'])))
    #     logger.info("Retry loop {}: Trying to load data for {} proteins".format(loop, len(s['errors'])))
    #     ct = 0
    #     dba_err_ct = 0
    #     for tid,chunk_cts in s['errors']:
    #         ct += 1
    #         target = dba.find_protein_ids(tid)
    #         logger.info("Processing target {}".format(target))
            
    #         p = target[0]
    #         xrefs = dba.get_pubmed_xref(p)
        
    #         pmids = [d for d in xrefs]
            
    #         chunk_ct = 0
    #         err_ct = 0
    #         for chunk in chunker(pmids, 200):
    #             chunk_ct += 1
    #             # only process chunks that are in the errors lists
    #             if chunk_ct not in chunk_cts:
    #                 continue
    #             r = get_pubmed(chunk)
    #             if not r or r.status_code != 200:
    #                 # try again...
    #                 r = get_pubmed(chunk)
    #                 if not r or r.status_code != 200:
    #                     logger.error("Bad E-Utils response for target {}, chunk {}".format(target['id'], chunk_ct))
    #                     err_ct += 1
    #                     continue
    #             soup = BeautifulSoup(r.text, "xml")
    #             pmas = soup.find('PubmedArticleSet')
    #             for pma in pmas.findAll('PubmedArticle'):
    #                 pmid = pma.find('PMID').text
    #                 if pmid not in s['pmids']:
    #                     # only store each pubmed once
    #                     logger.debug("  parsing XML for PMID: %s" % pmid)
    #                     init = parse_pubmed_article(pma)
    #                     rv = dba.ins_pubmed(init)
    #                     if not rv:
    #                         dba_err_ct += 1
    #                         continue
    #                     s['pmids'].append(pmid) # add pubmed id to list of saved ones
    #                 rv = dba.ins_protein2pubmed({'protein_id': p, 'pubmed_id': pmid})
    #                 if not rv:
    #                     dba_err_ct += 1
    #                     continue
    #                 s['p2p_ct'] += 1
    #             # remove chunk number from this target's error list
    #             s['errors'][tid].remove(chunk_ct)
    #             # it this target has no more errors, delete it from errors
    #             if len(s['errors'][tid]) == 0:
    #                 del(s['errors'][tid])
    #             time.sleep(0.5)
    #         if err_ct == 0:
    #             s['loaded'].append(target)


    # Find the set of TIN-X PubMed IDs not already stored in TCRD
    tinx_pmids = [str(pmid) for pmid in dba.get_tinx_pmids()]
    tinx_pmid_ct = len(tinx_pmids)
    pmids =  [str(pmid) for pmid in dba.get_pmids()]
    not_in_tcrd = list(set(tinx_pmids) - set(pmids))
    # print(tinx_pmids)
    # print(not_in_tcrd)
    # for pmid in tinx_pmids:
    #   rv = dba.get_pubmed(pmid)
    #   if not rv:
    #     not_in_tcrd.add(pmid)
    not_in_tcrd_ct = len(not_in_tcrd)
    ct = 0
    pm_ct = 0
    net_err_ct = 0
    dba_err_ct = 0
    chunk_ct = 0
    for chunk in chunker(list(not_in_tcrd), 200):
        chunk_ct += 1
        logger.info("Processing TIN-X PubMed IDs chunk {}".format(chunk_ct))
        r = get_pubmed(chunk)
        if not r or r.status_code != 200:
            # try again...
            r = get_pubmed(chunk)
            if not r or r.status_code != 200:
                logger.error("Bad E-Utils response for chunk {}".format(chunk_ct))
                net_err_ct += 1
                continue
        soup = BeautifulSoup(r.text, "xml")
        pmas = soup.find('PubmedArticleSet')
        for pma in pmas.findAll('PubmedArticle'):
            ct += 1
            slmf.update_progress(ct/not_in_tcrd_ct)
            logger.debug("  parsing XML for PMID: {}".format(pma))
            init = parse_pubmed_article(pma)
            rv = dba.ins_pubmed(init)
            if not rv:
                dba_err_ct += 1
                continue
            pm_ct += 1
        time.sleep(0.5)   
    

            
                    
            
      
  
  


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
    # dataset_id = dba.ins_dataset( {'name': 'PubMed', 'source': 'NCBI E-Utils', 'app': PROGRAM, 'app_version': __version__, 'url': 'https://www.ncbi.nlm.nih.gov/pubmed'} )
    # assert dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # # Provenance
    # rv = dba.ins_provenance({'dataset_id': dataset_id, 'table_name': 'pubmed'})
    # if not rv:
    #     print("WARNING: Error inserting provenance. See logfile %s for details." % logfile)
    #     sys.exit(1)
    # rv = dba.ins_provenance({'dataset_id': dataset_id, 'table_name': 'protein2pubmed'})
    # assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)
    
    dataset_id = 105
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    