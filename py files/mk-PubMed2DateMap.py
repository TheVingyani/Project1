#!/usr/bin/env python

"""

Usage:
    mk-PubMed2DateMap.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    mk-PubMed2DateMap.py -? | --help

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
import re
from bs4 import BeautifulSoup
import pickle
import requests
import calendar
import urllib

__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"

EMAIL = 'manoj19@iiserb.ac.in'
EFETCHURL = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?&db=pubmed&retmode=xml&email=%s&tool=%s&id=" % (urllib.parse.quote(EMAIL), urllib.parse.quote(PROGRAM))
PICKLE_FILE = '../data/TCRDv8_PubMed2Date.p'


def get_pubmed_article_date(pma):
  """
  Parse a BeautifulSoup PubmedArticle and return the publication date of the article.
  """
  article = pma.find('Article')
  journal = article.find('Journal')
  pd = journal.find('PubDate')
  if pd:
    year = pubdate2isostr(pd)
  if year:
    return year
  else:
    return None

# map abbreviated month names to ints
months_rdict = {v: str(i) for i,v in enumerate(calendar.month_abbr)}
mld_regex = re.compile(r'(\d{4}) (\w{3}) (\d\d?)-')

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


def get_pubmed(pmids):
  url = EFETCHURL + ','.join(pmids)
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

def chunker(l, size):
  return (l[pos:pos+size] for pos in range(0, len(l), size))
def load(args, dba, logger, logfile):
    generifs = dba.get_generifs()

    logger.info("Processing {} GeneRIFs".format(len(generifs)))
    yrre = re.compile(r'^(\d{4})')
    ct = 0
    yr_ct = 0
    skip_ct = 0
    net_err_ct = 0
    pubmed2date = {}
    missing_pmids = set()
    line_ct = len(generifs)
    for generif in generifs:
        #print(generif)
        ct += 1
        slmf.update_progress(ct/line_ct)
        for pmid in generif['pubmed_ids'].split("|"):
            if pmid in pubmed2date:
                continue
            # See if this PubMed is in TCRD...
            pm = dba.get_pubmed(pmid)
            if pm:
                # if so get date from there
                if pm['date']:
                    pubmed2date[pmid] = pm['date']
            else:
                # if not, will have to get it via EUtils
                missing_pmids.add(pmid)
    print(len(missing_pmids))
    logger.debug("Getting {} missing PubMeds from E-Utils".format(len(missing_pmids)))
    chunk_ct = 0
    err_ct = 0
    no_date_ct = 0
    pmids = list(missing_pmids)
    for chunk in chunker(pmids, 200):
        chunk_ct += 1
        #slmf.update_progress(ct/line_ct)
        logger.debug("Chunk {}: {}".format(chunk_ct, chunk))
        r = get_pubmed(chunk)
        if not r or r.status_code != 200:
            # try again...
            r = get_pubmed(pmid)
            if not r or r.status_code != 200:
                logger.error("Bad E-Utils response for PubMed ID {}".format(pmid))
                net_err_ct += 1
                continue
        soup = BeautifulSoup(r.text, "xml")
        pmas = soup.find('PubmedArticleSet')
        for pma in pmas.findAll('PubmedArticle'):
            pmid = pma.find('PMID').text
            date = get_pubmed_article_date(pma)
            if date:
                pubmed2date[pmid] = date
            else:
                no_date_ct += 1
        

    pickle.dump(pubmed2date, open(PICKLE_FILE, 'wb'))     
    

            
                    
            
      
  
  


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


    load(args , dba, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    