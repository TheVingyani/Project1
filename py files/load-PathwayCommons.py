#!/usr/bin/env python

"""

Usage:
    load-PathwayCommons.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-PathwayCommons.py -? | --help

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
import re
import urllib


__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"

DOWNLOAD_DIR = '../data/PathwayCommons/'
BASE_URL = 'http://www.pathwaycommons.org/archives/PC2/v12/'
PATHWAYS_FILE = 'PathwayCommons12.All.uniprot.gmt.gz'
PCAPP_BASE_URL = 'http://apps.pathwaycommons.org/pathways?uri='


def load(args, dba, dataset_id, logger, logfile):
    infile = (DOWNLOAD_DIR + PATHWAYS_FILE).replace('.gz', '')
    line_ct = slmf.wcl(infile)
    with open(infile, 'r') as tsv:
        tsvreader = csv.reader(tsv, delimiter='\t')
        # Example line:
        # http://identifiers.org/kegg.pathway/hsa00010    name: Glycolysis / Gluconeogenesis; datasource: kegg; organism: 9606; idtype: uniprot  A8K7J7  B4DDQ8  B4DNK4  E9PCR7  P04406  P06744  P07205  P07738  P09467 P09622   P09972  P10515  P11177  P14550  P30838  P35557  P51648  P60174  Q01813  Q16822  Q53Y25  Q6FHV6 Q6IRT1   Q6ZMR3  Q8IUN7  Q96C23  Q9BRR6  Q9NQR9  Q9NR19
        # However, note that pathway commons URLs in file give 404.
        # E.g. URL from this line:
        # http://pathwaycommons.org/pc2/Pathway_0136871cbdf9a3ecc09529f1878171df  name: VEGFR1 specific signals; datasource: pid; organism: 9606; idtype: uniprot    O14786  O15530  O60462  P05771  P07900  P15692  P16333  P17252  P17612  P17948  P19174  P20936     P22681  P27361  P27986  P28482  P29474  P31749  P42336  P49763  P49765  P62158  P98077  Q03135  Q06124  Q16665  Q9Y5K6
        # needs to be converted to:
        # http://apps.pathwaycommons.org/pathways?uri=http%3A%2F%2Fpathwaycommons.org%2Fpc2%2FPathway_0136871cbdf9a3ecc09529f1878171df
        ct = 0
        skip_ct = 0
        up2pid = {}
        pmark = set()
        notfnd = set()
        pw_ct = 0
        dba_err_ct = 0
        for row in tsvreader:
            ct += 1
            slmf.update_progress(ct/line_ct)
            src = re.search(r'datasource: (\w+)', row[1]).groups()[0]
            if src in ['kegg', 'wikipathways', 'reactome']:
                skip_ct += 1
                continue
            pwtype = 'PathwayCommons: ' + src
            name = re.search(r'name: (.+?);', row[1]).groups()[0]
            url = PCAPP_BASE_URL + urllib.parse.quote(row[0], safe='')
            ups = row[2:]
            for up in ups:
                if up in up2pid:
                    pid = up2pid[up]
                elif up in notfnd:
                    continue
                else:
                    targets = dba.find_protein_ids({'uniprot': up})
                if not targets:
                    notfnd.add(up)
                    continue
                t = targets[0]
                pid = t
                up2pid[up] = pid
                rv = dba.ins_pathway({'protein_id': pid, 'pwtype': pwtype, 'name': name, 'url': url})
                if rv:
                    pw_ct += 1
                    pmark.add(pid)
                else:
                    dba_err_ct += 1
               
    

            
                    
            
      
  
  


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

    # dataset_id = dba.ins_dataset( {'name': 'Pathway Commons', 'source': 'File %s'%BASE_URL+PATHWAYS_FILE, 'app': PROGRAM, 'app_version': __version__, 'url': 'http://www.pathwaycommons.org/'} )
    # assert dataset_id, "Error inserting dataset See logfile {} for details.".format(logfile)
    # # Provenance
    # rv = dba.ins_provenance({'dataset_id': dataset_id, 'table_name': 'pathway', 'where_clause': "pwtype LIKE 'PathwayCommons %s'"})
    # assert rv, "Error inserting provenance. See logfile {} for details.".format(logfile)
    
    dataset_id = 79
    
    print(dataset_id)

    load(args , dba, dataset_id, logger, logfile)
    
    time_taken=time.time()-start_time
    print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    