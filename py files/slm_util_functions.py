import os,sys,platform,time,re,gzip
from functools import reduce
from itertools import islice

def update_progress(progress):
    barLength=50
    status=""
    if isinstance(progress,int):
        progress=float(progress)
    if not isinstance(progress,float):
        progress=0
        status="Error:input must be float"
    if progress<0:
        progress=0
        status="Aborted"
    if progress>=1:
        progress=1
        status="done"
    prog=int(round(barLength*progress))
    pbar = "\rProgress: [{0}] {1:.1f}% {2}".format("#"*prog + "-"*(barLength-prog), progress*100, status)
    sys.stdout.write(pbar)
    sys.stdout.flush()

def secs2str(t):
  return "%d:%02d:%02d.%03d" % reduce(lambda ll,b : divmod(ll[0],b) + ll[1:], [(t*1000,),1000,60,60])
def wcl(fname):

    with open(fname,encoding='utf-8') as f:
        for i,L in enumerate(f):
            pass
    return i+1