MAXLINES = 100000000
MKDEV_VER='8'
PMID_RANKING_FILE = '../data/TIN-X/TCRDv%s/PMIDRanking.csv'%MKDEV_VER

csvfile = open(PMID_RANKING_FILE, mode='r', encoding='utf-8')
filename = 0
for rownum, line in enumerate(csvfile):
    if rownum % MAXLINES == 0:
        filename += 1
        outfile = open('../data/TIN-X/TCRDv8/'+str(filename) + '.csv', mode='w', encoding='utf-8')
    outfile.write(line)
outfile.close()
csvfile.close()