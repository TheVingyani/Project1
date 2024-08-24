#!/usr/bin/env python

"""
Usage:
    buildingkg.py [--dbhost=<str>] [--dbname=<str>] [--pwfile=<str>] [--dbuser=<str>] [--mondo=<str>]
    buildingkg.py -? | --help

Options:
  -m --mondoid MONDO   : mondo id
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
import pandas as pd
from collections import defaultdict
from neo4j import GraphDatabase
import networkx as nx

driver = GraphDatabase.driver('neo4j://68.183.83.1:7687', auth=("neo4j","neoiiserb"))

__author__='Manoj Kumar'
__email__='mkmahan2k@gmail.com'
__version__='8.0.0'

MKDEV_VER='8'

PROGRAM=os.path.basename(sys.argv[0])
LOGDIR=f"../log/mkdev{MKDEV_VER}logs/"
LOGFILE=f"{LOGDIR}{PROGRAM}.log"






def load(args, dba,mondoid, logger):
    savedir = ''
    mondos = dba.get_monodo(mondoid)[0]
    if not mondos:
        print(f"No disease found for given mondoid:{mondoid}")
        sys.exit(1)
    disease_name = mondos['name']
    disease_name = disease_name.replace(' ','_')
    if not os.path.exists(savedir+disease_name):
        os.makedirs(savedir+disease_name)
    savedir = savedir+disease_name+'/'
    
    disease_dataset  =dba.get_protein_ids_from_monodo(mondoid)
    disease_df = pd.DataFrame(disease_dataset)
    disease_df.to_csv(savedir+'disease_protein.csv',index=False)

    

    index = [0] 
    mondo_df = pd.DataFrame(mondos,index=index)
    mondo = mondo_df.to_csv(savedir+'mondo.csv',index=False)
    
    protein_ids = [x['protein_id'] for x in disease_dataset]
    #print(protein_ids)

    disease_uniprotes = dba.get_disease_uniprot(mondoid)
    #print(disease_uniprotes[1])
    disease_uniprot_df = pd.DataFrame(disease_uniprotes)
    disease_uniprot_df.to_csv(savedir+"protein_uniprot.csv",index=False)

    disease_protiens =dba.get_disease_protein(mondoid)
    disease_proteins_df =pd.DataFrame(disease_protiens)
    disease_proteins_df.to_csv(savedir+"disease_protein1.csv",index=False)

    print("\nExractinng protein pathway relations")
    protein_pathway = dba.get_protein_pathway(mondoid)
    protein_pathway_df = pd.DataFrame(protein_pathway)
    protein_pathway_df.to_csv(savedir + "protein_pathway.csv", index=False)

    

    if protein_ids:
        print("\nExractinng protein phenotype relations")
        phenotype_protein = dba.get_phenotype(protein_ids)
        phenotype_protein_df = pd.DataFrame(phenotype_protein)
        phenotype_protein_df.to_csv(savedir + "phenotype_protein.csv", index=False)

    print("\nExractinng protein metabolite relations")

    metabolite_protein = dba.get_metabolite(mondoid)
    metabolite_protein_df = pd.DataFrame(metabolite_protein)
    metabolite_protein_df.to_csv(savedir + "diease_protein_metabolite.csv", index=False)

    print("\nExractinng protein go relations")

    go_protein = dba.get_go(mondoid)
    go_protein_df = pd.DataFrame(go_protein)
    go_protein_df.to_csv(savedir + "go_protein.csv", index=False)

    print("\nExractinng protein gtex relations")

    gtex_protein = dba.get_gtex_protein(mondoid)
    gtex_protein_df = pd.DataFrame(gtex_protein)
    gtex_protein_df.to_csv(savedir + "gtex_protein.csv", index=False)

    print("\nExractinng ppi relations")
    if protein_ids:
        ppi_protein = dba.get_ppi_protein(protein_ids)

        filtered_ppi = []
        found_ids = set()  # Use a set for faster membership checking

        for interaction in ppi_protein:
            protein_id = interaction['protein1_id']
            related_protein_id = interaction['protein2_id']
            interaction_ids = (protein_id, related_protein_id)
            reverse_interaction_ids = (related_protein_id, protein_id)

            if interaction_ids not in found_ids and reverse_interaction_ids not in found_ids:
                filtered_ppi.append(interaction)
                found_ids.add(interaction_ids)

    ppi_protein_df = pd.DataFrame(filtered_ppi)
    ppi_protein_df.to_csv(savedir + "ppi_protein.csv", index=False)

    print("\nExractinng  drug protien relations")

    mondo_parent = dba.get_mondo_parent(mondoid)
    if len(mondo_parent)>0:
        mondo_parent.append(mondoid)
    else:
        mondo_parent=[mondoid]


    #print(mondo_parent)
    if mondo_parent:
        drug_related_to_disease = dba.get_drug(mondo_parent)
        drug_related_to_disease_df = pd.DataFrame(drug_related_to_disease)
        drug_related_to_disease_df.to_csv(savedir + 'drug_protein.csv',index=False)

        related_drug_names = [x['drug'] for x in drug_related_to_disease]
        #print(related_drug_names)
        print("\nExractinng related drug relations")
        drug_drug_interactions = dba.get_drug_drug(related_drug_names)

                
        filtered_drug_drug = []
        found_ids = set()  # Use a set for faster membership checking

        for interaction in drug_drug_interactions:
            drugbank_id = interaction['drugbank_id']
            related_drugbank_id = interaction['related_drug_drugbank_id']
            interaction_ids = (drugbank_id, related_drugbank_id)
            reverse_interaction_ids = (related_drugbank_id, drugbank_id)

            if interaction_ids not in found_ids and reverse_interaction_ids not in found_ids:
                filtered_drug_drug.append(interaction)
                found_ids.add(interaction_ids)

        #print(len(filterd_drug_drug))

        drug_drug_int = pd.DataFrame(filtered_drug_drug)

        related_drug = []
        for x in filtered_drug_drug:
            dc = {"related_drug_drugbank_id":x['related_drug_drugbank_id'],"related_drug_name":x['related_drug_name']}
            if dc not in related_drug and x['related_drug_name'] not in related_drug_names:
                related_drug.append(dc)
            
        drug_drug_int.to_csv(savedir+'drug_drug.csv',index=False)
        related_drug_df =pd.DataFrame(related_drug)
        related_drug_df.to_csv(savedir+'related_drug.csv',index=False)

    print("\nExractinng protein child disease relations")

    doids = dba.get_doid_from_mondo(mondoid)
    if doids:
        #print(doids)
        disease_child = dba.get_disease_child(doids)
        if disease_child:
            disease_child_protein = dba.get_disease_child_protein(disease_child)
            disease_child_protein_df = pd.DataFrame(disease_child_protein)
            disease_child_protein_df.to_csv(savedir+"child_disease_protein.csv",index=False)

    return disease_name


    
def load_neo4j(args , dba,d_name):
    query1 = """LOAD CSV WITH HEADERS FROM "file:///muscular_dystrophy/mondo.csv" AS row MERGE(:Disease{mondoid:row.mondoid, name:row.name , definition:row.def, doid:row.doid});"""
    results = driver.session().run(query1)

            





    


    


    





    

    

            
                    
            
      
  
  


if __name__ == '__main__':
    print("\n\n{} (v{}) [{}]:\n".format(PROGRAM, __version__, time.strftime("%c")))
    start_time = time.time()

    args = docopt(__doc__, version=__version__)
    dba_params={'dbname':args['--dbname'],'dbhost':args['--dbhost'],'pwfile':args['--pwfile'],'logger_name':__name__}

    dba=DBAdaptor(dba_params)
    dbi=dba.get_dbinfo()
    print(f"Connected to database {args['--dbname']} Schema_ver:{dbi['schema_ver']},data ver:{dbi['data_ver']}")

    mondoid = input("Enter mondo id:")

    #d_name=load(args , dba,mondoid, logger)
    d_name = 'muscular_dystrophy'
    load_neo4j(args , dba,d_name)

    
    time_taken=time.time()-start_time
    #print(f"\n{PROGRAM} done \n total time:{slmf.secs2str(time_taken)}")


    