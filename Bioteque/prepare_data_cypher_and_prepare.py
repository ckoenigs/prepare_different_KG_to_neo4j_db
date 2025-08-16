
import csv
import os
import gzip
import tarfile
import requests

import urllib.request

from rdflib.tools.csv2rdf import csv_reader

cypher_node = open('output/cypher_node.cypher', 'w', encoding='utf-8')
cypher_edge = open('output/cypher_edge.cypher', 'w', encoding='utf-8')

dict_abb_to_label={
    'CHE':'Chemical',
    'CLL':'Cell',
    'CMP':'CellularComponent',
    'CPD':'Compound',
    'DIS':'Disease',
    'DOM':'Domain',
    'GEN':'Gene',
    'MFN':'MolecularFunction',
    'PHC':'PharmacologicalClass',
    'PWY':'Pathway',
    'TIS':'Tissue'
}


dict_abb_to_edges={
    'CHE': [['has','CPD','chebi']],
    'CLL':[['sns','CPD','prism'],['sns','CPD','nci60_sens'],['sns','CPD','gdsc1000_sens'],['sns','CPD','drugcell'],['sns','CPD','ctrpv2_sens'],['has','DIS','cl_disease_clueio'],['upr','GEN','ccle_rna'],['upr','GEN','gdsc1000_mrna'],['pdf','GEN','ccle_proteome'],['pab','GEN','ccle_proteome'],['mut','GEN','ccle_mut'],['mut','GEN','cosmic_census'],['mut','GEN','gdsc1000_cfe'],['mth','GEN','gdsc1000_cfe'],['dwr','GEN','gdsc1000_mrna'],['dwr','GEN','ccle_rna'],['cnu','GEN','gdsc1000_cfe'],['cnu','GEN','cclecnv_HMZ'],['cnd','GEN','cclecnv_HMZ'],['cnd','GEN','gdsc1000_cfe']],
    'CMP':[['has','GEN','jensencompartmentcurated']],
    'CPD':[['ddi','CPD','drugbank_CC'],['ups','GEN','pharmacodb_associations'],['upr','GEN','lincs_hetionet'],['int','GEN','repohub'],['int','GEN','pharmacogenomic_targets'],['int','GEN','drugcentral'],['int','GEN','drugbank_pk'],['int','GEN','drugbank_pd'],['int','GEN','drugbank'],['int','GEN','curated_targets'],['dws','GEN','pharmacodb_associations'],['dwr','GEN','lincs_hetionet'],['trt','DIS','repodb'],['trt','DIS','ctdchemdis'],['cau','DIS','offsides+sider'],['cau','DIS','sider'],['cau','DIS','offsides'],['cau','DIS','ctdchemdis']],
    'DIS':[['cau','DIS','disease_cau_symptom_hetionet'],['cau','DIS','disease_cau_symptom_hetionet'],['upr','GEN','creeds_disease_curated'],['dwr','GEN','creeds_disease_curated'],['ass','GEN','opentargets'],['ass','GEN','disgenet_curated+disgenet_inferred+disgenet_befree'],['ass','GEN','disgenet_curated+disgenet_inferred'],['ass','GEN','disgenet_curated'],['ass','GEN','ctddisease']],
    'DOM':[['has','GEN','interpro']],
    # ,['_reg','GEN','dorothea_AB'],['_reg','GEN','dorothea_AB+dorothea_CD'],['_pho','GEN','omnipath'],['_pho','GEN','kea_HMZ'],['_dwr','GEN','dorothea_AB+dorothea_CD'],['_dwr','GEN','dorothea_AB'],['_dph','GEN','omnipath']
    'GEN':[['upr_','GEN','dorothea_AB'],['upr_','GEN','dorothea_AB+dorothea_CD'],['upr_','GEN','dorothea_AB'],['_upr','GEN','dorothea_AB+dorothea_CD'],['reg_','GEN','dorothea_AB'],['reg_','GEN','dorothea_AB+dorothea_CD'],['ppi','GEN','string'],['ppi','GEN','omnipath'],['ppi','GEN','intact'],['ppi','GEN','hi_union'],['ppi','GEN','corum'],['pho_','GEN','omnipath'],['pho_','GEN','kea_HMZ'],['pgi','GEN','rauscher_2018'],['ngi','GEN','eytan_2018'],['ngi','GEN','rauscher_2018'],['dwr_','GEN','dorothea_AB+dorothea_CD'],['dwr_','GEN','dorothea_AB'],['dph_','GEN','omnipath'],['cex','GEN','coexpressdb'],['cdp','GEN','depmap_agreement_ceres'],['cdp','GEN','depmap_agreement_ccr']],
    'MFN':[['has','GEN','gomf_goa_curated']],
    'PHC':[['has','CPD','atc_drugs']],
    'PWY':[['ass','CLL','cosmic_mutsig'],['ass','GEN','reactome']],
    'TIS':[['has','DIS','disease_loc_tissue_hetionet'],['upr','GEN','hpa_rna_cons'],['upr','GEN','gtextissue_HMZ'],['pdf','GEN','hpa_proteome'],['pab','GEN','hpa_proteome'],['dwr','GEN','hpa_rna_cons'],['dwr','GEN','gtextissue_HMZ'],['ass','GEN','jensentissuecurated']]
}

def prepare_node_query(file_name,node_entity, identifier, properties_node):
    head_query =f"LOAD CSV WITH HEADERS FROM 'file:/mnt/aba90170-e6a0-4d07-929e-1200a6bfc6e1/databases/KGs/Bioteque/{file_name}' AS line FIELDTERMINATOR '\t' Call {{ with line %s }} IN TRANSACTIONS OF 10000 ROWS;\n"
    query_start= f"Create (n:{node_entity} {{"
    prop_list=[]
    for prop in properties_node:
        if prop==identifier:
            prop_list.append('id:line.`'+prop+'`')
        elif prop =='embedding_universe':
            continue
        else:
            prop_list.append(prop.replace(' ','').replace('-','_').replace('.','_dot_')+':line.`'+prop+'`')
    query= query_start+ ', '.join(prop_list)+'}) '
    head_query=head_query %(query)
    cypher_node.write(head_query)
    cypher_node.write(f"CREATE INDEX index{node_entity} FOR (node:{node_entity}) ON (node.id);\n")

request_headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) ' +
                  'Chrome/35.0.1916.47 Safari/537.36'
}

def download_and_unzip_nodes():
    for abbreviation, label in dict_abb_to_label.items():
        url = f'https://bioteque.irbbarcelona.org/downloads/node_universe/{abbreviation}.tsv.gz'
        print('Downloading', url)
        request_headers["Accept-Encoding"] = "gzip"
        file_name= f'data/{label}.tsv'
        if not os.path.isfile(file_name):
            request = urllib.request.Request(url, headers=request_headers)
            with urllib.request.urlopen(request) as response, open(file_name, 'wb') as f:
                test = gzip.GzipFile(fileobj=response)
                f.write(test.read())
        with open(file_name,'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f, delimiter='\t')
            header=next(csv_reader)
            prepare_node_query(file_name,label, 'code', header)





def prepare_edge_query(node_entity_1, node_entity_2, edge_type, file_name, id_name_1, id_name_2, source):
    split_id_1=id_name_1.split('_')
    if dict_abb_to_label[split_id_1[1]]== node_entity_1:
        id_1=id_name_1
        id_2=id_name_2
    else:
        print('change', node_entity_1, node_entity_2,id_name_1, id_name_2, edge_type,source)
        id_1=id_name_2
        id_2=id_name_1
    query = f"LOAD CSV WITH HEADERS FROM 'file:/mnt/aba90170-e6a0-4d07-929e-1200a6bfc6e1/databases/KGs/Bioteque/{file_name}' AS line FIELDTERMINATOR '\t' Call {{ with line Match (n:{node_entity_1} {{id:line.{id_1} }}), (m:{node_entity_2} {{id:line.{id_2} }}) Create (n)-[:{edge_type} {{source:'{source}', cosine_distance:line.cosine_distance }}]->(m) }} IN TRANSACTIONS OF 10000 ROWS;\n"
    cypher_edge.write(query)

def download_and_unzip_edges():
    for abbreviation, list_of_rela_type_other_label_datasource in dict_abb_to_edges.items():
        for edge_rela_type_label_data_source in list_of_rela_type_other_label_datasource:
            url = f'https://bioteque.irbbarcelona.org/downloads/embeddings%3E{abbreviation}%3E{abbreviation}-{edge_rela_type_label_data_source[0]}-{edge_rela_type_label_data_source[1]}%3E{edge_rela_type_label_data_source[2]}/embeddings.tar.gz'
            print('Downloading', url)
            file_name= f'data/edges/{abbreviation}_{edge_rela_type_label_data_source[0]}_{edge_rela_type_label_data_source[1]}_{edge_rela_type_label_data_source[2]}.tsv'
            if not os.path.isfile(file_name):
                re = requests.get(url, stream=True)
                files=tarfile.open(fileobj=re.raw, mode='r:gz')
                print('huhu')
                # print(files.getnames())
                files.extractall('./data/edges/')
                os.rename('./data/edges/dists.tsv', file_name)
                for file in os.listdir('./data/edges/'):
                    if not file.endswith('.tsv'):
                        os.remove(os.path.join('./data/edges/', file))
                files.close()
            with open(file_name,'r', encoding='utf-8') as f:
                csv_reader =csv.reader(f, delimiter='\t')
                header=next(csv_reader)
                prepare_edge_query(dict_abb_to_label[abbreviation],dict_abb_to_label[edge_rela_type_label_data_source[1]],edge_rela_type_label_data_source[0],file_name,header[0],header[1], edge_rela_type_label_data_source[2])


download_and_unzip_nodes()
download_and_unzip_edges()

print('end')