import csv
import glob


cypher_node = open('output/cypher_node.cypher', 'w', encoding='utf-8')
cypher_edge = open('output/cypher_edge.cypher', 'w', encoding='utf-8')

def prepare_node_query(file_name,node_entity, identifier, properties_node):
    head_query =f"LOAD CSV WITH HEADERS FROM 'file:/mnt/aba90170-e6a0-4d07-929e-1200a6bfc6e1/databases/KGs/oregano/{file_name}' AS line FIELDTERMINATOR '\t' Call {{ with line %s }} IN TRANSACTIONS OF 10000 ROWS;\n"
    query_start= f"Create (n:{node_entity} {{"
    prop_list=[]
    for prop in properties_node:
        if prop==identifier:
            prop_list.append('id:line.`'+prop+'`')
        else:
            prop_list.append(prop.replace(' ','').replace('-','_').replace('.','_dot_')+':line.`'+prop+'`')
    query= query_start+ ', '.join(prop_list)+'}) '
    head_query=head_query %(query)
    cypher_node.write(head_query)
    cypher_node.write(f"CREATE INDEX index{node_entity} FOR (node:{node_entity}) ON (node.id);\n")


def prepare_edge_query(node_entity_1, node_entity_2, edge_type, file_name):
    query = f"LOAD CSV WITH HEADERS FROM 'file:/mnt/aba90170-e6a0-4d07-929e-1200a6bfc6e1/databases/KGs/oregano/{file_name}' AS line FIELDTERMINATOR '\t' Call {{ with line Match (n:{node_entity_1} {{id:line.id1}}), (m:{node_entity_2} {{id:line.id2}}) Create (n)-[:{edge_type} ]->(m) }} IN TRANSACTIONS OF 10000 ROWS;\n"
    cypher_edge.write(query)


def prepare_csv_and_cypher_edge(node_entity_1, node_entity_2, edge_type):
    file_name = f'output/{node_entity_1}_{node_entity_2}_{edge_type}.tsv'
    file = open(file_name, 'w', encoding='utf-8')
    csv_writer = csv.writer(file, delimiter='\t')
    csv_writer.writerow(['id1', 'id2'])
    prepare_edge_query(node_entity_1, node_entity_2, edge_type, file_name)
    return csv_writer


def check_for_atc(id_info, set_atcs,csv_writer_atc):
    splitted_id = id_info.split(':')
    if len(splitted_id) == 1:
        node_entity = 'ATC'
        if id_info not in set_atcs:
            csv_writer_atc.writerow([id_info])
            set_atcs.add(id_info)
    else:
        node_entity = splitted_id[0]
        if node_entity == 'PROTEIN' or node_entity == 'MOLECULE':
            node_entity = 'TARGET'
    return node_entity

file_name='output/atc.tsv'
with open(file_name, 'w', encoding='utf-8') as g:
    csv_writer_atc = csv.writer(g, delimiter='\t')
    csv_writer_atc.writerow(['id'])
    prepare_node_query(file_name,'ATC','id',['id'])

    dict_tuple_to_tsv_writer={}
    set_atcs=set()

    for file in glob.glob('*.tsv'):
        print(file)
        if file!='OREGANO_V2.1.tsv':
            with open(file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter='\t')
                header = next(reader)
                identifier=header[0]
                node_type=file.split('.')[0]
                if node_type in ['DISEASES','GENES','PATHWAYS','PHENOTYPES']:
                    node_type=node_type[:-1]
                prepare_node_query(file, node_type, identifier, header)
        else:
            with open(file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter='\t')
                for row in reader:
                    id_1= row[0]
                    id_2= row[2]
                    edge_type=row[1]
                    node_entity_1= check_for_atc(id_1, set_atcs,csv_writer_atc)
                    node_entity_2= check_for_atc(id_2, set_atcs,csv_writer_atc)
                    if not (node_entity_1,node_entity_2,edge_type) in dict_tuple_to_tsv_writer:
                        csv_writer=prepare_csv_and_cypher_edge(node_entity_1,node_entity_2,edge_type)
                        dict_tuple_to_tsv_writer[(node_entity_1,node_entity_2,edge_type)]=csv_writer
                    dict_tuple_to_tsv_writer[(node_entity_1, node_entity_2, edge_type)].writerow([id_1, id_2])


