import csv

cypher_node = open('output/cypher_node.cypher', 'w', encoding='utf-8')
cypher_edge = open('output/cypher_edge.cypher', 'w', encoding='utf-8')


def prepare_node_query(node_entity):
    query = f"LOAD CSV WITH HEADERS FROM 'file:/mnt/aba90170-e6a0-4d07-929e-1200a6bfc6e1/databases/KGs/drkg/output/{node_entity}.tsv' AS line FIELDTERMINATOR '\t' Call {{ with line Create (n:{node_entity} {{id:line.id, source:split(line.source,'|'), resource:split(line.resource,'|') }}) }} IN TRANSACTIONS OF 10000 ROWS;\n"
    cypher_node.write(query)
    cypher_node.write(f"CREATE INDEX index{node_entity} FOR (node:{node_entity}) ON (node.id);\n")


def prepare_csv_and_cypher(node_entity):
    file = open(f'output/{node_entity}.tsv', 'w', encoding='utf-8')
    csv_writer = csv.writer(file, delimiter='\t')
    csv_writer.writerow(['id', 'source', 'resource'])
    prepare_node_query(node_entity)
    return csv_writer


def prepare_edge_query(node_entity_1, node_entity_2, edge_type, file_name):
    query = f"LOAD CSV WITH HEADERS FROM 'file:/mnt/aba90170-e6a0-4d07-929e-1200a6bfc6e1/databases/KGs/drkg/output/{file_name}.tsv' AS line FIELDTERMINATOR '\t' Call {{ with line Match (n:{node_entity_1} {{id:line.id1}}), (m:{node_entity_2} {{id:line.id2}}) Create (n)-[:{edge_type} {{source:line.source }}]->(m) }} IN TRANSACTIONS OF 10000 ROWS;\n"
    cypher_edge.write(query)


def prepare_csv_and_cypher_edge(node_entity_1, node_entity_2, edge_type):
    file_name = f'{node_entity_1}_{node_entity_2}_{edge_type}'
    file = open(f'output/{file_name}.tsv', 'w', encoding='utf-8')
    csv_writer = csv.writer(file, delimiter='\t')
    csv_writer.writerow(['id1', 'id2', 'source'])
    prepare_edge_query(node_entity_1, node_entity_2, edge_type, file_name)
    return csv_writer


dict_type_to_csv = {}
set_of_ids = set()

with open('entity2src.tsv', 'r', encoding='utf-8') as f:
    csv_reader = csv.reader(f, delimiter='\t')
    for row in csv_reader:
        identifier = row[0]
        set_of_ids.add(identifier)
        node_entity = identifier.split('::')[0].replace(' ','')
        if not node_entity in dict_type_to_csv:
            csv_writer = prepare_csv_and_cypher(node_entity)
            dict_type_to_csv[node_entity] = csv_writer
        source = []
        resource = set()
        for prop in row[1:]:
            source.append(prop)
            resource.add(prop.split(']')[0][1:])
        dict_type_to_csv[node_entity].writerow([identifier, '||'.join(source), '|'.join(resource)])

dict_tuple_type_type_rela_type_to_tsv = {}

with open('drkg.tsv', 'r', encoding='utf-8') as f:
    csv_reader = csv.reader(f, delimiter='\t')
    for row in csv_reader:
        id_1 = row[0]
        id_2 = row[2]
        if id_1 not in set_of_ids or id_2 not in set_of_ids:
            print('oh no')
        node_entity_1 = id_1.split('::')[0].replace(' ','')
        node_entity_2 = id_2.split('::')[0].replace(' ','')
        source_rela_type_tuple = row[1].split('::')
        source=source_rela_type_tuple[0]
        rela_type='_'.join(source_rela_type_tuple[1:]).replace(':','_').replace(' ','').replace('-','_').replace('+','_plus').replace('>','_to_')
        if not (node_entity_1, node_entity_2, rela_type) in dict_tuple_type_type_rela_type_to_tsv:
            print(rela_type)
            csv_writer=prepare_csv_and_cypher_edge(node_entity_1, node_entity_2, rela_type)
            dict_tuple_type_type_rela_type_to_tsv[(node_entity_1, node_entity_2, rela_type)] = csv_writer

        dict_tuple_type_type_rela_type_to_tsv[(node_entity_1, node_entity_2, rela_type)].writerow([id_1, id_2, source])