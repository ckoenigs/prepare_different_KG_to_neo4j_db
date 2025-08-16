import csv
import glob

cypher_node = open('output/cypher_node.cypher', 'w', encoding='utf-8')
cypher_edge = open('output/cypher_edge.cypher', 'w', encoding='utf-8')


def prepare_node_query(file_name, node_entity, identifier, properties_node):
    head_query = f"LOAD CSV WITH HEADERS FROM 'file:/mnt/aba90170-e6a0-4d07-929e-1200a6bfc6e1/databases/KGs/iBHK/{file_name}' AS line FIELDTERMINATOR ',' Call {{ with line %s }} IN TRANSACTIONS OF 10000 ROWS;\n"
    query_start = f"Create (n:{node_entity} {{"
    prop_list = []
    for prop in properties_node:
        if prop == identifier:
            prop_list.append('id:line.`' + prop + '`')
        else:
            prop_list.append(prop.replace(' ', '').replace('-', '_').replace('.', '_dot_') + ':line.`' + prop + '`')
    query = query_start + ', '.join(prop_list) + '}) '
    head_query = head_query % (query)
    cypher_node.write(head_query)
    cypher_node.write(f"CREATE INDEX index{node_entity} FOR (node:{node_entity}) ON (node.id);\n")


def prepare_edge_query(node_entity_1, node_entity_2, node_id_1, node_id_2, edge_type_correction, edge_type, file_name):
    query = f"LOAD CSV WITH HEADERS FROM 'file:/mnt/aba90170-e6a0-4d07-929e-1200a6bfc6e1/databases/KGs/iBHK/{file_name}' AS line FIELDTERMINATOR ',' Call {{ with line with line Where line.`{edge_type}`='1' Match (n:{node_entity_1} {{id:line.{node_id_1} }}), (m:{node_entity_2} {{id:line.{node_id_2} }}) Create (n)-[:{edge_type_correction} {{source:split(line.Source,';')}} ]->(m) }} IN TRANSACTIONS OF 10000 ROWS;\n"
    cypher_edge.write(query)


print('here')
for file in glob.glob('iBKH_entity-selected/*.csv'):
    print(file)
    with open(file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=',')
        header = next(reader)
        identifier = header[0]
        node_type = file.rsplit('_', 1)[0].split('/')[1]
        prepare_node_query(file, node_type, identifier, header)


def perpare_label(property):
    property = property.lower()
    splitted_property = property.split('_')
    if len(splitted_property) > 1 and len(splitted_property[1]) == 1:
        return splitted_property[0]
    if property == 'dsi':
        return 'sdsi'
    return property


for file in glob.glob('iBHK_relation-selected/*.csv'):
    # print(file)
    with open(file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=',')
        header = next(reader)
        node_entity_1 = perpare_label(header[0])
        node_entity_2 = perpare_label(header[1])
        # node_entity_1 = header[0].split('_')[0].lower()
        # node_entity_2 = header[1].split('_')[0].lower()
        source = header[-1]
        position_source = header.index('Source')
        for edge_type in header[2:position_source]:
            print(edge_type)
            edge_type_correction = edge_type.replace('/', '_').replace('. ', '_').replace(' (', '_').replace(')',
                                                                                                             '').replace(
                ', ', '_').replace(' ', '_')
            print(edge_type)
            prepare_edge_query(node_entity_1, node_entity_2, header[0], header[1], edge_type_correction, edge_type,
                               file)
