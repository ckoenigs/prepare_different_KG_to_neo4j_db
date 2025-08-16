import datetime, csv, os
import pandas as pd
from neo4j import GraphDatabase
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', 'test1234'))
g = driver.session(database='pharmebinet')


def get_count_query_result(query, label):
    query = query % label
    results = g.run(query)
    count = results.single()['a']
    return count


node_label_to_id = {}
identifier = 0


def prepare_node_infos():
    global identifier
    query = 'Match (n) With labels(n) as lab Unwind lab as l Return Distinct l'
    set_of_labels = set()
    for label, in g.run(query):
        set_of_labels.add(label)
        node_label_to_id[label] = identifier

    print('#######################################################################################################')
    print(datetime.datetime.now())
    print('node')
    file = open('analysis/nodes_information.tsv', 'w', encoding='utf-8')
    csv_writer = csv.writer(file, delimiter='\t')
    csv_writer.writerow(['Label', 'Nodes', 'Disconnected', 'Edge types'])

    # prepare node csv file wit label abbreviation number of nodes disconnected and edgtypes
    for label in set_of_labels:
        query = "Match (n:%s) Return Count(n) as a"
        number_of_nodes = get_count_query_result(query, label)

        query = "Match (n:%s) Where not (n)--() Return Count(n) as a"
        disconnected = get_count_query_result(query, label)

        query = "Match (n:%s)-[r]-() Return  count(Distinct Type(r)) as a"
        count_edge_types = get_count_query_result(query, label)

        csv_writer.writerow([label, number_of_nodes, disconnected, count_edge_types])
    file.close()




def distribution_node_degree():
    query = 'Match p=(n) With  apoc.node.degree(n) as l Return l Order by l'
    list_degree = []
    for degree, in g.run(query):
        list_degree.append(degree)

    print('average:', sum(list_degree) / len(list_degree))
    query = 'Match p=(n) With n ,  apoc.node.degree(n) as l With l, count(n) as number  Return l as degree, number Order By degree'
    df = pd.DataFrame(g.run(query).data())
    df['logarithm_base10_number'] = np.log10(df['number'])
    df['logarithm_base10_degree'] = np.log10(df['degree'])
    print(df.head())
    print(df.describe())

    sns.relplot(data=df, x="logarithm_base10_number", y="logarithm_base10_degree")
    # plt.show()
    plt.savefig('analysis/scale_free.png')
    plt.close()

def count_unique_edge_types_without_abbreviations():

    query='Match ()-[r]->() Return Distinct Type(r)'
    set_of_types=set()
    for type_edge, in g.run(query):
        print(type_edge)
        without_abb=type_edge.rsplit('_',1)[0]
        print(without_abb)
        set_of_types.add(without_abb)
    print(len(set_of_types))

def main():
    print('#############################################################')
    print(datetime.datetime.now())
    print('prepare cypherfile and tsv files ')

    # prepare_node_infos()

    print('#############################################################')
    print(datetime.datetime.now())
    print('prepare cypherfile and tsv files ')


    count_unique_edge_types_without_abbreviations()

    print('#############################################################')
    print(datetime.datetime.now())
    print('prepare cypherfile and tsv files ')


    distribution_node_degree()

    print('#############################################################')
    print(datetime.datetime.now())
    print('label to count and sources')

    print('#############################################################')
    print(datetime.datetime.now())

    driver.close()


if __name__ == "__main__":
    # execute only if run as a script
    main()
