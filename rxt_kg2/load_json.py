import ijson
import csv


def correct_label(category):
    return category.replace('biolink:', '')


with open('kg2c_lite.json', 'r') as f:
    print('start')
    parser = ijson.items(f, 'nodes.item')

    with open('output/node.csv', 'w', encoding='utf-8') as file:
        csv_writer = csv.writer(file, delimiter=',')
        csv_writer.writerow(['nodeId:ID', 'name', ':LABEL'])
        for node in parser:
            string_labels = ';'.join([correct_label(x) for x in node['all_categories']])
            csv_writer.writerow([node['id'], node['name'], correct_label(node['category'])])
            csv_writer.writerow([node['id'], node['name'], string_labels])

    parser = ijson.items(f, 'edges.item')

    with open('output/edges.csv', 'w', encoding='utf-8') as file:
        csv_writer = csv.writer(file, delimiter=',')
        headers = ["qualified_object_direction", "qualified_predicate", "primary_knowledge_source",
                  "qualified_object_aspect", "domain_range_exclusion", "id"]
        csv_writer.writerow([':START_ID', ':END_ID', ':TYPE' ]+headers)
        counter=0
        for edges in parser:
            row=[edges['subject'],edges['object'], correct_label(edges['predicate'])]
            for header in headers:
                row.append(edges[header])
            csv_writer.writerow(row)
            counter+=1
            if counter % 1000000 == 0:
                print(counter)
