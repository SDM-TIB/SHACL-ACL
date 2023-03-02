__author__ = 'Philipp D. Rohde'

import argparse

import shaclacl


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SHACL-ACL: Access Control with SHACL')
    parser.add_argument('query', metavar='query', type=str,
                        help='Path to a file containing the SPARQL query to be executed')
    parser.add_argument('-d', metavar='source_description', type=str, default='config/rdfmts.json', required=False,
                        help='Path to the source description file for DeTrusty (default: ./config/rdfmts.json)')
    parser.add_argument('-c', metavar='config_rdfizer', type=str, default='config/config_rdfizer.ini', required=False,
                        help='Path to the configuration file for the RDFizer (default: ./config/config_rdfizer.ini)')
    parser.add_argument('-s', metavar='schema_dir', type=str, default='shapes/', required=False,
                        help='Directory containing the SHACL shape schema (default: ./shapes/')
    args = parser.parse_args()

    query_file = args.query
    source_description = args.d
    config_rdfizer = args.c
    schema_dir = args.s

    shaclacl.get_machine_condition()
    virtual_kg = shaclacl.semantify_data(config_rdfizer)
    query_result = shaclacl.query_with_access_control(virtual_kg, query_file, source_description, schema_dir)
    print(query_result)
