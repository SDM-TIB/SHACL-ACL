__author__ = 'Philipp D. Rohde'

import re
import time
import uuid

import psutil
from DeTrusty import run_query, ConfigFile
from TravSHACL import GraphTraversal, ShapeSchema
from TravSHACL.sparql.SPARQLEndpoint import SPARQLEndpoint
from rdflib import Graph

from rdfizer import kg_generation
import pandas as pd

re_service = re.compile(r".*[^:][Ss][Ee][Rr][Vv][Ii][Cc][Ee]\s*<.+>\s*{.*", flags=re.DOTALL)


def get_machine_condition() -> None:
    """
    Gets current machine condition and stores it in a CSV.

    Gets the CPU usage (in percent), available RAM (in percent), and current time (24h, HH:MM:SS) of the system.
    The information is stored in the file `data/observation.csv`.
    """
    cpu_usage = psutil.cpu_percent(1)
    mem_free_percent = psutil.virtual_memory().available * 100 / psutil.virtual_memory().total
    local_time = time.localtime()
    current_time = time.strftime("%H:%M:%S", local_time)
    id_ = uuid.uuid4().hex
    with open('data/observation.csv', 'w', encoding='utf8') as out:
        out.write('observation,time,cpu_usage,ram_free\n')
        out.write(id_ + ',' + current_time + ',' + str(cpu_usage) + ',' + str(mem_free_percent))


def semantify_data(config_rdfizer: str) -> Graph:
    """
    Semantifies the data necessary for the policy validation and returns the virtual knowledge graph.

    Given the configuration file, the SDM-RDFizer semantifies the data following the RML mappings mentioned
    in the configuration. Data from multiple heterogeneous sources are transformed into RDF and a virtual
    knowledge graph is generated.

    Parameters
    ----------
    config_rdfizer
        The path to the configuration file for the SDM-RDFizer.

    Returns
    -------
    Graph
        An RDFlib graph containing the semantified data.
    """
    return kg_generation(config_rdfizer)


def graph_from_file(file_path: str) -> Graph:
    """
    Parses an RDF file and creates an in-memory knowledge graph from it.

    The file at the given path is loaded into an RDFlib graph in order to create an in-memory knowledge graph.

    Parameters
    ----------
    file_path
        The path to the RDF file which should be loaded into an RDFlib graph.

    Returns
    -------
    Graph
        An RDFlib graph containing the RDF data from the parsed file.
    """
    return Graph().parse(file_path)


def query_result_to_dataframe(query_result: dict) -> pd.DataFrame:
    """
    Transforms a SPARQL query result from a Python dictionary into a pandas DataFrame.

    By iterating over the dictionary containing the SPARQL query result, the query result is
    transformed into a pandas DataFrame for better readability.

    Parameters
    ----------
    query_result
        A Python dictionary containing the SPARQL query result to transform.

    Returns
    -------
    DataFrame
        A pandas DataFrame representing the SPARQL query result.
    """
    columns = query_result['head']['vars']
    df_result = pd.DataFrame(columns=columns)

    cardinality = 0
    for res in query_result['results']['bindings']:
        df_result.loc[cardinality] = [res[var]['value'] for var in columns]
        cardinality += 1

    return df_result


def query_with_access_control(data_graph: Graph, query_file: str, source_description: str, schema_dir: str) -> dict:
    """
    Validates the access control policies and, if access is granted, executes the given SPARQL query.

    The given access control policies are validated using Trav-SHACL. If the validation process does not
    identify any violations, the access is granted and the given query is executed by DeTrusty. If the
    access is denied, the result dictionary contains an error message indicating that the access was denied.

    Parameters
    ----------
    data_graph
        The RDFlib graph containing the information necessary for the policy validation.
    query_file
        Path to the file containing the SPARQL query.
    source_description
        Path to the source description file for DeTrusty.
    schema_dir
        Path to the directory containing the access control policies specified in SHACL.

    Returns
    -------
    dict
        A Python dictionary containing the SPARQL query answer if the access was granted.
        The dictionary will include an error message in the case the access was denied.
    """
    # Validate the Access Control Policies
    SPARQLEndpoint.instance = None  # need to be reset since it is a singleton and won't change otherwise
    shape_schema = ShapeSchema(
        schema_dir=schema_dir,
        schema_format='SHACL',
        endpoint=data_graph,
        graph_traversal=GraphTraversal.DFS,
        heuristics={'target': True, 'degree': 'in', 'properties': 'big'},
        use_selective_queries=True,
        max_split_size=256,
        output_dir=None,
        order_by_in_queries=False,
        save_outputs=False
    )
    validation_result = shape_schema.validate()

    # Check if the access should be granted
    valid_instances = False
    invalid_instances = False
    for shape in validation_result:
        if len(validation_result[shape]['valid_instances']) > 0:
            valid_instances = True
        if shape != 'unbound' and len(validation_result[shape]['invalid_instances']) > 0:
            invalid_instances = True
            break

    access_granted = valid_instances & (not invalid_instances)

    if access_granted:
        # Access granted -> Execute query
        query = open(query_file, "r", encoding="utf8").read()
        service = True if re_service.match(query) else False
        return run_query(query,
                         sparql_one_dot_one=service,
                         config=ConfigFile(source_description),
                         join_stars_locally=False)
    else:
        # Access denied -> Return error
        return {'error': 'Access denied! Query not executed!'}
