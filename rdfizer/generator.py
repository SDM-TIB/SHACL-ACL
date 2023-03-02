import os
import csv
import pandas as pd
import rdflib
from rdflib.plugins.sparql import prepareQuery
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser, ExtendedInterpolation

try:
	from string_subs import *
except:
	from .string_subs import *

import json
from urllib.request import urlopen
try:
	from triples_map import TriplesMap as tm
except:
	from .triples_map import TriplesMap as tm

global id_number
id_number = 0
global g_triples 
g_triples = {}
global number_triple
number_triple = 0
global join_table 
join_table = {}
global po_table
po_table = {}
global dic_table
dic_table = {}
global base
base = ""
global duplicate
duplicate = "yes"
global ignore
ignore = "yes"
global blank_message
blank_message = True
global knowledge_graph
knowledge_graph = ""
global general_predicates
general_predicates = {"http://www.w3.org/2000/01/rdf-schema#subClassOf":"",
						"http://www.w3.org/2002/07/owl#sameAs":"",
						"http://www.w3.org/2000/01/rdf-schema#seeAlso":"",
						"http://www.w3.org/2000/01/rdf-schema#subPropertyOf":""}

def join_iterator(data, iterator, parent, child):
	if iterator != "":
		new_iterator = ""
		temp_keys = iterator.split(".")
		row = data
		executed = True
		for tp in temp_keys:
			new_iterator += tp + "."
			if "$" != tp and "" != tp:
				if "[*][*]" in tp:
					if tp.split("[*][*]")[0] in row:
						row = row[tp.split("[*][*]")[0]]
					else:
						row = []
				elif "[*]" in tp:
					if tp.split("[*]")[0] in row:
						row = row[tp.split("[*]")[0]]
					else:
						row = []
				elif "*" == tp:
					pass
				else:
					if tp in row:
						row = row[tp]
					else:
						row = []
			elif tp == "":
				if len(row.keys()) == 1:
					while list(row.keys())[0] not in temp_keys:
						if list(row.keys())[0] not in temp_keys:
							row = row[list(row.keys())[0]]
							if isinstance(row,list):
								for sub_row in row:
									join_iterator(sub_row, iterator, parent, child)
								executed = False
								break
							elif isinstance(row,str):
								row = []
								break
						else:
							join_iterator(row[list(row.keys())[0]], "", parent, child)
				else:
					path = jsonpath_find(temp_keys[len(temp_keys)-1],row,"",[])
					for key in path[0].split("."):
						if key in temp_keys:
							join_iterator(row[key], "", parent, child)
						elif key in row:
							row = row[key]
							if isinstance(row,list):
								for sub_row in row:
									join_iterator(sub_row, iterator, parent, child)
								executed = False
								break
							elif isinstance(row,dict):
								join_iterator(row, iterator, parent, child)
								executed = False
								break
							elif isinstance(row,str):
								row = []
								break
			if new_iterator != ".":
				if "*" == new_iterator[-2]:
					for sub_row in row:
						join_iterator(sub_row, iterator.replace(new_iterator[:-1],""), parent, child)
					executed = False
					break
				if "[*][*]" in new_iterator:
					for sub_row in row:
						for sub_sub_row in row[sub_row]:
							join_iterator(sub_sub_row, iterator.replace(new_iterator[:-1],""), parent, child)
					executed = False
					break
				if isinstance(row,list):
					for sub_row in row:
						join_iterator(sub_row, iterator.replace(new_iterator[:-1],""), parent, child)
					executed = False
					break
	else:
		if parent.triples_map_id + "_" + child.child[0] not in join_table:
			hash_maker([data], parent, child)
		else:
			hash_update([data], parent, child, parent.triples_map_id + "_" + child.child[0])
def hash_maker(parent_data, parent_subject, child_object):
	global blank_message
	hash_table = {}
	for row in parent_data:
		if child_object.parent[0] in row.keys():
			if row[child_object.parent[0]] in hash_table:
				if duplicate == "yes":
					if parent_subject.subject_map.subject_mapping_type == "reference":
						value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
						if value != None:
							if "http" in value and "<" not in value:
								value = "<" + value[1:-1] + ">"
							elif "http" in value and "<" in value:
								value = value[1:-1]
						if value not in hash_table[row[child_object.parent[0]]]:
							hash_table[row[child_object.parent[0]]].update({value : "object"})
					else:
						if string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) != None:
							value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator)
							if value != None:
								if parent_subject.subject_map.term_type != None:
									if "BlankNode" in parent_subject.subject_map.term_type:
										if "/" in value:
											value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
											if "." in value:
												value = value.replace(".","2E")
											if blank_message:
												print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
												blank_message = False
										else:
											value = "_:" + encode_char(value).replace("%","")
											if "." in value:
												value = value.replace(".","2E")
								else:
									value = "<" + value + ">"
								hash_table[row[child_object.parent[0]]].update({value : "object"})
				else:
					if parent_subject.subject_map.subject_mapping_type == "reference":
						value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1]
						hash_table[row[child_object.parent[0]]].update({value : "object"})
					else:
						value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator)
						if value != None:
							if parent_subject.subject_map.term_type != None:
								if "BlankNode" in parent_subject.subject_map.term_type:
									if "/" in value:
										value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
										if "." in value:
											value = value.replace(".","2E")
										if blank_message:
											print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
											blank_message = False
									else:
										value = "_:" + encode_char(value).replace("%","")
										if "." in value:
											value = value.replace(".","2E")
							else:
								value = "<" + value + ">"
							hash_table[row[child_object.parent[0]]].update({value : "object"})

			else:
				if parent_subject.subject_map.subject_mapping_type == "reference":
					value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
					if value != None:
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1]
					hash_table.update({row[child_object.parent[0]] : {value : "object"}})
				else:
					value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator)
					if value != None:
						if parent_subject.subject_map.term_type != None:
							if "BlankNode" in parent_subject.subject_map.term_type:
								if "/" in value:
									value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
									if blank_message:
										print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
										blank_message = False
								else:
									value = "_:" + encode_char(value).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
						else:
							value = "<" + value + ">"
						hash_table.update({row[child_object.parent[0]] : {value : "object"}})
	join_table.update({parent_subject.triples_map_id + "_" + child_object.child[0] : hash_table})

def hash_update(parent_data, parent_subject, child_object,join_id):
	hash_table = {}
	for row in parent_data:
		if child_object.parent[0] in row.keys():
			if row[child_object.parent[0]] in hash_table:
				if duplicate == "yes":
					if parent_subject.subject_map.subject_mapping_type == "reference":
						value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
						if value != None:
							if "http" in value and "<" not in value:
								value = "<" + value[1:-1] + ">"
							elif "http" in value and "<" in value:
								value = value[1:-1]
						if value not in hash_table[row[child_object.parent[0]]]:
							hash_table[row[child_object.parent[0]]].update({value : "object"})
					else:
						if string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) is not None:
							if "<" + string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) + ">" not in hash_table[row[child_object.parent[0]]]:
								hash_table[row[child_object.parent[0]]].update({"<" + string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) + ">" : "object"})
				else:
					if parent_subject.subject_map.subject_mapping_type == "reference":
						value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore)
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1]
						hash_table[row[child_object.parent[0]]].update({value : "object"})
					else:
						if string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) is not None:
							hash_table[row[child_object.parent[0]]].update({"<" + string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) + ">" : "object"})

			else:
				if parent_subject.subject_map.subject_mapping_type == "reference":
					value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
					if value != None:
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1]
					hash_table.update({row[child_object.parent[0]] : {value : "object"}})
				else:
					if string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) is not None:
						hash_table.update({row[child_object.parent[0]] : {"<" + string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) + ">" : "object"}})
	join_table[join_id].update(hash_table)

def hash_maker_list(parent_data, parent_subject, child_object):
	hash_table = {}
	global blank_message
	for row in parent_data:
		if sublist(child_object.parent,row.keys()):
			if child_list_value(child_object.parent,row) in hash_table:
				if duplicate == "yes":
					if parent_subject.subject_map.subject_mapping_type == "reference":
						value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
						if value != None:
							if "http" in value and "<" not in value:
								value = "<" + value[1:-1] + ">"
							elif "http" in value and "<" in value:
								value = value[1:-1]
						hash_table[child_list_value(child_object.parent,row)].update({value : "object"})
					else:
						value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator)
						if value != None:
							if parent_subject.subject_map.term_type != None:
								if "BlankNode" in parent_subject.subject_map.term_type:
									if "/" in value:
										value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
										if "." in value:
											value = value.replace(".","2E")
										if blank_message:
											print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
											blank_message = False
									else:
										value = "_:" + encode_char(value).replace("%","")
										if "." in value:
											value = value.replace(".","2E")
							else:
								value = "<" + value + ">"
							hash_table[child_list_value(child_object.parent,row)].update({value: "object"})


				else:
					if parent_subject.subject_map.subject_mapping_type == "reference":
						value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1]
						hash_table[child_list_value(child_object.parent,row)].update({value : "object"})
					else:
						value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator)
						if value != None:
							if parent_subject.subject_map.term_type != None:
								if "BlankNode" in parent_subject.subject_map.term_type:
									if "/" in value:
										value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
										if "." in value:
											value = value.replace(".","2E")
										if blank_message:
											print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
											blank_message = False
									else:
										value = "_:" + encode_char(value).replace("%","")
										if "." in value:
											value = value.replace(".","2E")
							else:
								value = "<" + value + ">"
							hash_table[child_list_value(child_object.parent,row)].update({value: "object"})

			else:
				if parent_subject.subject_map.subject_mapping_type == "reference":
					value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
					if value != None:
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1]
					hash_table.update({child_list_value(child_object.parent,row) : {value : "object"}})
				else:
					value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator)
					if value != None:
						if parent_subject.subject_map.term_type != None:
							if "BlankNode" in parent_subject.subject_map.term_type:
								if "/" in value:
									value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
									if blank_message:
										print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
										blank_message = False
								else:
									value = "_:" + encode_char(value).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
						else:
							value = "<" + value + ">"
						hash_table.update({child_list_value(child_object.parent,row) : {value : "object"}})
	join_table.update({parent_subject.triples_map_id + "_" + child_list(child_object.child) : hash_table})

def hash_maker_array(parent_data, parent_subject, child_object):
	hash_table = {}
	row_headers=[x[0] for x in parent_data.description]
	for row in parent_data:
		element =row[row_headers.index(child_object.parent[0])]
		if type(element) is int:
			element = str(element)
		if row[row_headers.index(child_object.parent[0])] in hash_table:
			if duplicate == "yes":
				if "<" + string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers,"object",ignore) + ">" not in hash_table[row[row_headers.index(child_object.parent[0])]]:
					hash_table[element].update({"<" + string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers,"object",ignore) + ">" : "object"})
			else:
				hash_table[element].update({"<" + string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers, "object",ignore) + ">" : "object"})
			
		else:
			hash_table.update({element : {"<" + string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers, "object",ignore) + ">" : "object"}}) 
	join_table.update({parent_subject.triples_map_id + "_" + child_object.child[0]  : hash_table})

def hash_maker_array_list(parent_data, parent_subject, child_object, r_w):
	hash_table = {}
	global blank_message
	row_headers=[x[0] for x in parent_data.description]
	for row in parent_data:
		if child_list_value_array(child_object.parent,row,row_headers) in hash_table:
			if duplicate == "yes":
				if parent_subject.subject_map.subject_mapping_type == "reference":
					value = string_substitution_array(parent_subject.subject_map.value, ".+", row, row_headers,"object",ignore)
					if value is not None:
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1] 
					if value not in hash_table[child_list_value_array(child_object.parent,row,row_headers)]:
						hash_table[child_list_value_array(child_object.parent,row,row_headers)].update({value + ">" : "object"})

				else:
					value = string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers,"object",ignore)
					if value is not None:
						if parent_subject.subject_map.term_type != None:
							if "BlankNode" in parent_subject.subject_map.term_type:
								if "/" in value:
									value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
									if blank_message:
										print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
										blank_message = False
								else:
									value = "_:" + encode_char(value).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
						else:
							value = "<" + value + ">"
						if value not in hash_table[child_list_value_array(child_object.parent,row,row_headers)]:
							hash_table[child_list_value_array(child_object.parent,row,row_headers)].update({value : "object"})
			else:
				if parent_subject.subject_map.subject_mapping_type == "reference":
					value = string_substitution_array(parent_subject.subject_map.value, ".+", row, row_headers,"object",ignore)
					if value is not None:
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1] 
					hash_table[child_list_value_array(child_object.parent,row,row_headers)].update({value : "object"})
				else:
					value = string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers,"object",ignore)
					if value is not None:
						if parent_subject.subject_map.term_type != None:
							if "BlankNode" in parent_subject.subject_map.term_type:
								if "/" in value:
									value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
									if blank_message:
										print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
										blank_message = False
								else:
									value = "_:" + encode_char(value).replace("%","")
								if "." in value:
									value = value.replace(".","2E")
						else:
							value = "<" + value + ">"
						hash_table[child_list_value_array(child_object.parent,row,row_headers)].update({value : "object"})
			
		else:
			if parent_subject.subject_map.subject_mapping_type == "reference":
				value = string_substitution_array(parent_subject.subject_map.value, ".+", row, row_headers,"object",ignore)
				if value is not None:
					if "http" in value and "<" not in value:
						value = "<" + value[1:-1] + ">"
					elif "http" in value and "<" in value:
							value = value[1:-1]
				hash_table.update({child_list_value_array(child_object.parent,row,row_headers):{value : "object"}})
			else:
				value = string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers,"object",ignore)
				if value is not None:
					if parent_subject.subject_map.term_type != None:
						if "BlankNode" in parent_subject.subject_map.term_type:
							if "/" in value:
								value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
								if blank_message:
									print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
									blank_message = False
							else:
								value = "_:" + encode_char(value).replace("%","")
							if "." in value:
								value = value.replace(".","2E")
					else:
						value = "<" + value + ">"
					hash_table.update({child_list_value_array(child_object.parent,row,row_headers) : {value : "object"}}) 
	join_table.update({parent_subject.triples_map_id + "_" + child_list(child_object.child)  : hash_table})

def dictionary_table_update(resource):
	if resource not in dic_table:
		global id_number
		dic_table[resource] = base36encode(id_number)
		id_number += 1

def mapping_parser(mapping_file):

	"""
	(Private function, not accessible from outside this package)
	Takes a mapping file in Turtle (.ttl) or Notation3 (.n3) format and parses it into a list of
	TriplesMap objects (refer to TriplesMap.py file)
	Parameters
	----------
	mapping_file : string
		Path to the mapping file
	Returns
	-------
	A list of TriplesMap objects containing all the parsed rules from the original mapping file
	"""

	mapping_graph = rdflib.Graph()

	try:
		mapping_graph.parse(mapping_file, format='n3')
	except Exception as n3_mapping_parse_exception:
		print(n3_mapping_parse_exception)
		print('Could not parse {} as a mapping file'.format(mapping_file))
		print('Aborting...')
		sys.exit(1)

	mapping_query = """
		prefix rr: <http://www.w3.org/ns/r2rml#> 
		prefix rml: <http://semweb.mmlab.be/ns/rml#> 
		prefix ql: <http://semweb.mmlab.be/ns/ql#> 
		prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#>
		prefix fnml: <http://semweb.mmlab.be/ns/fnml#>
		prefix td: <https://www.w3.org/2019/wot/td#>
		prefix htv: <http://www.w3.org/2011/http#>
		prefix hctl: <https://www.w3.org/2019/wot/hypermedia#> 
		SELECT DISTINCT *
		WHERE {
	# Subject -------------------------------------------------------------------------
		
			?triples_map_id rml:logicalSource ?_source .
			OPTIONAL{
				?_source rml:source ?data_source .
			}
			OPTIONAL{
				?_source rml:source ?data_link .
				?data_link td:hasForm ?form .
				?form hctl:hasTarget ?url_source .
		}
			OPTIONAL {?_source rml:referenceFormulation ?ref_form .}
			OPTIONAL { ?_source rml:iterator ?iterator . }
			OPTIONAL { ?_source rr:tableName ?tablename .}
			OPTIONAL { ?_source rml:query ?query .}
			
			OPTIONAL {?triples_map_id rr:subjectMap ?_subject_map .}
			OPTIONAL {?_subject_map rr:template ?subject_template .}
			OPTIONAL {?_subject_map rml:reference ?subject_reference .}
			OPTIONAL {?_subject_map rr:constant ?subject_constant}
			OPTIONAL { ?_subject_map rr:class ?rdf_class . }
			OPTIONAL { ?_subject_map rr:termType ?termtype . }
			OPTIONAL { ?_subject_map rr:graph ?graph . }
			OPTIONAL { ?_subject_map rr:graphMap ?_graph_structure .
					   ?_graph_structure rr:constant ?graph . }
			OPTIONAL { ?_subject_map rr:graphMap ?_graph_structure .
					   ?_graph_structure rr:template ?graph . }
		   	OPTIONAL {?_subject_map fnml:functionValue ?subject_function .}		   
	# Predicate -----------------------------------------------------------------------
			OPTIONAL {
			?triples_map_id rr:predicateObjectMap ?_predicate_object_map .
			
			OPTIONAL {
				?triples_map_id rr:predicateObjectMap ?_predicate_object_map .
				?_predicate_object_map rr:predicateMap ?_predicate_map .
				?_predicate_map rr:constant ?predicate_constant .
			}
			OPTIONAL {
				?_predicate_object_map rr:predicateMap ?_predicate_map .
				?_predicate_map rr:template ?predicate_template .
			}
			OPTIONAL {
				?_predicate_object_map rr:predicateMap ?_predicate_map .
				?_predicate_map rml:reference ?predicate_reference .
			}
			OPTIONAL {
				?_predicate_object_map rr:predicate ?predicate_constant_shortcut .
			 }
			
	# Object --------------------------------------------------------------------------
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rr:constant ?object_constant .
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rr:template ?object_template .
				OPTIONAL {?_object_map rr:termType ?term .}
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rml:reference ?object_reference .
				OPTIONAL { ?_object_map rr:language ?language .}
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rr:parentTriplesMap ?object_parent_triples_map .
				OPTIONAL {
					?_object_map rr:joinCondition ?join_condition .
					?join_condition rr:child ?child_value;
								 rr:parent ?parent_value.
				 	OPTIONAL{?parent_value fnml:functionValue ?parent_function.}
				 	OPTIONAL{?child_value fnml:functionValue ?child_function.}
				 	OPTIONAL {?_object_map rr:termType ?term .}
				}
				OPTIONAL {
					?_object_map rr:joinCondition ?join_condition .
					?join_condition rr:child ?child_value;
								 rr:parent ?parent_value;
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:object ?object_constant_shortcut .
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
			}
			OPTIONAL{
				?_predicate_object_map rr:objectMap ?_object_map .
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
				?_object_map fnml:functionValue ?function .
				OPTIONAL {?_object_map rr:termType ?term .}
				
			}
			}
			OPTIONAL {
				?_source a d2rq:Database;
  				d2rq:jdbcDSN ?jdbcDSN; 
  				d2rq:jdbcDriver ?jdbcDriver; 
			    d2rq:username ?user;
			    d2rq:password ?password .
			}
			
		} """

	mapping_query_results = mapping_graph.query(mapping_query)
	triples_map_list = []


	for result_triples_map in mapping_query_results:
		triples_map_exists = False
		for triples_map in triples_map_list:
			triples_map_exists = triples_map_exists or (str(triples_map.triples_map_id) == str(result_triples_map.triples_map_id))
		
		subject_map = None
		if result_triples_map.jdbcDSN is not None:
			jdbcDSN = result_triples_map.jdbcDSN
			jdbcDriver = result_triples_map.jdbcDriver
		if not triples_map_exists:
			if result_triples_map.subject_template is not None:
				if result_triples_map.rdf_class is None:
					reference, condition = string_separetion(str(result_triples_map.subject_template))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_template), condition, "template", [result_triples_map.rdf_class], result_triples_map.termtype, [result_triples_map.graph])
				else:
					reference, condition = string_separetion(str(result_triples_map.subject_template))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_template), condition, "template", [str(result_triples_map.rdf_class)], result_triples_map.termtype, [result_triples_map.graph])
			elif result_triples_map.subject_reference is not None:
				if result_triples_map.rdf_class is None:
					reference, condition = string_separetion(str(result_triples_map.subject_reference))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_reference), condition, "reference", [result_triples_map.rdf_class], result_triples_map.termtype, [result_triples_map.graph])
				else:
					reference, condition = string_separetion(str(result_triples_map.subject_reference))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_reference), condition, "reference", [str(result_triples_map.rdf_class)], result_triples_map.termtype, [result_triples_map.graph])
			elif result_triples_map.subject_constant is not None:
				if result_triples_map.rdf_class is None:
					reference, condition = string_separetion(str(result_triples_map.subject_constant))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_constant), condition, "constant", [result_triples_map.rdf_class], result_triples_map.termtype, [result_triples_map.graph])
				else:
					reference, condition = string_separetion(str(result_triples_map.subject_constant))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_constant), condition, "constant", [str(result_triples_map.rdf_class)], result_triples_map.termtype, [result_triples_map.graph])
			elif result_triples_map.subject_function is not None:
				if result_triples_map.rdf_class is None:
					reference, condition = string_separetion(str(result_triples_map.subject_constant))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_function), condition, "function", [result_triples_map.rdf_class], result_triples_map.termtype, [result_triples_map.graph])
				else:
					reference, condition = string_separetion(str(result_triples_map.subject_constant))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_function), condition, "function", [str(result_triples_map.rdf_class)], result_triples_map.termtype, [result_triples_map.graph])
				
			mapping_query_prepared = prepareQuery(mapping_query)


			mapping_query_prepared_results = mapping_graph.query(mapping_query_prepared, initBindings={'triples_map_id': result_triples_map.triples_map_id})




			predicate_object_maps_list = []

			function = False
			for result_predicate_object_map in mapping_query_prepared_results:

				if result_predicate_object_map.predicate_constant is not None:
					predicate_map = tm.PredicateMap("constant", str(result_predicate_object_map.predicate_constant), "")
				elif result_predicate_object_map.predicate_constant_shortcut is not None:
					predicate_map = tm.PredicateMap("constant shortcut", str(result_predicate_object_map.predicate_constant_shortcut), "")
				elif result_predicate_object_map.predicate_template is not None:
					template, condition = string_separetion(str(result_predicate_object_map.predicate_template))
					predicate_map = tm.PredicateMap("template", template, condition)
				elif result_predicate_object_map.predicate_reference is not None:
					reference, condition = string_separetion(str(result_predicate_object_map.predicate_reference))
					predicate_map = tm.PredicateMap("reference", reference, condition)
				else:
					predicate_map = tm.PredicateMap("None", "None", "None")

				if "execute" in predicate_map.value:
					function = True

				if result_predicate_object_map.object_constant is not None:
					object_map = tm.ObjectMap("constant", str(result_predicate_object_map.object_constant), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.object_template is not None:
					object_map = tm.ObjectMap("template", str(result_predicate_object_map.object_template), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.object_reference is not None:
					object_map = tm.ObjectMap("reference", str(result_predicate_object_map.object_reference), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.object_parent_triples_map is not None:
					if (result_predicate_object_map.child_function is not None) and (result_predicate_object_map.parent_function is not None):
						object_map = tm.ObjectMap("parent triples map function", str(result_predicate_object_map.object_parent_triples_map), str(result_predicate_object_map.object_datatype), str(result_predicate_object_map.child_function), str(result_predicate_object_map.parent_function), result_predicate_object_map.term, result_predicate_object_map.language)
					elif (result_predicate_object_map.child_function is None) and (result_predicate_object_map.parent_function is not None):
						object_map = tm.ObjectMap("parent triples map parent function", str(result_predicate_object_map.object_parent_triples_map), str(result_predicate_object_map.object_datatype), str(result_predicate_object_map.child_function), str(result_predicate_object_map.parent_value), result_predicate_object_map.term, result_predicate_object_map.language)
					elif (result_predicate_object_map.child_function is not None) and (result_predicate_object_map.parent_function is None):
						object_map = tm.ObjectMap("parent triples map child function", str(result_predicate_object_map.object_parent_triples_map), str(result_predicate_object_map.object_datatype), str(result_predicate_object_map.child_value), str(result_predicate_object_map.parent_function), result_predicate_object_map.term, result_predicate_object_map.language)
					else:
						object_map = tm.ObjectMap("parent triples map", str(result_predicate_object_map.object_parent_triples_map), str(result_predicate_object_map.object_datatype), str(result_predicate_object_map.child_value), str(result_predicate_object_map.parent_value), result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.object_constant_shortcut is not None:
					object_map = tm.ObjectMap("constant shortcut", str(result_predicate_object_map.object_constant_shortcut), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.function is not None:
					object_map = tm.ObjectMap("reference function", str(result_predicate_object_map.function),str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				else:
					object_map = tm.ObjectMap("None", "None", "None", "None", "None", "None", "None")

				predicate_object_maps_list += [tm.PredicateObjectMap(predicate_map, object_map)]

			if function:
				current_triples_map = tm.TriplesMap(str(result_triples_map.triples_map_id), str(result_triples_map.data_source), None, predicate_object_maps_list, ref_form=str(result_triples_map.ref_form), iterator=str(result_triples_map.iterator), tablename=str(result_triples_map.tablename), query=str(result_triples_map.query),function=True)
			else:
				if result_triples_map.url_source is not None:
					current_triples_map = tm.TriplesMap(str(result_triples_map.triples_map_id), str(result_triples_map.url_source), subject_map, predicate_object_maps_list, ref_form=str(result_triples_map.ref_form), iterator=str(result_triples_map.iterator), tablename=str(result_triples_map.tablename), query=str(result_triples_map.query),function=False)
				else:
					current_triples_map = tm.TriplesMap(str(result_triples_map.triples_map_id), str(result_triples_map.data_source), subject_map, predicate_object_maps_list, ref_form=str(result_triples_map.ref_form), iterator=str(result_triples_map.iterator), tablename=str(result_triples_map.tablename), query=str(result_triples_map.query),function=False)
			triples_map_list += [current_triples_map]

	return triples_map_list

def semantify_file(triples_map, triples_map_list, delimiter, data):

	object_list = []
	triples_string = ""
	end_turtle = ""
	no_update = True
	global blank_message
	global knowledge_graph
	# print("TM:",triples_map.triples_map_name)
	for row in data:
		generated = 0
		duplicate_type = False
		if triples_map.subject_map.subject_mapping_type == "template":
			subject_value = string_substitution(triples_map.subject_map.value, "{(.+?)}", row, "subject", ignore, triples_map.iterator)	
			if triples_map.subject_map.term_type is None:
				if triples_map.subject_map.condition == "":

					try:
						subject = "<" + subject_value + ">"
					except:
						subject = None

				else:
					try:
						subject = "<" + subject_value  + ">"
					except:
						subject = None 
			else:
				if "IRI" in triples_map.subject_map.term_type:
					subject_value = string_substitution(triples_map.subject_map.value, "{(.+?)}", row, "subject", ignore, triples_map.iterator)
					if triples_map.subject_map.condition == "":

						try:
							if "http" not in subject_value:
								subject = "<" + base + subject_value + ">"
							else:
								subject = "<" + encode_char(subject_value) + ">"
						except:
							subject = None

					else:
						try:
							if "http" not in subject_value:
								subject = subject = "<" + base + subject_value + ">"
							else:
								subject = "<" + subject_value + ">"
						except:
							subject = None 

				elif "BlankNode" in triples_map.subject_map.term_type:
					if triples_map.subject_map.condition == "":
						try:
							if "/" in subject_value:
								subject  = "_:" + encode_char(subject_value.replace("/","2F")).replace("%","")
								if "." in subject:
									subject = subject.replace(".","2E")
								if blank_message:
									print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
									blank_message = False
							else:
								subject = "_:" + encode_char(subject_value).replace("%","")
								if "." in subject:
									subject = subject.replace(".","2E")
						except:
							subject = None

					else:
						try:
							subject = "_:" + subject_value  
						except:
							subject = None
				elif "Literal" in triples_map.subject_map.term_type:
					subject = None			
				else:
					if triples_map.subject_map.condition == "":

						try:
							subject = "<" + subject_value + ">"
						except:
							subject = None

					else:
						try:
							subject = "<" + subject_value + ">"
						except:
							subject = None 
		elif "reference" in triples_map.subject_map.subject_mapping_type:
			subject_value = string_substitution(triples_map.subject_map.value, ".+", row, "subject",ignore , triples_map.iterator)
			if subject_value != None:
				subject_value = subject_value[1:-1]
				if triples_map.subject_map.condition == "":
					if " " not in subject_value:
						if "http" not in subject_value:
							subject = "<" + base + subject_value + ">"
						else:
							subject = "<" + subject_value + ">"
					else:
						subject = None

			else:
				try:
					if "http" not in subject_value:
						subject = "<" + base + subject_value + ">"
					else:
						subject = "<" + subject_value + ">"
				except:
					subject = None

		elif "constant" in triples_map.subject_map.subject_mapping_type:
			subject = "<" + subject_value + ">"

		else:
			if triples_map.subject_map.condition == "":

				try:
					subject = "\"" + triples_map.subject_map.value + "\""
				except:
					subject = None

			else:
				try:
					subject = "\"" + triples_map.subject_map.value + "\""
				except:
					subject = None


		if triples_map.subject_map.rdf_class != None and subject != None:
			predicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
			for rdf_class in triples_map.subject_map.rdf_class:
				if rdf_class != None and  "str" == type(rdf_class).__name__:
					obj = "<{}>".format(rdf_class)
					rdf_type = subject + " " + predicate + " " + obj + ".\n"
					for graph in triples_map.subject_map.graph:
						if graph != None and "defaultGraph" not in graph:
							if "{" in graph:	
								rdf_type = rdf_type[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + "> .\n"
								dictionary_table_update("<" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
							else:
								rdf_type = rdf_type[:-2] + " <" + graph + "> .\n"
								dictionary_table_update("<" + graph + ">")
					if duplicate == "yes":
						dictionary_table_update(subject)
						dictionary_table_update(obj)
						dictionary_table_update(predicate + "_" + obj)
						if dic_table[predicate + "_" + obj] not in g_triples:
							knowledge_graph += rdf_type
						elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + obj]]:
							knowledge_graph += rdf_type
					else:
						knowledge_graph += rdf_type
		
		for predicate_object_map in triples_map.predicate_object_maps_list:
			if predicate_object_map.predicate_map.mapping_type == "constant" or predicate_object_map.predicate_map.mapping_type == "constant shortcut":
				predicate = "<" + predicate_object_map.predicate_map.value + ">"
			elif predicate_object_map.predicate_map.mapping_type == "template":
				if predicate_object_map.predicate_map.condition != "":
						try:
							predicate = "<" + string_substitution(predicate_object_map.predicate_map.value, "{(.+?)}", row, "predicate",ignore, triples_map.iterator) + ">"
						except:
							predicate = None
				else:
					try:
						predicate = "<" + string_substitution(predicate_object_map.predicate_map.value, "{(.+?)}", row, "predicate",ignore, triples_map.iterator) + ">"
					except:
						predicate = None
			elif predicate_object_map.predicate_map.mapping_type == "reference":
				if predicate_object_map.predicate_map.condition != "":
					predicate = string_substitution(predicate_object_map.predicate_map.value, ".+", row, "predicate",ignore, triples_map.iterator)
				else:
					predicate = string_substitution(predicate_object_map.predicate_map.value, ".+", row, "predicate",ignore, triples_map.iterator)
				predicate = "<" + predicate[1:-1] + ">"
			else:
				predicate = None

			if predicate_object_map.object_map.mapping_type == "constant" or predicate_object_map.object_map.mapping_type == "constant shortcut":
				if "/" in predicate_object_map.object_map.value:
					object = "<" + predicate_object_map.object_map.value + ">"
				else:
					object = "\"" + predicate_object_map.object_map.value + "\""
				if predicate_object_map.object_map.datatype != None:
					object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
			elif predicate_object_map.object_map.mapping_type == "template":
				try:
					if predicate_object_map.object_map.term is None:
						object = "<" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row, "object",ignore, triples_map.iterator) + ">"
					elif "IRI" in predicate_object_map.object_map.term:
						object = "<" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row, "object",ignore, triples_map.iterator) + ">"
					elif "BlankNode" in predicate_object_map.object_map.term:
						object = "_:" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row, "object",ignore, triples_map.iterator)
						if "/" in object:
							object  = object.replace("/","2F")
							if blank_message:
								print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
								blank_message = False
						if "." in object:
							object = object.replace(".","2E")
						object = encode_char(object)
					else:
						object = "\"" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row, "object",ignore, triples_map.iterator) + "\""
						if predicate_object_map.object_map.datatype != None:
							object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
						elif predicate_object_map.object_map.language != None:
							if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
								object += "@es"
							elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
								object += "@en"
							elif len(predicate_object_map.object_map.language) == 2:
								object += "@"+predicate_object_map.object_map.language
						elif predicate_object_map.object_map.language_map != None:
							lang = string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object",ignore, triples_map.iterator)
							if lang != None:
								object += "@" + string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object",ignore, triples_map.iterator)[1:-1]  
				except TypeError:
					object = None
			elif predicate_object_map.object_map.mapping_type == "reference":
				object = string_substitution(predicate_object_map.object_map.value, ".+", row, "object",ignore, triples_map.iterator)
				if object != None:
					if "\\" in object[1:-1]:
						object = "\"" + object[1:-1].replace("\\","\\\\") + "\""
					if "'" in object[1:-1]:
						object = "\"" + object[1:-1].replace("'","\\\\'") + "\""
					if "\n" in object:
						object = object.replace("\n","\\n")
					if predicate_object_map.object_map.datatype != None:
						object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
					elif predicate_object_map.object_map.language != None:
						if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
							object += "@es"
						elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
							object += "@en"
						elif len(predicate_object_map.object_map.language) == 2:
							object += "@"+predicate_object_map.object_map.language
					elif predicate_object_map.object_map.language_map != None:
						lang = string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object",ignore, triples_map.iterator)
						if lang != None:
							object += "@"+ string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object",ignore, triples_map.iterator)[1:-1]
					elif predicate_object_map.object_map.term != None:
						if "IRI" in predicate_object_map.object_map.term:
							if " " not in object:
								object = "\"" + object[1:-1].replace("\\\\'","'") + "\""
								object = "<" + encode_char(object[1:-1]) + ">"
							else:
								object = None
			elif predicate_object_map.object_map.mapping_type == "parent triples map":
				if subject != None:
					for triples_map_element in triples_map_list:
						if triples_map_element.triples_map_id == predicate_object_map.object_map.value:
							if triples_map_element.data_source != triples_map.data_source:
								if len(predicate_object_map.object_map.child) == 1:
									if (triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]) not in join_table:
										if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
											with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
												if str(triples_map_element.file_format).lower() == "csv":
													reader = pd.read_csv(str(triples_map_element.data_source), dtype = str, encoding = "latin-1")
													reader = reader.where(pd.notnull(reader), None)
													reader = reader.drop_duplicates(keep ='first')
													data = reader.to_dict(orient='records')
													hash_maker(data, triples_map_element, predicate_object_map.object_map)
												else:
													data = json.load(input_file_descriptor)
													if triples_map_element.iterator:
														if triples_map_element.iterator != "None" and triples_map_element.iterator != "$.[*]":
															join_iterator(data, triples_map_element.iterator, triples_map_element, predicate_object_map.object_map)
														else:
															if isinstance(data, list):
																hash_maker(data, triples_map_element, predicate_object_map.object_map)
															elif len(data) < 2:
																hash_maker(data[list(data.keys())[0]], triples_map_element, predicate_object_map.object_map)
													else:
														if isinstance(data, list):
															hash_maker(data, triples_map_element, predicate_object_map.object_map)
														elif len(data) < 2:
															hash_maker(data[list(data.keys())[0]], triples_map_element, predicate_object_map.object_map)
							

									if sublist(predicate_object_map.object_map.child,row.keys()):
										if child_list_value(predicate_object_map.object_map.child,row) in join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)]:
											object_list = join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)][child_list_value(predicate_object_map.object_map.child,row)]
										else:
											if no_update:
												if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
													with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
														if str(triples_map_element.file_format).lower() == "csv":
															reader = pd.read_csv(str(triples_map_element.data_source), dtype = str, encoding = "latin-1")
															reader = reader.where(pd.notnull(reader), None)
															reader = reader.drop_duplicates(keep ='first')
															data = reader.to_dict(orient='records')
															hash_update(data, triples_map_element, predicate_object_map.object_map, triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0])
														else:
															data = json.load(input_file_descriptor)
															if triples_map_element.iterator:
																if triples_map_element.iterator != "None" and triples_map_element.iterator != "$.[*]":
																	join_iterator(data, triples_map_element.iterator, triples_map_element, predicate_object_map.object_map)
																else:
																	if isinstance(data, list):
																		hash_maker(data, triples_map_element, predicate_object_map.object_map)
																	elif len(data) < 2:
																		hash_maker(data[list(data.keys())[0]], triples_map_element, predicate_object_map.object_map)
															else:
																if isinstance(data, list):
																	hash_maker(data, triples_map_element, predicate_object_map.object_map)
																elif len(data) < 2:
																	hash_maker(data[list(data.keys())[0]], triples_map_element, predicate_object_map.object_map)
												if child_list_value(predicate_object_map.object_map.child,row) in join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]]:
													object_list = join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]][row[predicate_object_map.object_map.child[0]]]
												else:
													object_list = []
												no_update = False
									object = None
								else:
									if (triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)) not in join_table:
										if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
											with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
												if str(triples_map_element.file_format).lower() == "csv":
													data = csv.DictReader(input_file_descriptor, delimiter=delimiter)
													hash_maker_list(data, triples_map_element, predicate_object_map.object_map)
												else:
													data = json.load(input_file_descriptor)
													if isinstance(data, list):
														hash_maker_list(data, triples_map_element, predicate_object_map.object_map)
													elif len(data) < 2:
														hash_maker_list(data[list(data.keys())[0]], triples_map_element, predicate_object_map.object_map)						
										
									if sublist(predicate_object_map.object_map.child,row.keys()):
										if child_list_value(predicate_object_map.object_map.child,row) in join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)]:
											object_list = join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)][child_list_value(predicate_object_map.object_map.child,row)]
										else:
											object_list = []
									object = None
							else:
								if predicate_object_map.object_map.parent != None:
									if predicate_object_map.object_map.parent[0] != predicate_object_map.object_map.child[0]:
										if (triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)) not in join_table:
											with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
												if str(triples_map_element.file_format).lower() == "csv":
													parent_data = csv.DictReader(input_file_descriptor, delimiter=delimiter)
													hash_maker_list(parent_data, triples_map_element, predicate_object_map.object_map)
												else:
													parent_data = json.load(input_file_descriptor)
													if isinstance(parent_data, list):
														hash_maker_list(parent_data, triples_map_element, predicate_object_map.object_map)
													else:
														hash_maker_list(parent_data[list(parent_data.keys())[0]], triples_map_element, predicate_object_map.object_map)
										if sublist(predicate_object_map.object_map.child,row.keys()):
											if child_list_value(predicate_object_map.object_map.child,row) in join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)]:
												object_list = join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)][child_list_value(predicate_object_map.object_map.child,row)]
											else:
												object_list = []
										object = None
									else:
										try:
											object = "<" + string_substitution(triples_map_element.subject_map.value, "{(.+?)}", row, "object",ignore, triples_map.iterator) + ">"
										except TypeError:
											object = None
								else:
									try:
										object = "<" + string_substitution(triples_map_element.subject_map.value, "{(.+?)}", row, "object",ignore, triples_map.iterator) + ">"
									except TypeError:
										object = None
							break
						else:
							continue
				else:
					object = None
			else:
				object = None

			if duplicate == "yes":
				if predicate in general_predicates:
					dictionary_table_update(predicate + "_" + predicate_object_map.object_map.value)
				else:
					dictionary_table_update(predicate)


			if predicate != None and object != None and subject != None:
				for graph in triples_map.subject_map.graph:
					triple = subject + " " + predicate + " " + object + ".\n"
					if graph != None and "defaultGraph" not in graph:
						if "{" in graph:
							triple = triple[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">.\n"
							dictionary_table_update("<" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
						else:
							triple = triple[:-2] + " <" + graph + ">.\n"
							dictionary_table_update("<" + graph + ">")

					if duplicate == "yes":
						dictionary_table_update(subject)
						dictionary_table_update(object)
						if predicate in general_predicates:
							if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
								knowledge_graph += triple
							elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
								knowledge_graph += triple
						else:
							if dic_table[predicate] not in g_triples:					
								knowledge_graph += triple
							elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
								knowledge_graph += triple
					else:
						knowledge_graph += triple
			elif predicate != None and subject != None and object_list:
				for obj in object_list:
					if obj != None:
						for graph in triples_map.subject_map.graph:
							if predicate_object_map.object_map.term != None:
								if "IRI" in predicate_object_map.object_map.term:
									triple = subject + " " + predicate + " <" + obj[1:-1] + ">.\n"
								else:
									triple = subject + " " + predicate + " " + obj + ".\n"
							else:
								triple = subject + " " + predicate + " " + obj + ".\n"
							if graph != None and "defaultGraph" not in graph:
								if "{" in graph:
									triple = triple[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">.\n"
									dictionary_table_update("<" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
								else:
									triple = triple[:-2] + " <" + graph + ">.\n"
									dictionary_table_update("<" + graph + ">")
							if duplicate == "yes":
								dictionary_table_update(subject)
								dictionary_table_update(obj)	
								if predicate in general_predicates:
									if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
										knowledge_graph += triple
									elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
										knowledge_graph += triple
								else:
									if dic_table[predicate] not in g_triples:
										knowledge_graph += triple
									elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
										knowledge_graph += triple
							else:
								knowledge_graph += triple

				object_list = []
			else:
				continue

def semantify_json(triples_map, triples_map_list, delimiter, data, iterator):
	# print("TM:", triples_map.triples_map_name)

	triples_map_triples = {}
	generated_triples = {}
	object_list = []
	global blank_message
	global knowledge_graph
	if iterator != "None" and iterator != "$.[*]" and iterator != "":
		new_iterator = ""
		temp_keys = iterator.split(".")
		row = data
		executed = True
		for tp in temp_keys:
			new_iterator += tp + "."
			if "$" != tp and "" != tp:
				if "[*][*]" in tp:
					if tp.split("[*][*]")[0] in row:
						row = row[tp.split("[*][*]")[0]]
					else:
						row = []
				elif "[*]" in tp:
					if tp.split("[*]")[0] in row:
						row = row[tp.split("[*]")[0]]
					else:
						row = []
				elif "*" == tp:
					pass
				else:
					if tp in row:
						row = row[tp]
					else:
						row = []
			elif "" == tp and isinstance(row,dict):
				if len(row.keys()) == 1:
					while list(row.keys())[0] not in temp_keys:
						new_iterator += "."
						row = row[list(row.keys())[0]]
						if isinstance(row,list):
							for sub_row in row:
								semantify_json(triples_map, triples_map_list, delimiter, sub_row, iterator.replace(new_iterator[:-1],""))
							executed = False
							break
						if isinstance(row,str):
							row = []
							break			
				if "*" == new_iterator[-2]:
					for sub_row in row:
						semantify_json(triples_map, triples_map_list, delimiter, row[sub_row], iterator.replace(new_iterator[:-1],""))
					executed = False
					break
				if "[*][*]" in new_iterator:
					for sub_row in row:
						for sub_sub_row in row[sub_row]:
							semantify_json(triples_map, triples_map_list, delimiter, sub_sub_row, iterator.replace(new_iterator[:-1],""))
					executed = False
					break
				if isinstance(row,list):
					for sub_row in row:
						semantify_json(triples_map, triples_map_list, delimiter, sub_row, iterator.replace(new_iterator[:-1],""))
					executed = False
					break
		if executed:
			if isinstance(row,list):
				for sub_row in row:
					semantify_json(triples_map, triples_map_list, delimiter, sub_row, iterator.replace(new_iterator[:-1],""))
			else:
				semantify_json(triples_map, triples_map_list, delimiter, row, iterator.replace(new_iterator[:-1],""))
	else:
		subject_value = string_substitution_json(triples_map.subject_map.value, "{(.+?)}", data, "subject",ignore,iterator) 		
		if triples_map.subject_map.subject_mapping_type == "template":
			if triples_map.subject_map.term_type is None:
				if triples_map.subject_map.condition == "":

					try:
						subject = "<" + subject_value + ">"
					except:
						subject = None

				else:
				#	field, condition = condition_separetor(triples_map.subject_map.condition)
				#	if row[field] == condition:
					try:
						subject = "<" + subject_value  + ">"
					except:
						subject = None
			else:
				if "IRI" in triples_map.subject_map.term_type:
					if triples_map.subject_map.condition == "":

						try:
							if "http" not in subject_value:
								subject = "<" + base + subject_value + ">"
							else:
								subject = "<" + encode_char(subject_value) + ">"
						except:
							subject = None

					else:
					#	field, condition = condition_separetor(triples_map.subject_map.condition)
					#	if row[field] == condition:
						try:
							if "http" not in subject_value:
								subject = "<" + base + subject_value + ">"
							else:
								subject = "<" + subject_value + ">" 
						except:
							subject = None
					
				elif "BlankNode" in triples_map.subject_map.term_type:
					if triples_map.subject_map.condition == "":

						try:
							if "/" in subject_value:
								subject  = "_:" + encode_char(subject_value.replace("/","2F")).replace("%","")
								if blank_message:
									print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
									blank_message = False
							else:
								subject = "_:" + encode_char(subject_value).replace("%","")
							if "." in subject:
								subject = subject.replace(".","2E")
							 
						except:
							subject = None
					else:
					#	field, condition = condition_separetor(triples_map.subject_map.condition)
					#	if row[field] == condition:
						try:
							subject = "_:" + subject_value 	 
						except:
							subject = None
				elif "Literal" in triples_map.subject_map.term_type:
					subject = None
				else:
					if triples_map.subject_map.condition == "":

						try:
							subject = "<" + subject_value + ">"
							 
						except:
							subject = None
					else:
					#	field, condition = condition_separetor(triples_map.subject_map.condition)
					#	if row[field] == condition:
						try:
							subject = "<" + subject_value + ">"
							 
						except:
							subject = None

		elif "reference" in triples_map.subject_map.subject_mapping_type:
			if triples_map.subject_map.condition == "":
				subject_value = string_substitution_json(triples_map.subject_map.value, ".+", data, "subject",ignore,iterator)
				subject_value = subject_value[1:-1]
				try:
					if " " not in subject_value:
						if "http" not in subject_value:
							subject = "<" + base + subject_value + ">"
						else:
							subject = "<" + subject_value + ">"
					else:
						print("<http://example.com/base/" + subject_value + "> is an invalid URL")
						subject = None 
				except:
					subject = None
				if triples_map.subject_map.term_type == "IRI":
					if " " not in subject_value:
						subject = "<" + encode_char(subject_value) + ">"
					else:
						subject = None
			else:
			#	field, condition = condition_separetor(triples_map.subject_map.condition)
			#	if row[field] == condition:
				try:
					if "http" not in subject_value:
						subject = "<" + base + subject_value + ">"
					else:
						subject = "<" + subject_value + ">"
				except:
					subject = None

		elif "constant" in triples_map.subject_map.subject_mapping_type:
			subject = "<" + subject_value + ">"
		elif "Literal" in triples_map.subject_map.term_type:
			subject = None
		else:
			if triples_map.subject_map.condition == "":

				try:
					subject =  "\"" + triples_map.subject_map.value + "\""
				except:
					subject = None

			else:
			#	field, condition = condition_separetor(triples_map.subject_map.condition)
			#	if row[field] == condition:
				try:
					subject =  "\"" + triples_map.subject_map.value + "\""
				except:
					subject = None

		if triples_map.subject_map.rdf_class != None and subject != None:
			predicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
			for rdf_class in triples_map.subject_map.rdf_class:
				if rdf_class != None:
					for graph in triples_map.subject_map.graph:
						obj = "<{}>".format(rdf_class)
						dictionary_table_update(subject)
						dictionary_table_update(obj)
						dictionary_table_update(predicate + "_" + obj)
						rdf_type = subject + " " + predicate + " " + obj + ".\n"
						if graph != None and "defaultGraph" not in graph:
							if "{" in graph:	
								rdf_type = rdf_type[:-2] + " <" + string_substitution_json(graph, "{(.+?)}", data, "subject",ignore,iterator) + "> .\n"
								dictionary_table_update("<" + string_substitution_json(graph, "{(.+?)}", data, "subject",ignore,iterator) + ">")
							else:
								rdf_type = rdf_type[:-2] + " <" + graph + "> .\n"
								dictionary_table_update("<" + graph + ">")
						if duplicate == "yes":
							if dic_table[predicate + "_" + obj] not in g_triples:
								knowledge_graph += rdf_type
								g_triples.update({dic_table[predicate  + "_" + obj ] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
							elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + obj]]:
								knowledge_graph += rdf_type
								g_triples[dic_table[predicate + "_" + obj]].update({dic_table[subject] + "_" + dic_table[obj] : ""})
						else:
							knowledge_graph += rdf_type
		
		for predicate_object_map in triples_map.predicate_object_maps_list:
			if predicate_object_map.predicate_map.mapping_type == "constant" or predicate_object_map.predicate_map.mapping_type == "constant shortcut":
				predicate = "<" + predicate_object_map.predicate_map.value + ">"
			elif predicate_object_map.predicate_map.mapping_type == "template":
				if predicate_object_map.predicate_map.condition != "":
						#field, condition = condition_separetor(predicate_object_map.predicate_map.condition)
						#if row[field] == condition:
						try:
							predicate = "<" + string_substitution_json(predicate_object_map.predicate_map.value, "{(.+?)}", data, "predicate",ignore, iterator) + ">"
						except:
							predicate = None
						#else:
						#	predicate = None
				else:
					try:
						predicate = "<" + string_substitution_json(predicate_object_map.predicate_map.value, "{(.+?)}", data, "predicate",ignore, iterator) + ">"
					except:
						predicate = None
			elif predicate_object_map.predicate_map.mapping_type == "reference":
				if predicate_object_map.predicate_map.condition != "":
					predicate = string_substitution_json(predicate_object_map.predicate_map.value, ".+", data, "predicate",ignore, iterator)
				else:
					predicate = string_substitution_json(predicate_object_map.predicate_map.value, ".+", data, "predicate",ignore, iterator)
				predicate = "<" + predicate[1:-1] + ">"
			else:
				predicate = None

			if predicate_object_map.object_map.mapping_type == "constant" or predicate_object_map.object_map.mapping_type == "constant shortcut":
				if "/" in predicate_object_map.object_map.value:
					object = "<" + predicate_object_map.object_map.value + ">"
				else:
					object = "\"" + predicate_object_map.object_map.value + "\""
				if predicate_object_map.object_map.datatype != None:
					object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
			elif predicate_object_map.object_map.mapping_type == "template":
				try:
					if predicate_object_map.object_map.term is None:
						object = "<" + string_substitution_json(predicate_object_map.object_map.value, "{(.+?)}", data, "object",ignore, iterator) + ">"
					elif "IRI" in predicate_object_map.object_map.term:
						object = "<" + string_substitution_json(predicate_object_map.object_map.value, "{(.+?)}", data, "object",ignore, iterator) + ">"
					elif "BlankNode" in predicate_object_map.object_map.term:
						object = "_:" + string_substitution_json(predicate_object_map.object_map.value, "{(.+?)}", data, "object",ignore, iterator)
						if "/" in object:
							object  = object.replace("/","2F")
							print("Incorrect format for Blank Nodes. \"/\" will be replace with \"-\".")
						if "." in object:
							object = object.replace(".","2E")
						object = encode_char(object)
					else:
						object = "\"" + string_substitution_json(predicate_object_map.object_map.value, "{(.+?)}", data, "object",ignore, iterator) + "\""
						if predicate_object_map.object_map.datatype != None:
							object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
						elif predicate_object_map.object_map.language != None:
							if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
								object += "@es"
							elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
								object += "@en"
							elif len(predicate_object_map.object_map.language) == 2:
								object += "@"+predicate_object_map.object_map.language
						elif predicate_object_map.object_map.language_map != None:
							lang = string_substitution_json(predicate_object_map.object_map.language_map, ".+", data, "object",ignore, iterator)
							if lang != None:
								object += "@" + string_substitution_json(predicate_object_map.object_map.language_map, ".+", data, "object",ignore, iterator)[1:-1]
				except TypeError:
					object = None
			elif predicate_object_map.object_map.mapping_type == "reference":
				object = string_substitution_json(predicate_object_map.object_map.value, ".+", data, "object", ignore, iterator)
				if isinstance(object,list):
					object_list = object
					object = None
					if object_list:
						i = 0
						while i < len(object_list):
							if "\\" in object[i][1:-1]:
								object = "\"" + object[i][1:-1].replace("\\","\\\\") + "\""
							if "'" in object_list[i][1:-1]:
								object_list[i] = "\"" + object_list[i][1:-1].replace("'","\\\\'") + "\""
							if "\n" in object_list[i]:
								object_list[i] = object_list[i].replace("\n","\\n")
							if predicate_object_map.object_map.datatype != None:
								object_list[i] = "\"" + object_list[i][1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
							elif predicate_object_map.object_map.language != None:
								if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
									object_list[i] += "@es"
								elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
									object_list[i] += "@en"
								elif len(predicate_object_map.object_map.language) == 2:
									object_list[i] += "@"+predicate_object_map.object_map.language
							elif predicate_object_map.object_map.language_map != None:
									object_list[i] += "@"+ string_substitution_json(predicate_object_map.object_map.language_map, ".+", data, "object",ignore, iterator)[1:-1]
							elif predicate_object_map.object_map.term != None:
								if "IRI" in predicate_object_map.object_map.term:
									if " " not in object_list[i]:
										object_list[i] = "\"" + object_list[i][1:-1].replace("\\\\'","'") + "\""
										object_list[i] = "<" + encode_char(object_list[i][1:-1]) + ">"
									else:
										object_list[i] = None
							i += 1
						if None in object_list:
							temp = []
							for obj in object_list:
								temp.append(obj)
							object_list = temp

				else:
					if object != None:
						if "\\" in object[1:-1]:
							object = "\"" + object[1:-1].replace("\\","\\\\") + "\""
						if "'" in object[1:-1]:
							object = "\"" + object[1:-1].replace("'","\\\\'") + "\""
						if "\n" in object:
							object = object.replace("\n","\\n")
						if predicate_object_map.object_map.datatype != None:
							object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
						elif predicate_object_map.object_map.language != None:
							if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
								object += "@es"
							elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
								object += "@en"
							elif len(predicate_object_map.object_map.language) == 2:
								object += "@"+predicate_object_map.object_map.language
						elif predicate_object_map.object_map.language_map != None:
							lang = string_substitution_json(predicate_object_map.object_map.language_map, ".+", data, "object",ignore, iterator)
							if lang != None:
								object += "@"+ string_substitution_json(predicate_object_map.object_map.language_map, ".+", data, "object",ignore, iterator)[1:-1]
						elif predicate_object_map.object_map.term != None:
							if "IRI" in predicate_object_map.object_map.term:
								if " " not in object:
									object = "\"" + object[1:-1].replace("\\\\'","'") + "\""
									object = "<" + encode_char(object[1:-1]) + ">"
								else:
									object = None
			elif predicate_object_map.object_map.mapping_type == "parent triples map":
				if subject != None:
					for triples_map_element in triples_map_list:
						if triples_map_element.triples_map_id == predicate_object_map.object_map.value:
							if triples_map_element.data_source != triples_map.data_source:
								if triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0] not in join_table:
									if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
										with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
											if str(triples_map_element.file_format).lower() == "csv":
												data_element = csv.DictReader(input_file_descriptor, delimiter=delimiter)
												hash_maker(data_element, triples_map_element, predicate_object_map.object_map)
											else:
												data_element = json.load(input_file_descriptor)
												if triples_map_element.iterator != "None" and triples_map_element.iterator != "$.[*]":
													join_iterator(data_element, triples_map_element.iterator, triples_map_element, predicate_object_map.object_map)
												else:
													hash_maker(data_element[list(data_element.keys())[0]], triples_map_element, predicate_object_map.object_map)
						
									
								if sublist(predicate_object_map.object_map.child,data.keys()):
									if child_list_value(predicate_object_map.object_map.child,data) in join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)]:
										object_list = join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)][child_list_value(predicate_object_map.object_map.child,data)]
									else:
										object_list = []
								object = None
							else:
								if predicate_object_map.object_map.parent != None:
									if triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0] not in join_table:
										with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
											if str(triples_map_element.file_format).lower() == "csv":
												data = csv.DictReader(input_file_descriptor, delimiter=delimiter)
												hash_maker(data, triples_map_element, predicate_object_map.object_map)
											else:
												parent_data = json.load(input_file_descriptor)
												if triples_map_element.iterator != "None":
													join_iterator(parent_data, triples_map_element.iterator, triples_map_element, predicate_object_map.object_map)
												else:
													hash_maker(parent_data[list(parent_data.keys())[0]], triples_map_element, predicate_object_map.object_map)
									if "." in predicate_object_map.object_map.child[0]:
										temp_keys = predicate_object_map.object_map.child[0].split(".")
										temp_data = data
										for temp in temp_keys:
											if temp in temp_data:
												temp_data = temp_data[temp]
											else:
												temp_data = ""
												break
										if temp_data in join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]] and temp_data != "":
											object_list = join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]][temp_data]
										else:
											object_list = []
									else:
										if predicate_object_map.object_map.child[0] in data.keys():
											if data[predicate_object_map.object_map.child[0]] in join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]]:
												object_list = join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]][data[predicate_object_map.object_map.child[0]]]
											else:
												object_list = []
										else:
											if "." in predicate_object_map.object_map.child[0]:
												iterators = predicate_object_map.object_map.child[0].split(".")
												if "[*]" in iterators[0]:
													data = data[iterators[0].split("[*]")[0]]
													for row in data:
														if str(row[iterators[1]]) in join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]]:
															object_list = join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]][str(row[iterators[1]])]
															if predicate != None and subject != None and object_list:
																for obj in object_list:
																	for graph in triples_map.subject_map.graph:
																		if predicate_object_map.object_map.term != None:
																			if "IRI" in predicate_object_map.object_map.term:
																				triple = subject + " " + predicate + " <" + obj[1:-1] + ">.\n"
																			else:
																				triple = subject + " " + predicate + " " + obj + ".\n"
																		else:
																			triple = subject + " " + predicate + " " + obj + ".\n"
																		if graph != None and "defaultGraph" not in graph:
																			if "{" in graph:
																				triple = triple[:-2] + " <" + string_substitution_json(graph, "{(.+?)}", data, "subject",ignore, iterator) + ">.\n"
																			else:
																				triple = triple[:-2] + " <" + graph + ">.\n"
																		if duplicate == "yes":
																			if (triple not in generated_triples) and (triple not in g_triples):
																				knowledge_graph += triple
																				generated_triples.update({triple : number_triple})
																				g_triples.update({triple : number_triple})
																		else:
																			knowledge_graph += triple
																	if predicate[1:-1] in predicate_object_map.graph:
																		triple = subject + " " + predicate + " " + obj + ".\n"
																		if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
																			if "{" in predicate_object_map.graph[predicate[1:-1]]:
																				triple = triple[:-2] + " <" + string_substitution_json(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", data, "subject",ignore, iterator) + ">.\n"
																			else:
																				triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
																			if duplicate == "yes":
																				if predicate not in g_triples:					
																					knowledge_graph += triple
																					generated_triples.update({triple : number_triple})
																					g_triples.update({predicate : {subject + "_" + object: triple}})
																				elif subject + "_" + object not in g_triples[predicate]:
																					knowledge_graph += triple
																					generated_triples.update({triple : number_triple})
																					g_triples[predicate].update({subject + "_" + object: triple})
																				elif triple not in g_triples[predicate][subject + "_" + obj]: 
																					knowledge_graph += triple
																			else:
																				knowledge_graph += triple
														object_list = []
												elif "[" in iterators[0] and "]" in iterators[0]:
													data = data[iterators[0].split("[")[0]]
													index = int(iterators[0].split("[")[1].split("]")[0])
													if index < len(data):
														if str(data[index][iterators[1]]) in join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]]:
															object_list = join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]][str(data[int(index)][iterators[1]])]
														else:
															object_list = []
													else:
														print("Requesting an element outside list range.")
														object_list = []	

									object = None
								else:
									if triples_map_element.iterator != triples_map.iterator:
										parent_iterator = triples_map_element.iterator
										child_keys = triples_map.iterator.split(".")
										for child in child_keys:
											if child in parent_iterator:
												parent_iterator = parent_iterator.replace(child,"")[1:]
											else:
												break
									else:
										parent_iterator = ""
									try:
										object = "<" + string_substitution_json(triples_map_element.subject_map.value, "{(.+?)}", data, "object",ignore, parent_iterator) + ">"
									except TypeError:
										object = None
							break
						else:
							continue
				else:
					object = None
			else:
				object = None
			
			if predicate in general_predicates:
				dictionary_table_update(predicate + "_" + predicate_object_map.object_map.value)
			else:
				dictionary_table_update(predicate)
			if predicate != None and object != None and subject != None:
				dictionary_table_update(subject)
				dictionary_table_update(object)
				for graph in triples_map.subject_map.graph:
					triple = subject + " " + predicate + " " + object + ".\n"
					if graph != None and "defaultGraph" not in graph:
						if "{" in graph:
							triple = triple[:-2] + " <" + string_substitution_json(graph, "{(.+?)}", data, "subject",ignore, iterator) + ">.\n"
							dictionary_table_update("<" + string_substitution_json(graph, "{(.+?)}", data, "subject",ignore, iterator) + ">")
						else:
							triple = triple[:-2] + " <" + graph + ">.\n"
							dictionary_table_update("<" + graph + ">")
					if duplicate == "yes":
						if predicate in general_predicates:
							if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
								knowledge_graph += triple
								g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[object]: ""}})
							elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
								knowledge_graph += triple
								g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[object]: ""})
						else:
							if dic_table[predicate] not in g_triples:					
								knowledge_graph += triple
								g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[object]: ""}})
							elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
								knowledge_graph += triple
								g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[object]: ""}) 
					else:
						knowledge_graph += triple
			elif predicate != None and subject != None and object_list:
				dictionary_table_update(subject)
				for obj in object_list:
					dictionary_table_update(obj)
					for graph in triples_map.subject_map.graph:
						if predicate_object_map.object_map.term != None:
							if "IRI" in predicate_object_map.object_map.term:
								triple = subject + " " + predicate + " <" + obj[1:-1] + ">.\n"
							else:
								triple = subject + " " + predicate + " " + obj + ".\n"
						else:
							triple = subject + " " + predicate + " " + obj + ".\n"
						if graph != None and "defaultGraph" not in graph:
							if "{" in graph:
								triple = triple[:-2] + " <" + string_substitution_json(graph, "{(.+?)}", data, "subject",ignore, iterator) + ">.\n"
								dictionary_table_update("<" + string_substitution_json(graph, "{(.+?)}", data, "subject",ignore, iterator) + ">")
							else:
								triple = triple[:-2] + " <" + graph + ">.\n"
								dictionary_table_update("<" + graph + ">")
						if duplicate == "yes":
							if predicate in general_predicates:
								if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
									knowledge_graph += triple
									g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
								elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
									knowledge_graph += triple
									g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
							else:
								if dic_table[predicate] not in g_triples:
									knowledge_graph += triple
									g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
								elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
									knowledge_graph += triple
									g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
						else:
							knowledge_graph += triple

				object_list = []
			else:
				continue
	

def kg_generation(config_path):

	if os.path.isfile(config_path) == False:
		print("The configuration file " + config_path + " does not exist.")
		print("Aborting...")
		sys.exit(1)

	config = ConfigParser(interpolation=ExtendedInterpolation())
	config.read(config_path)

	print("Beginning Knowledge Graph Creation Process.")
	for dataset_number in range(int(config["datasets"]["number_of_datasets"])):
		dataset_i = "dataset" + str(int(dataset_number) + 1)
		triples_map_list = mapping_parser(config[dataset_i]["mapping"])
		with ThreadPoolExecutor(max_workers=10) as executor:
			for triples_map in triples_map_list:
				if triples_map.file_format == "JSONPath":
					if "http" in triples_map.data_source:
						response = urlopen(triples_map.data_source)
						data = json.loads(response.read())
					else:
						data = json.loads(triples_map.data_source)
					executor.submit(semantify_json, triples_map, triples_map_list,",",data,triples_map.iterator).result()
				elif str(triples_map.file_format).lower() == "csv":
					with open(triples_map.data_source, "r", encoding = "latin-1") as input_file_descriptor:
						data = csv.DictReader(input_file_descriptor, delimiter=',')
						blank_message = True
						executor.submit(semantify_file, triples_map, triples_map_list, ",", data).result()
	global knowledge_graph
	print("The Process has ended.")
	"""print(knowledge_graph)
	graph = rdflib.Graph().parse(data=knowledge_graph, format='n3')
	data_list = json.loads(graph.serialize(format='json-ld', indent=4))
	print(graph.serialize(format='json-ld', indent=4)) 
	for elem in data_list:
		print(elem)"""
	return rdflib.Graph().parse(data=knowledge_graph, format='n3')

if __name__ == '__main__':
	kg_generation("/home/enrique/Documents/ESWC_2023_Demo/config.ini")