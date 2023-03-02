import re
import csv
import sys
import os
import urllib
import pandas as pd 
import math

def jsonpath_find(element, JSON, path, all_paths):
	result_path = []
	if all_paths != []:
		return all_paths
	if element in JSON:
		path = path + element
		all_paths.append(path)
		jsonpath_find(element, {}, path, all_paths)
	for key in JSON:
		if isinstance(JSON[key], dict):
			newpath = jsonpath_find(element, JSON[key],path + key + '.',all_paths)
			if len(newpath) > 0:
				if newpath[0] not in result_path:
					result_path.append(newpath[0])
		elif isinstance(JSON[key], list):
			for row in JSON[key]:
				newpath = jsonpath_find(element,row,path + key + '.',all_paths)
				if len(newpath) > 0:
					if newpath[0] not in result_path:
						result_path.append(newpath[0])
	return result_path

def extract_base(file):
	base = ""
	f = open(file,"r")
	file_lines = f.readlines()
	for line in file_lines:
		if "@base" in line:
			base = line.split(" ")[1][1:-3]
	return base

def encode_char(string):
	encoded = ""
	valid_char = ["~","#","/",":"]
	for s in string:
		if s in valid_char:
			encoded += s
		elif s == "/":
			encoded += "%2F"
		else:
			encoded += urllib.parse.quote(s)
	return encoded

def string_substitution_json(string, pattern, row, term, ignore, iterator):

	template_references = re.finditer(pattern, string)
	new_string = string
	offset_current_substitution = 0
	if iterator != "None" and "" != iterator:
		if iterator != "$.[*]":
			temp_keys = iterator.split(".")
			for tp in temp_keys:
				if "$" != tp:
					if "[*]" in tp:
						row = row[tp.split("[*]")[0]]
					else:
						row = row[tp]
				elif tp == "":
					if len(row.keys()) == 1:
						row = row[list(row.keys())[0]]
	for reference_match in template_references:
		start, end = reference_match.span()[0], reference_match.span()[1]
		if pattern == "{(.+?)}":
			if "[*]" in reference_match.group(1):
				match = reference_match.group(1)
			else:
				match = reference_match.group(1).split("[")[0]
			if "\\" in match:
				temp = match.split("{")
				match = temp[len(temp)-1]
				if match in row.keys():
					value = row[match]
				else:
					print('The attribute ' + match + ' is missing.')
					if ignore == "yes":
						return None
					print('Aborting...')
					sys.exit(1)

			elif "." in match:
				if "[*]" in match:
					child_list = row[match.split("[*]")[0]]
					match = match.split(".")[1:]
					if len(match) > 1:
						for child in child_list:
							found = False
							value = child[match[0]]
							for elem in match[1:]:
								if elem in value:
									value = value[elem]
									found = True
								else:
									found = False
									value = None
									break
							if found:
								break
						value = None
					else:
						value = None
						for child in child_list:
							if match[0] in child:
								value = child[match[0]]
								break

				else:
					temp = match.split(".")
					value = row[temp[0]]
					for t in temp:
						if t != temp[0]:
							value = value[t]						
			else:
				if match in row:
					value = row[match]
				else:
					return None
			
			if value is not None:
				if (type(value).__name__) != "str":
					if (type(value).__name__) != "float":
						value = str(value)
					else:
						value = str(math.ceil(value))
				else:
					if re.match(r'^-?\d+(?:\.\d+)$', value) is not None:
						value = str(math.ceil(float(value))) 
				if re.search("^[\s|\t]*$", value) is None:
					if "http" not in value:
						value = encode_char(value)
					new_string = new_string[:start + offset_current_substitution] + value.strip() + new_string[ end + offset_current_substitution:]
					offset_current_substitution = offset_current_substitution + len(value) - (end - start)
					if "\\" in new_string:
						new_string = new_string.replace("\\", "")
						count = new_string.count("}")
						i = 0
						new_string = " " + new_string
						while i < count:
							new_string = "{" + new_string
							i += 1
						#new_string = new_string.replace(" ", "")

				else:
					return None
			else:
				return None

		elif pattern == ".+":
			match = reference_match.group(0)
			if "[*]" in match:
				child_list = row[match.split("[*]")[0]]
				match = match.split(".")[1:]
				object_list = []
				for child in child_list:
					if len(match) > 1:
						value = child[match[0]]
						for element in match:
							if element in value:
								value = value[element]
					else:
						if match[0] in child:
							value = child[match[0]]
						else:
							value = None

					if match is not None:
						if (type(value).__name__) == "int":
								value = str(value)
						if isinstance(value, dict):
							if value:
								print("Index needed")
								return None
							else:
								return None
						elif isinstance(value, list):
							print("This level is a list.")
							return None
						else:		
							if value is not None:
								if re.search("^[\s|\t]*$", value) is None:
									new_string = new_string[:start] + value.strip().replace("\"", "'") + new_string[end:]
									new_string = "\"" + new_string + "\"" if new_string[0] != "\"" and new_string[-1] != "\"" else new_string
									object_list.append(new_string)
									new_string = string
				return object_list
			else:
				if "." in match:
					if match in row:
						value = row[match]
					else:
						temp = match.split(".")
						if temp[0] in row:
							value = row[temp[0]]
							for element in temp:
								if element in value:
									value = value[element]
						else:
							return None
				else:
					if match in row:
						value = row[match]
					else:
						return None

				if match is not None:
					if (type(value).__name__) == "int":
						value = str(value)
					elif (type(value).__name__) == "float":
						value = str(value)
					if isinstance(value, dict):
						if value:
							print("Index needed")
							return None
						else:
							return None
					else:		
						if value is not None:
							if re.search("^[\s|\t]*$", value) is None:
								new_string = new_string[:start] + value.strip().replace("\"", "'") + new_string[end:]
								new_string = "\"" + new_string + "\"" if new_string[0] != "\"" and new_string[-1] != "\"" else new_string
							else:
								return None
				else:
					return None
		else:
			print("Invalid pattern")
			if ignore == "yes":
				return None
			print("Aborting...")
			sys.exit(1)

	return new_string

def string_substitution(string, pattern, row, term, ignore, iterator):

	"""
	(Private function, not accessible from outside this package)

	Takes a string and a pattern, matches the pattern against the string and perform the substitution
	in the string from the respective value in the row.

	Parameters
	----------
	string : string
		String to be matched
	triples_map_list : string
		Pattern containing a regular expression to match
	row : dictionary
		Dictionary with CSV headers as keys and fields of the row as values

	Returns
	-------
	A string with the respective substitution if the element to be subtitued is not invalid
	(i.e.: empty string, string with just spaces, just tabs or a combination of both), otherwise
	returns None
	"""
	template_references = re.finditer(pattern, string)
	new_string = string
	offset_current_substitution = 0
	if iterator != "None":
		if iterator != "$.[*]":
			temp_keys = iterator.split(".")
			for tp in temp_keys:
				if "$" != tp and tp in row:
					if "[*]" in tp:
						row = row[tp.split("[*]")[0]]
					else:
						row = row[tp]
	for reference_match in template_references:
		start, end = reference_match.span()[0], reference_match.span()[1]
		if pattern == "{(.+?)}":
			no_match = True
			if "]." in reference_match.group(1):
				temp = reference_match.group(1).split("].")
				match = temp[1]
				condition = temp[0].split("[")
				temp_value = row[condition[0]]
				if "==" in condition[1]:
					temp_condition = condition[1][2:-1].split("==")
					iterators = temp_condition[0].split(".")
					if isinstance(temp_value,list):
						for tv in temp_value:
							t_v = tv
							for cond in iterators[:-1]:
								if cond != "@":
									t_v = t_v[cond]
							if temp_condition[1][1:-1] == t_v[iterators[-1]]:
								row = t_v
								no_match = False
					else:
						for cond in iterators[-1]:
							if cond != "@":
								temp_value = temp_value[cond]
						if temp_condition[1][1:-1] == temp_value[iterators[-1]]:
							row = temp_value
							no_match = False
				elif "!=" in condition[1]:
					temp_condition = condition[1][2:-1].split("!=")
					iterators = temp_condition[0].split(".")
					match = iterators[-1]
					if isinstance(temp_value,list):
						for tv in temp_value:
							for cond in iterators[-1]:
								if cond != "@":
									temp_value = temp_value[cond]
							if temp_condition[1][1:-1] != temp_value[iterators[-1]]:
								row = t_v
								no_match = False
					else:
						for cond in iterators[-1]:
							if cond != "@":
								temp_value = temp_value[cond]
						if temp_condition[1][1:-1] != temp_value[iterators[-1]]:
							row = temp_value
							no_match = False
				if no_match:
					return None
			else:
				match = reference_match.group(1).split("[")[0]
			if "\\" in match:
				temp = match.split("{")
				match = temp[len(temp)-1]
			if "." in match:
				if match not in row.keys():
					temp_keys = match.split(".")
					match = temp_keys[len(temp_keys) - 1]
					for tp in temp_keys[:-1]:
						if tp in row:
							row = row[tp]
						else:
							return None
			if row == None:
				return None
			if match in row.keys():
				if row[match] != None and row[match] != "nan":
					if (type(row[match]).__name__) != "str" and row[match] != None:
						if (type(row[match]).__name__) == "float":
							row[match] = repr(row[match])
						else:
							row[match] = str(row[match])
					else:
						if re.match(r'^-?\d+(?:\.\d+)$', row[match]) is not None:
							row[match] = repr(float(row[match]))
					if isinstance(row[match],dict):
						print("The key " + match + " has a Json structure as a value.\n")
						print("The index needs to be indicated.\n")
						return None
					else:
						if re.search("^[\s|\t]*$", row[match]) is None:
							value = row[match]
							if "http" not in value and "http" in new_string[:start + offset_current_substitution]:
								value = encode_char(value)
							new_string = new_string[:start + offset_current_substitution] + value.strip() + new_string[ end + offset_current_substitution:]
							offset_current_substitution = offset_current_substitution + len(value) - (end - start)
							if "\\" in new_string:
								new_string = new_string.replace("\\", "")
								count = new_string.count("}")
								i = 0
								while i < count:
									new_string = "{" + new_string
									i += 1
								new_string = new_string.replace(" ", "")

						else:
							return None
				else:
					return None
			else:
				print('The attribute ' + match + ' is missing.')
				if ignore == "yes":
					return None
				print('Aborting...')
				sys.exit(1)
		elif pattern == ".+":
			match = reference_match.group(0)
			if "." in match:
				if match not in row.keys():
					temp_keys = match.split(".")
					match = temp_keys[len(temp_keys) - 1]
					for tp in temp_keys[:-1]:
						if tp in row:
							row = row[tp]
						else:
							return None
			if row == None:
				return None
			if match in row.keys():
				if (type(row[match]).__name__) != "str" and row[match] != None:
					if (type(row[match]).__name__) == "float":
						row[match] = repr(row[match])
					else:
						row[match] = str(row[match])
				if isinstance(row[match],dict):
					print("The key " + match + " has a Json structure as a value.\n")
					print("The index needs to be indicated.\n")
					return None
				else:
					if row[match] != None and row[match] != "nan":
						if re.search("^[\s|\t]*$", row[match]) is None:
							new_string = new_string[:start] + row[match].strip().replace("\"", "'") + new_string[end:]
							new_string = "\"" + new_string + "\"" if new_string[0] != "\"" and new_string[-1] != "\"" else new_string
						else:
							return None
					else:
						return None
			else:
				print('The attribute ' + match + ' is missing.')
				if ignore == "yes":
					return None
				print('Aborting...')
				sys.exit(1)
		else:
			print("Invalid pattern")
			if ignore == "yes":
				return None
			print("Aborting...")
			sys.exit(1)
	return new_string

def string_substitution_array(string, pattern, row, row_headers, term, ignore):

	"""
	(Private function, not accessible from outside this package)
	Takes a string and a pattern, matches the pattern against the string and perform the substitution
	in the string from the respective value in the row.
	Parameters
	----------
	string : string
		String to be matched
	triples_map_list : string
		Pattern containing a regular expression to match
	row : dictionary
		Dictionary with CSV headers as keys and fields of the row as values
	Returns
	-------
	A string with the respective substitution if the element to be subtitued is not invalid
	(i.e.: empty string, string with just spaces, just tabs or a combination of both), otherwise
	returns None
	"""

	template_references = re.finditer(pattern, string)
	new_string = string
	offset_current_substitution = 0
	for reference_match in template_references:
		start, end = reference_match.span()[0], reference_match.span()[1]
		if pattern == "{(.+?)}":
			match = reference_match.group(1).split("[")[0]
			if "\\" in match:
				temp = match.split("{")
				match = temp[len(temp)-1]
			if match in row_headers:
				if row[row_headers.index(match)] != None:# or row[row_headers.index(match)] != "None":
					value = row[row_headers.index(match)]
					if (type(value).__name__) != "str":
						if (type(value).__name__) != "float":
							value = str(value)
						else:
							value = str(math.ceil(value))
					else:
						if re.match(r'^-?\d+(?:\.\d+)$', value) is not None:
							value = str(math.ceil(float(value)))
					if "b\'" == value[0:2] and "\'" == value[len(value)-1]:
						value = value.replace("b\'","")
						value = value.replace("\'","")
					if re.search("^[\s|\t]*$", value) is None:
						if "http" not in value:
							value = encode_char(value)
						new_string = new_string[:start + offset_current_substitution] + value.strip() + new_string[ end + offset_current_substitution:]
						offset_current_substitution = offset_current_substitution + len(value) - (end - start)
						if "\\" in new_string:
							new_string = new_string.replace("\\", "")
							count = new_string.count("}")
							i = 0
							new_string = " " + new_string
							while i < count:
								new_string = "{" + new_string
								i += 1
							#new_string = new_string.replace(" ", "")

					else:
						return None
				else:
						return None
			else:
				print('The attribute ' + match + ' is missing.')
				if ignore == "yes":
					return None
				print('Aborting...')
				sys.exit(1)
				return
				# To-do:
				# Generate blank node when subject in csv is not a valid string (empty string, just spaces, just tabs or a combination of the last two)
				#if term == "subject":
				#	new_string = new_string[:start + offset_current_substitution] + str(uuid.uuid4()) + new_string[end + offset_current_substitution:]
				#	offset_current_substitution = offset_current_substitution + len(row[match]) - (end - start)
				#else:
				#	return None
		elif pattern == ".+":
			match = reference_match.group(0)
			if match in row_headers:
				if row[row_headers.index(match)] is not None:
					value = row[row_headers.index(match)]
					if type(value).__name__ == "date":
						value = value.strftime("%Y-%m-%d")
					elif type(value).__name__ == "datetime":
						value = value.strftime("%Y-%m-%d T%H:%M:%S")
					elif type(value).__name__ != "str":
						value = str(value)
					if "b\'" == value[0:2] and "\'" == value[len(value)-1]:
						value = value.replace("b\'","")
						value = value.replace("\'","")
					if re.search("^[\s|\t]*$", str(value)) is None:
						new_string = new_string[:start] + str(value).strip().replace("\"", "'") + new_string[end:]
						new_string = "\"" + new_string + "\"" if new_string[0] != "\"" and new_string[-1] != "\"" else new_string
					else:
						return None
				else:
					return None
			else:
				print('The attribute ' + match + ' is missing.')
				if ignore == "yes":
					return None
				print('Aborting...')
				sys.exit(1)
				return
		else:
			print("Invalid pattern")
			if ignore == "yes":
				return None
			print("Aborting...")
			sys.exit(1)

	return new_string

def base36encode(number, alphabet='0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
	"""Converts an integer to a base36 string."""


	base36 = ''
	sign = ''

	if number < 0:
		sign = '-'
		number = -number

	if 0 <= number < len(alphabet):
		return sign + alphabet[number]

	while number != 0:
		number, i = divmod(number, len(alphabet))
		base36 = alphabet[i] + base36

	return sign + base36

def sublist(part_list, full_list):
	for part in part_list:
		if part not in full_list:
			return False
	return True

def child_list(childs):
	value = ""
	for child in childs:
		if child not in value:
			value += child + "_"
	return value[:-1]

def child_list_value(childs,row):
	value = ""
	v = []
	for child in childs:
		if child not in v:
			if row[child] != None:
				value += str(row[child]) + "_"
				v.append(child)
	return value[:-1]

def child_list_value_array(childs,row,row_headers):
	value = ""
	v = []
	for child in childs:
		if child not in v:
			if row[row_headers.index(child)] != None:
				value += row[row_headers.index(child)] + "_"
				v.append(child)
	return value[:-1]

def dic_builder(keys,values):
    dic = {}
    for key in keys:
        if "template" == key[1]:
            for string in key[0].split("{"):
                if "}" in string:
                    dic[string.split("}")[0]] = str(values[string.split("}")[0]])
        elif (key[1] != "constant") and ("reference function" not in key[1]):
            dic[key[0]] = values[key[0]]
    return dic

def string_separetion(string):
	if ("{" in string) and ("[" in string):
		prefix = string.split("{")[0]
		condition = string.split("{")[1].split("}")[0]
		postfix = string.split("{")[1].split("}")[1]
		field = prefix + "*" + postfix
	elif "[" in string:
		return string, string
	else:
		return string, ""
	return string, condition

def condition_separetor(string):
	condition_field = string.split("[")
	field = condition_field[1][:len(condition_field[1])-1].split("=")[0]
	value = condition_field[1][:len(condition_field[1])-1].split("=")[1]
	return field, value