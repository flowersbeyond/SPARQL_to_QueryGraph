import json
from tqdm import tqdm
from Sparql_Utils import append_missing_prefix
import copy
import re
import subprocess
from subprocess import CalledProcessError

class ErrorType:
    VARIABLE_EXPRESSION = 'VARIABLE_EXPRESSION'
    MORE_THAN_TWO_VARIABLES = 'MORE_THAN_TWO_VARIABLES'
    UNION_STATEMENT = 'UNION_STATEMENT'
    VALUES_STATEMENT = 'VALUES_STATEMENT'
    PREDICATE_VARIABLE = 'PREDICATE_VARIABLE'
    OPTIONAL_STATEMENT = 'OPTIONAL_STATEMENT'
    ASK_TWO_TRIPLE_NO_VARIABLE = 'ASK_TWO_TRIPLE_NO_VARIABLE'
    CC_NOT_FOUND = 'CC_NOT_FOUND'

def format_query(query):
    formatted_query = query
    pattern = re.compile(r'\s+')
    formatted_query = re.sub(pattern, ' ', formatted_query)
    formatted_query = append_missing_prefix(formatted_query)

    # queries with COUNT/DISTINCT ... structure wrapped around the SELECT variable needs to be transformed.

    replace_map = {'SELECT COUNT(DISTINCT ?uri)':'SELECT ?uri', 'SELECT COUNT(DISTINCT ?x)': 'SELECT ?x',
                  'SELECT DISTINCT COUNT(?uri)': 'SELECT ?uri', 'SELECT COUNT(DISTINCT ?uri AS ?uri)': 'SELECT ?uri',
                  'SELECT COUNT(DISTINCT ?y)':'SELECT ?y',
                   'SELECT Count(?sub) as ?c': 'SELECT ?sub',
                   'SELECT DISTINCT xsd:date(?num)': 'SELECT ?num',
                    'SELECT (COUNT(?Awards) AS ?Counter)':'SELECT ?Awards',
                    'SELECT (COUNT(DISTINCT ?uri) as ?c)': 'SELECT ?uri',
                    'SELECT (COUNT(DISTINCT ?uri) AS ?count)':'SELECT ?uri',
                    'SELECT (COUNT(?uri) AS ?count)':'SELECT ?uri',
                    'SELECT DISTINCT (( xsd:double(?radius) * 2 ) AS ?diameter)': 'SELECT ?radius'
                   }
    for key in replace_map:
        if formatted_query.find(key) >= 0:
            formatted_query = formatted_query.replace(key, replace_map[key])

    return formatted_query


def sparql_2_json(data_file, sparql_json_graph_file, failing_cases_file = None):
    queries = {}
    with open(data_file, encoding='utf-8') as fin:
        questions = json.load(fin)
        for q in questions:
            id = q['id']
            query = q['query']['sparql']
            queries[id] = {'query':query}

    success_count = 0
    failing_count = 0
    failing_cases = {}
    print('Start transforming SPARQL to JSON')

    pbar = tqdm(queries)
    for id in pbar:
        query = queries[id]['query']
        formatted_query = format_query(query)

        SPARQL_buffer_file = 'SPARQL_buffer%s.sparql' % str(id)
        with open(SPARQL_buffer_file, encoding='utf-8', mode='w') as tempf:
            tempf.write(formatted_query)

        batcmd = 'node code/SPARQL.js/bin/sparql-to-json %s' % SPARQL_buffer_file
        try:
            result = subprocess.check_output(batcmd)
            success_count += 1
            result = result.decode('utf-8')
            parsed_structure = json.loads(result)
            queries[id]['json'] = parsed_structure
            queries[id]['formatted'] = formatted_query

        except CalledProcessError as e:
            failing_count += 1
            failing_cases[id] = {'query':query, 'formatted':formatted_query}

    with open(sparql_json_graph_file, encoding='utf-8', mode='w') as fout:
        json.dump(queries, fout, indent=2)

    if failing_cases_file != None:
        with open(failing_cases_file, encoding='utf-8', mode='w') as ferr:
            json.dump(failing_cases, ferr, indent=2)

    print('sparql parse %d succeeded, %d failed' %(success_count, failing_count))


def extract_core_chain(data_set_name, data_file, succeeded_cc_output, cc_with_constraint_variable_output, failing_cases_file):

    print('Name:\t%s' % data_set_name)
    data = {}
    with open(data_file, encoding='utf-8') as fin:
        data = json.load(fin)

    failing_cases = {}
    with_constraint_variable_cases = {}
    succeeded_cases = {}

    #pbar = tqdm(data)
    for id in data:
        item = data[id]
        parse_results = parse_json_graph(item['json'])
        if 'err' in parse_results:
            item['err'] = parse_results['err']
            failing_cases[id] = item
        else:

            if 'all_good' in parse_results:
                item['core_chain'] = parse_results['all_good']
                succeeded_cases[id] = item

            elif 'var_in_constraint' in parse_results:
                item['core_chain'] = parse_results['var_in_constraint']
                with_constraint_variable_cases[id] = item
            else:
                item['err'] = ErrorType.CC_NOT_FOUND
                failing_cases[id] = item

    print('\nSuccess\t%d\nVariable Constraint\t%d\nFail\t%d'
          % (len(succeeded_cases), len(with_constraint_variable_cases), len(failing_cases)))

    failing_stat = {ErrorType.VARIABLE_EXPRESSION:[], ErrorType.MORE_THAN_TWO_VARIABLES:[], ErrorType.UNION_STATEMENT:[],
                    ErrorType.VALUES_STATEMENT:[], ErrorType.OPTIONAL_STATEMENT:[], ErrorType.PREDICATE_VARIABLE:[],
                    ErrorType.ASK_TWO_TRIPLE_NO_VARIABLE:[], ErrorType.CC_NOT_FOUND:[]}
    for id in failing_cases:
        item = failing_cases[id]
        failing_stat[item['err']].append(id)

    for key in failing_stat:
        if len(failing_stat[key]) > 0:
            print('%s\t%d' % (key, len(failing_stat[key])))

    for key in failing_stat:
        if len(failing_stat[key]) > 0:
            print(key + ':' + str(failing_stat[key]))



    with open(succeeded_cc_output, encoding='utf-8', mode='w') as fout:
        succeeded_cases.update(with_constraint_variable_cases)
        json.dump(succeeded_cases, fout, indent=4)
    with open(cc_with_constraint_variable_output, encoding='utf-8', mode='w') as fout:
        json.dump(with_constraint_variable_cases, fout, indent=4)
    with open(failing_cases_file, encoding='utf-8', mode='w') as fout:
        json.dump(failing_cases, fout, indent=4)

def parse_target_variables(variables):
    var_names = []
    for var in variables:
        if 'expression' in var:
            return {'err':ErrorType.VARIABLE_EXPRESSION}
        var_names.append(var['value'])
    return {'target_variables':var_names}
def parse_triple_term(term):
    if term['termType'] == 'Variable':
        return '?' + term['value']
    elif term['termType'] == 'Literal':
        return term['value']
    elif term['termType'] == 'NamedNode':
        return '<' + term['value'] + '>'
    else:
        assert False

def parse_bgp(bgp_clause):
    triples = []
    for triple in bgp_clause:
        subj = parse_triple_term(triple['subject'])
        pred = parse_triple_term(triple['predicate'])
        obj = parse_triple_term(triple['object'])
        triples.append([subj, pred, obj])
    return triples

def parse_bgp_variables(bgp_clause):
    variables = []
    for triple in bgp_clause:
        for key in triple:
            if key not in ['subject', 'predicate', 'object']:
                assert False
            if triple[key]['termType'] == 'Variable':
                variables.append(triple[key]['value'])
            elif triple[key]['termType'] not in ['Literal', 'NamedNode']:
                assert False

    return variables

def parse_operation_args(args):
    variable_names = []
    for arg in args:
        if isinstance(arg, dict):
            if 'type' in arg:
                if arg['type'] == 'operation' or arg['type'] == 'functionCall':
                    variable_names.extend(parse_operation_args(arg['args']))
                elif arg['type'] == 'bgp':
                    variable_names.extend(parse_bgp_variables(arg['triples']))
                elif arg['type'] == 'aggregate':
                    variable_names.extend(parse_expression_variables(arg['expression']))
                else:
                    assert False
            else:
                if arg['termType'] == 'Variable':
                    variable_names.append(arg['value'])
                elif arg['termType'] not in ['Literal', 'NamedNode']:
                    assert False
        elif isinstance(arg, list):
            for item in arg:
                if item['termType'] == 'Variable':
                    variable_names.append(item['value'])
                elif item['termType'] not in ['Literal', 'NamedNode']:
                    assert False
        else:
            assert False
    return variable_names

def parse_filter(filter_clause):
    return parse_operation_args(filter_clause['args'])

def parse_where(where_clause):
    triples = []
    filter_variables = []
    for clause in where_clause:
        if clause['type'] == 'bgp':
            triples = parse_bgp(clause['triples'])
        elif clause['type'] == 'filter':
            filter_variables = parse_filter(clause['expression'])
        elif clause['type'] == 'union':
            return {'err': ErrorType.UNION_STATEMENT}
        elif clause['type'] == 'values':
            return {'err': ErrorType.VALUES_STATEMENT}
        elif clause['type'] == 'optional':
            return {'err': ErrorType.OPTIONAL_STATEMENT}
            assert False

    return {'triples':triples, 'filter_variables':filter_variables}

def parse_expression_variables(expression):

    variables = []

    if isinstance(expression, dict):
        if 'termType' in expression:
            if expression['termType'] == 'Variable':
                variables.append(expression['value'])
            else:
                assert False
        elif 'expression' in expression:
            variables.extend(parse_expression_variables(expression['expression']))
        else:
            assert False
    else:
        assert False
    return variables

def parse_constraint_variables(constraint_clause):
    variables = []
    if isinstance(constraint_clause, list):
        for clause in constraint_clause:
            if 'expression' in clause:
                term = clause['expression']
                variables.extend(parse_expression_variables(term))
            elif clause['type'] == 'operation':
                variables.extend(parse_operation_args(clause['args']))
            else:
                assert False
    else:
        assert False

    return variables

def gen_core_chain(target_variable, triples, constraint_variables):
    subj_map = {}
    obj_map = {}
    all_variables = set()
    for triple in triples:
        subj = triple[0]
        pred = triple[1]
        obj = triple[2]
        if subj.startswith('?'):
            all_variables.add(subj)
        if pred.startswith('?'):
            return {'err':ErrorType.PREDICATE_VARIABLE}
        if obj.startswith('?'):
            all_variables.add(obj)

        if subj not in subj_map:
            subj_map[subj] = {}
        if pred not in subj_map[subj]:
            subj_map[subj][pred] = set()
        subj_map[subj][pred].add(obj)

        if obj not in obj_map:
            obj_map[obj] = {}
        if pred not in obj_map[obj]:
            obj_map[obj][pred] = set()
        obj_map[obj][pred].add(subj)

    all_valid_paths = {}
    current_path = []
    visited = set()
    gen_core_chain_inner(target_variable, subj_map, obj_map, all_variables, constraint_variables, current_path, visited,all_valid_paths)

    return all_valid_paths

def gen_core_chain_inner(start_node, subj_map, obj_map, all_variables, constraint_variables, current_path, visited, all_valid_paths):
    if start_node.startswith('<') and start_node.endswith('>'):
        # reached a topic entity, check if the path fits:
        var_constraint_map = {}
        for var in all_variables:
            if var not in visited:
                for constraint_type in constraint_variables:
                    if var in constraint_variables[constraint_type]:
                        var_constraint_map[var] = constraint_type
        for var in all_variables:
            if var not in visited and var not in var_constraint_map:
                return
        final_path = copy.deepcopy(current_path)
        final_path.reverse()
        if len(var_constraint_map) != 0:
            if 'var_in_constraint' not in all_valid_paths:
                all_valid_paths['var_in_constraint'] = []
            all_valid_paths['var_in_constraint'].append({'path':final_path, 'var_constraint_map':var_constraint_map})
            return
        else:
            if 'all_good' not in all_valid_paths:
                all_valid_paths['all_good'] = []
            all_valid_paths['all_good'].append(final_path)
            return
    elif not start_node.startswith('?'):
        return
    else:
        if start_node in visited:
            return

        visited.add(start_node)
        if start_node in subj_map:
            for pred in subj_map[start_node]:
                for obj in subj_map[start_node][pred]:
                    current_path.append([start_node,pred,obj])
                    gen_core_chain_inner(obj, subj_map, obj_map, all_variables, constraint_variables, current_path, visited,
                                         all_valid_paths)
                    current_path = current_path[0:-1]

        if start_node in obj_map:
            for pred in obj_map[start_node]:
                for subj in obj_map[start_node][pred]:
                    current_path.append([subj, pred, start_node])
                    gen_core_chain_inner(subj, subj_map, obj_map, all_variables, constraint_variables, current_path, visited,
                                         all_valid_paths)
                    current_path = current_path[0:-1]
            visited.remove(start_node)

        return




def parse_json_graph(graph):
    query_type = graph['queryType']
    if query_type == 'SELECT':
        parse_result = parse_target_variables(graph['variables'])
        if isinstance(parse_result,dict) and 'err' in parse_result:
            return parse_result
        target_variables = parse_result['target_variables']

        if len(target_variables) >= 2:
            return {'err':ErrorType.MORE_THAN_TWO_VARIABLES}

        parse_result = parse_where(graph['where'])
        
        if 'err' in parse_result:
            return parse_result
        else:
            triples = parse_result['triples']
            filter_variables = set(parse_result['filter_variables'])
        
            variables_in_constraints = {}
            variables_in_constraints['filter'] = set(['?' + name for name in filter_variables])
            
            constraint_keys = ['group', 'having', 'order']
            for key in constraint_keys:
                if key in graph:
                    variables = parse_constraint_variables(graph[key])
                    variables_in_constraints[key] = set(['?' + name for name in variables])


            core_chains = gen_core_chain('?' + target_variables[0], triples, variables_in_constraints)
        
            return core_chains
    elif query_type == 'ASK':
        parse_result = parse_where(graph['where'])

        if 'err' in parse_result:
            return parse_result
        else:
            triples = parse_result['triples']
            filter_variables = set(parse_result['filter_variables'])

            variables_in_constraints = {}
            variables_in_constraints['filter'] = set(['?' + name for name in filter_variables])

            constraint_keys = ['group', 'having', 'order']
            for key in constraint_keys:
                if key in graph:
                    variables = parse_constraint_variables(graph[key])
                    variables_in_constraints[key] = set(['?' + name for name in variables])

            triple_vars = set()
            for triple in triples:
                for item in triple:
                    if item.startswith('?'):
                        triple_vars.add(item)

            if len(triple_vars) != 0:
                possible_core_chains = {'all_good':[], 'var_in_constraint':[]}
                for var in triple_vars:
                    core_chain_i = gen_core_chain(var, triples, variables_in_constraints)
                    if 'all_good' in core_chain_i:
                        possible_core_chains['all_good'].extend(core_chain_i['all_good'])
                    if 'var_in_constraint' in core_chain_i:
                        possible_core_chains['var_in_constraint'].extend(core_chain_i['var_in_constraint'])
                return possible_core_chains


            elif len(triples) == 1:
                triple = triples[0]
                core_chains1 = gen_core_chain('?uri', [['?uri', triple[1], triple[2]]], {})
                core_chains2 = gen_core_chain('?uri', [[triple[0], triple[1], '?uri']], {})
                assert 'var_in_constraint' not in core_chains1
                assert 'var_in_constraint' not in core_chains2

                core_chain = {'all_good':[], 'var_in_constraint':[]}
                if 'all_good' in core_chains1:
                    core_chain['all_good'] = core_chains1['all_good']
                if 'all_good' in core_chains2:
                    core_chain['all_good'].extend(core_chains2['all_good'])
                if len(core_chain['all_good']) == 0:
                    return {}
                return core_chain
            else:
                assert False
                return {'err': ErrorType.ASK_TWO_TRIPLE_NO_VARIABLE}
    else:
        assert False
    return {}


if __name__=="__main__":
    configs = ['train', 'test']
    for config in configs:
        data_path = './data/QALD/%s-multilingual-merged.json' % config
        sparql_json_graph_file = './data/core_chain/cc_extraction/%s-sparql_2_json.json' % config
        cc_file = './data/core_chain/cc_extraction/%s-core_chain.json' % config
        failing_cases_file = './data/core_chain/cc_extraction/%s-extract_cc_failure.json' % config
        with_2hop_variable_constraint_file = './data/core_chain/cc_extraction/%s-extract_cc_2hop_constraints.json' % config

        #sparql_2_json(data_path, sparql_json_graph_file)
        extract_core_chain(config, sparql_json_graph_file, cc_file, with_2hop_variable_constraint_file, failing_cases_file)