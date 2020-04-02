import json
import progressbar
import copy
import os
def read_SPARQL(data_path, do_parsing):
    data = []
    if do_parsing == True:
        with open(data_path, encoding='utf-8') as fin:
            for l in fin:
                data.append(json.loads(l))### data为list->dict类型
            #print('type(data): ', type(data))
        '''
        ### WebQSP
        new_data = []
        print('Start processing dataset')
        progress_bar = progressbar.ProgressBar(max_value=len(data['Questions']))
        new_data_temp = {}
        for i, Questions_temp in enumerate(data['Questions']):
            for j, Parses_temp in enumerate(Questions_temp['Parses']):
                #print(i)
                ### Parses_temp['Sparql'] 可能会是 null，json的null 对应 python的None
                ### 跳过Parses_temp['Sparql']值为 null 的Question
                if Parses_temp['Sparql'] == None:
                    #print('%s Parses_temp[\'Sparql\']: ' % (Questions_temp['QuestionId']), Parses_temp['Sparql'])
                    continue

                new_data_temp.update({'question': Questions_temp['RawQuestion']})
                new_data_temp.update({'sparql': Parses_temp['Sparql']})
                new_data_temp.update({'PotentialTopicEntityMention': Parses_temp['PotentialTopicEntityMention']})
                new_data_temp.update({'TopicEntityName': Parses_temp['TopicEntityName']})
                new_data_temp.update({'TopicEntityMid': Parses_temp['TopicEntityMid']})
                new_data_temp.update({'InferentialChain': Parses_temp['InferentialChain']})
                new_data_temp.update({'Constraints': Parses_temp['Constraints']})
                new_data.append(copy.deepcopy(new_data_temp))
                ### copy.deepcopy()的目的是保存深复制的版本，否则下一次迭代修改temp dict的字段时，会同时修改已保存的数据
            
            progress_bar.update(i + 1)
        data = copy.deepcopy(new_data)
        '''

        '''
        ### 测试Questions_temp['Parses']的长度
            if len(Questions_temp['Parses']) != 1:
                print('%s len(Questions_temp[\'Parses\']) != 1' % (Questions_temp['QuestionId']))
                exit(0)
        print('\n')
        print('ALL len(Questions_temp) == 1')
        exit(0)
        '''
        '''
        print('\n')
        '''

        ### 保存数据
        f = open('data_in_SPARQL.json', 'w')
        json.dump(obj=data, fp=f, indent=4)
        f.close()

    else:
        ### 读取数据
        f = open('data_in_SPARQL.json', 'r')
        data = json.load(f) ### data为list->dict类型
        f.close()

    return data

def SPARQL_to_JSON(data, do_parsing):
    if do_parsing == True:
        ### 默认SPARQL全部有值
        number_parsable = 0
        number_unparsable = 0
        print('Start transforming SPARQL to JSON')
        progress_bar = progressbar.ProgressBar(maxval=len(data))
        progress_bar.start()
        for i in range(len(data)):
            '''
            ### WebQSP
            ### 处理WebQSP的SPARQL缺陷
            SPARQL_temp = copy.deepcopy(data[i]['sparql'])
            ### 无法解析 'OR' 运算符
            ### 已解决， 用 ' || ' 替换 ' OR '
            SPARQL_temp = SPARQL_temp.replace(' OR ', ' || ')
            ### prefix xsd未定义
            ### 已解决，如果SPARQL中有'xsd:'，那么在SPARQL开头加'PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n'
            if SPARQL_temp.find('xsd:') != -1:
                SPARQL_temp = 'PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n' + SPARQL_temp
            ### 无法解析 'GROUP BY ?city\nHaving COUNT(?city) = 2'
            ### 已解决 'GROUP BY ?city\nHaving (COUNT(?city) = 2)'
            SPARQL_temp = SPARQL_temp.replace('GROUP BY ?city\nHaving COUNT(?city) = 2', 'GROUP BY ?city\nHaving (COUNT(?city) = 2)')
            '''
            ### QALD
            SPARQL_temp = copy.deepcopy(data[i]['query']['sparql'])
            ### 处理WebQSP的SPARQL缺陷
            ### 无法解析 'SELECT COUNT(DISTINCT ?uri)'
            ### SELECT COUNT(DISTINCT ?uri)
            ### ----------------------^
            ### Expecting '*', 'VAR', '(', 'DISTINCT', 'REDUCED', got 'COUNT'
            ### 未解决
            ### prefix xsd未定义
            ### 已解决，如果SPARQL中有'xsd:'，那么在SPARQL开头加'PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n'
            if SPARQL_temp.find('xsd:') != -1:
                SPARQL_temp = 'PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n' + SPARQL_temp
            ### prefix rdf未定义
            ### 已解决，如果SPARQL中有'rdf:'，那么在SPARQL开头加'PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n'
            if SPARQL_temp.find('rdf:') != -1:
                SPARQL_temp = 'PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n' + SPARQL_temp
            ### prefix foaf未定义
            ### 已解决，如果SPARQL中有'foaf:'，那么在SPARQL开头加'PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n'
            if SPARQL_temp.find('foaf:') != -1:
                SPARQL_temp = 'PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n' + SPARQL_temp
            ### prefix dbo未定义
            ### 已解决，如果SPARQL中有'dbo:'，那么在SPARQL开头加'PREFIX dbo: <http://dbpedia.org/ontology/>\n'
            if SPARQL_temp.find('dbo:') != -1:
                SPARQL_temp = 'PREFIX dbo: <http://dbpedia.org/ontology/>\n' + SPARQL_temp    

            SPARQL_buffer = open('SPARQL_buffer.sparql', 'w')
            SPARQL_buffer.write(SPARQL_temp)
            SPARQL_buffer.close()

            f = open('mutex.txt', 'w')
            f.write('0')
            f.close()
            os.system('node SPARQL.js-master/bin/SPARQL_to_JSON.js')
            ### 当互斥量为'1'时，说明写入完毕
            while True:
                f = open('mutex.txt', 'r')
                mutex = f.read()
                f.close()
                if mutex == '1':
                    break

            f = open('JSON_buffer.json', 'r')
            JSON = json.load(f)
            data[i].update({'json': copy.deepcopy(JSON)}) ### json.load()返回dict类型
            if JSON != None:
                number_parsable += 1
            else:
                number_unparsable += 1

            progress_bar.update(i + 1)
        print('\n')
        print('The number of parsable SPARQL: ', number_parsable)
        print('The number of unparsable SPARQL: ', number_unparsable)

        ### 保存数据
        f = open('data_in_JSON.json', 'w')
        json.dump(obj=data, fp=f, indent=4)
        f.close()

    else:
        ### 读取数据
        f = open('data_in_JSON.json', 'r')
        data = json.load(f)   
        f.close()

    return data

def JSON_to_QueryGraph(data, do_parsing):
    if do_parsing == True:
        print('Start transforming JSON to Query Graph')
        number_None = 0
        number_unparsable = 0
        number_no_topic_entity_and_no_core_inferential_chain = 0
        number_no_constraints = 0
        number_all_pass = 0
        progress_bar = progressbar.ProgressBar(maxval=len(data))
        progress_bar.start()
        for i, data_temp in enumerate(data):
            ### data_temp是引用
            object_JSON_to_QueryGraph = class_JSON_to_QueryGraph()
            query_graph_temp = None
            topic_entity_temp = None
            core_inferential_chain_temp = None
            all_core_chains = None
            constraints_temp = None
            if data_temp['json'] == None: ### 原SPARQL无法解析成JSON
                print("\"id\": %d, 原SPARQL无法解析成JSON\n" % (data_temp['id']))
                number_None += 1
            elif object_JSON_to_QueryGraph.generate_graph(data_temp['json']) == False: ### JSON无法解析成Query Graph
                print("\"id\": %d, JSON无法解析成Query Graph\n" % (data_temp['id']))
                number_unparsable += 1
                ###  无法解析可能的原因：不等式的操作数还是表达式；functionCall无法处理
            elif object_JSON_to_QueryGraph.get_topic_entity() == None: ### Query Graph无法提取topic entity和core inference chain
                print("\"id\": %d, Query Graph无法提取topic entity和core inference chain\n" % (data_temp['id']))
                query_graph_temp = copy.deepcopy(object_JSON_to_QueryGraph.graph)
                number_no_topic_entity_and_no_core_inferential_chain += 1
            elif object_JSON_to_QueryGraph.get_constraints() == None: ### Query Graph无法提取constraints
                print("\"id\": %d, Query Graph无法提取constraints\n" % (data_temp['id']))
                query_graph_temp = copy.deepcopy(object_JSON_to_QueryGraph.graph)
                topic_entity_temp = copy.deepcopy(object_JSON_to_QueryGraph.get_topic_entity())
                core_inferential_chain_temp = copy.deepcopy(object_JSON_to_QueryGraph.get_core_inferential_chain())
                all_core_chains = copy.deepcopy(object_JSON_to_QueryGraph.get_all_core_chains())
                number_no_constraints += 1
            else:
                #print("\"id\": %d, 解析成功\n" % (data_temp['id']))
                query_graph_temp = copy.deepcopy(object_JSON_to_QueryGraph.graph)
                topic_entity_temp = copy.deepcopy(object_JSON_to_QueryGraph.get_topic_entity())
                core_inferential_chain_temp = copy.deepcopy(object_JSON_to_QueryGraph.get_core_inferential_chain())
                all_core_chains = copy.deepcopy(object_JSON_to_QueryGraph.get_all_core_chains())
                constraints_temp = copy.deepcopy(object_JSON_to_QueryGraph.get_constraints())
                number_all_pass += 1

            data_temp.update({'graph': query_graph_temp})
            data_temp.update({'topic_entity': topic_entity_temp})
            data_temp.update({'core_inferential_chain': core_inferential_chain_temp})
            data_temp.update({'core_chain_list': all_core_chains})
            data_temp.update({'constraints': constraints_temp})

            progress_bar.update(i + 1)

        print('\n')
        print('The number of None JSON: ', number_None)
        print('The number of unparsable JSON: ', number_unparsable)
        print('The number of no_topic_entity_and_no_core_inferential_chain: ', number_no_topic_entity_and_no_core_inferential_chain)
        print('The number of no_constraints: ', number_no_constraints)
        print('The number of all_pass: ', number_all_pass)

        ### 保存数据
        f = open('data_in_QueryGraph.json', 'w')
        json.dump(obj=data, fp=f, indent=4)
        f.close()

    else:
        ### 读取数据
        f = open('data_in_QueryGraph.json', 'r')
        data = json.load(f) 
        f.close()

    return data

### 充分利用python语言在 函数传参 , = , for temp in data 中是传引用而不是传值，
### 因此修改被字典或列表赋值的变量，就修改了原始的字典或列表
class class_JSON_to_QueryGraph:
    def __init__(self):
        self.init()

    def init(self):
        self.graph = None
        self.direction = ['out', 'in']
        self.and_not = ['and', 'not']
        self.topic_entity = None
        self.core_inferential_chain = None
        self.all_core_chains = None
        self.constraints = None

    ### generate_graph()返回True，则建立成功；generate_graph()返回False，则建立失败
    def generate_graph(self, data):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)

        ### 队列用于处理或逻辑运算符，算法基于BFS
        ### 队列元素为字典型，
        ### queue_temp['data']是JSON字典数据，用于当前分支节点的生成，
        ### queue_temp['graph']是空字典，用于存放当前分支节点的'and': {}, 'not': {}, 'or': []
        self.graph = {} ### 初始化 self.graph 为空字典
        self.queue = []
        self.queue.append({'data': data, 'graph': self.graph, 'and_not_index': 0})
        while len(self.queue) > 0:
            ### 出队
            queue_temp = self.queue[0]
            del self.queue[0]

            queue_temp['graph'].update({'queryType': None})
            queue_temp['graph'].update({'variables': None})          
            queue_temp['graph'].update({'and': {}})
            queue_temp['graph'].update({'not': {}})
            queue_temp['graph'].update({'or': []})
            queue_temp['graph'].update({'order_limit_offset': None})
            queue_temp['graph'].update({'group_having': None})  

            if self.all_type(queue_temp['data'], queue_temp['graph'], queue_temp['and_not_index']) == False:
                self.graph = None
                return False

        return True

    def all_type(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)

        if data['type'] == 'query':
            return self.query_type(data, graph, and_not_index)
        elif data['type'] == 'bgp':
            return self.bgp_type(data, graph, and_not_index)
        elif data['type'] == 'filter':
            return self.filter_type(data, graph, and_not_index)
        elif data['type'] == 'operation':
            return self.operation_type(data, graph, and_not_index)
        elif data['type'] == 'union':
            return self.union_type(data, graph, and_not_index)
        elif data['type'] == 'optional':
            return self.optional_type(data, graph, and_not_index)
        elif data['type'] == 'empty':
            return True
        elif data['type'] == 'group':
            return self.group_type(data, graph, and_not_index)
        else: ### 其他未知情况
            print('未知data[\'type\']')
            return False

    def query_type(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)

        ### QALD 中 data['queryType'] 为 'SELECT' 或 'ASK'
        graph['queryType'] = copy.deepcopy(data['queryType'])
        if data['queryType'] == 'SELECT':
            graph['variables'] = []
            for variables_temp in data['variables']:
                ### 不处理SELECT查询变量形如 SELECT (( year(xsd:date(?end)) - year(xsd:date(?start)) ) AS ?years) 的SPARQL
                if 'expression' in variables_temp:
                    print('SELECT查询变量中包含expression')
                    return False
                graph['variables'].append(copy.deepcopy(variables_temp))
        ### 默认'where'一定存在
        for where_temp in data['where']:
            if self.all_type(where_temp, graph, and_not_index) == False:
                return False
        if 'order' in data:
            if self.order_limit_offset(data, graph, and_not_index) == False:
                return False
        if ('group' in data) and ('having' in data):
            ### 聚合语句 GROUP BY ... HAVING ... 中，'group' 和 'having' 一定成对出现
            if self.group_having(data, graph, and_not_index) == False:
                return False

        return True

    def bgp_type(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)

        for triples_temp in data['triples']:
            ### 'Literal'类型不能作顶点
            if triples_temp['subject']['termType'] != 'Literal':
                ### 把三元组的主语、谓语添加到graph[self.and_not[and_not_index]]键中
                if triples_temp['subject']['value'] not in graph[self.and_not[and_not_index]]:
                    graph[self.and_not[and_not_index]].update({copy.deepcopy(triples_temp['subject']['value']): {
                        'termType': copy.deepcopy(triples_temp['subject']['termType']),
                        'edge': []
                    }})
                ### 把三元组添加到graph[self.and_not[and_not_index]]值中
                graph[self.and_not[and_not_index]][triples_temp['subject']['value']]['edge'].append({
                    'type': 'triple', ### 边的类型
                    'role': 'subject', ### 当前顶点的类型
                    'predicate': copy.deepcopy(triples_temp['predicate']), ### 谓语
                    'endpoint': copy.deepcopy(triples_temp['object']) ### 边的另一端顶点
                })

            if triples_temp['object']['termType'] != 'Literal':
                if triples_temp['object']['value'] not in graph[self.and_not[and_not_index]]:
                    graph[self.and_not[and_not_index]].update({copy.deepcopy(triples_temp['object']['value']): {
                        'termType': copy.deepcopy(triples_temp['object']['termType']),
                        'edge': []
                    }})
                graph[self.and_not[and_not_index]][triples_temp['object']['value']]['edge'].append({
                    'type': 'triple',
                    'role': 'object',
                    'predicate': copy.deepcopy(triples_temp['predicate']),
                    'endpoint': copy.deepcopy(triples_temp['subject'])
                })

        return True

    def filter_type(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)

        if data['expression']['type'] == 'operation':
            return self.operation_type(data['expression'], graph, and_not_index)
        
        ### 未知错误
        print('未知data[\'expression\'][\'type\']')
        return False

    def operation_type(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)

        ### 运算符 '<', '>', '>=', '<=', '=', '!=' 都是2操作数，非逻辑运算符
        if (data['operator'] == '<') or (data['operator'] == '>') or (data['operator'] == '>=') or \
            (data['operator'] == '<=') or (data['operator'] == '=') or (data['operator'] == '!='):
            number_args = 2
            for i in range(number_args):
                ### 如果操作数有一方不是单一节点，该SPARQL不处理
                ### 形如 FILTER(xsd:dateTime(?pFrom) - xsd:dateTime(?from) > 0)
                ### 形如 FILTER (xsd:dateTime(?d) - xsd:dateTime(?from) >= 0)
                if 'termType' not in data['args'][i]:
                    print('不等式操作数有expression')
                    return False
    
            for i in range(number_args):
                if data['args'][i]['termType'] != 'Literal':
                    ### 把二元不等式的2个操作数添加到graph[self.and_not[and_not_index]]键中
                    if data['args'][i]['value'] not in graph[self.and_not[and_not_index]]:
                        graph[self.and_not[and_not_index]].update({copy.deepcopy(data['args'][i]['value']): {
                            'termType': copy.deepcopy(data['args'][i]['termType']),
                            'edge': []
                        }})

                    graph[self.and_not[and_not_index]][data['args'][i]['value']]['edge'].append({
                        'type': 'operation',
                        'direction': self.direction[i], ### 边的方向，当前顶点是左操作数，'direction': 'out'；当前顶点是右操作数，'direction': 'in'
                        'operator': data['operator'], ### 运算符
                        'endpoint': copy.deepcopy(data['args'][self.reverse(i)])
                    })

            return True

        ### 算术运算符 '+', '-', '*', '/' 都是2操作数
        ### QALD的算术运算符只出现在'type': 'query'的'variables':[]

        ### 正则表达式 'regex' 都是2操作数，第1个操作数为 "termType": "Variable" ，第2个操作数为 "termType": "Literal"
        elif data['operator'] == 'regex':
            if data['args'][0]['termType'] != 'Literal':

                ### 把正则表达式的第1个操作数添加到graph[self.and_not[and_not_index]]键中
                if data['args'][0]['value'] not in graph[self.and_not[and_not_index]]:
                    graph[self.and_not[and_not_index]].update({copy.deepcopy(data['args'][0]['value']): {
                        'termType': copy.deepcopy(data['args'][0]['termType']),
                        'edge': []
                    }})

                graph[self.and_not[and_not_index]][data['args'][0]['value']]['edge'].append({
                    'type': 'operation',
                    'direction': self.direction[0], ### 增加方向性，当前顶点是左操作数，'direction': 'out'；当前顶点是右操作数，'direction': 'in'
                    'operator': data['operator'],
                    'endpoint': copy.deepcopy(data['args'][1])
                })

            return True

        ### 且运算符，依次处理每一个操作数（表达式）即可
        elif data['operator'] == '&&':
            if self.and_not[and_not_index] == 'and':
                for args_temp in data['args']:
                    if self.all_type(args_temp, graph, and_not_index) == False:
                        return False
            else: ### self.and_not[and_not_index] == 'not'，非且 等于 或非，必须把非放到子式上！
                graph['or'].append([])
                for args_temp in data['args']:
                    graph['or'][-1].append({})
                    self.queue.append({'data': args_temp, 'graph': graph['or'][-1][-1], 'and_not_index': self.reverse(and_not_index)})
            
            return True

        ### 或运算符，
        ### 先在graph['or']中为这个或运算符添加一个空列表
        ### 为每一个操作数在graph['or'][-1]添加一个空字典，并把每一个操作数和空字典加入队列
        elif data['operator'] == '||':
            if self.and_not[and_not_index] == 'and':
                graph['or'].append([])
                for args_temp in data['args']:
                    graph['or'][-1].append({})
                    self.queue.append({'data': args_temp, 'graph': graph['or'][-1][-1], 'and_not_index': and_not_index})
            else: ### self.and_not[and_not_index] == 'not'，非或 等于 且非，必须把非放到子式上！
                for args_temp in data['args']:
                    if self.all_type(args_temp, graph, self.reverse(and_not_index)) == False:
                        return False

            return True

        ### 非运算符，都是1操作数
        elif data['operator'] == '!':
            return self.all_type(data['args'][0], graph, self.reverse(and_not_index))

        ### 非运算符，都是1操作数
        elif data['operator'] == 'exists':
            return self.all_type(data['args'][0], graph, and_not_index)

        ### 非运算符，都是1操作数
        elif data['operator'] == 'notexists':
            return self.all_type(data['args'][0], graph, self.reverse(and_not_index))
        ### 'notin', 'in'运算符先不考虑

        ### 无法处理操作符 bound()
        ### 未知错误
        else: 
            print('data[\'operator\']: ', data['operator'])
            print('未知data[\'operator\']')
            return False

    def reverse(self, x):
        return (x + 1) % 2

    ### 等价于 或逻辑
    def union_type(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)

        if self.and_not[and_not_index] == 'and':
            graph['or'].append([])
            for patterns_temp in data['patterns']:
                graph['or'][-1].append({})
                self.queue.append({'data': patterns_temp, 'graph': graph['or'][-1][-1], 'and_not_index': and_not_index})
        else: ### self.and_not[and_not_index] == 'not'，非或 等于 且非，必须把非放到子式上！
            for patterns_temp in data['patterns']:
                if self.all_type(patterns_temp, graph, self.reverse(and_not_index)) == False:
                    return False

        return True

    def optional_type(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)

        if self.and_not[and_not_index] == 'and':
            graph['or'].append([])
            ### 经验证，'optional' 的 'patterns' 一定只有1个元素
            graph['or'][-1].append({})
            self.queue.append({'data': data['patterns'][0], 'graph': graph['or'][-1][-1], 'and_not_index': and_not_index})
            ### 'optional'相当于， {'patterns'} 'union' {空'graph'}
            graph['or'][-1].append({})
            self.queue.append({'data': self.get_empty_data(), 'graph': graph['or'][-1][-1],  'and_not_index': and_not_index})
        else: ### self.and_not[and_not_index] == 'not'，非或 等于 且非，必须把非放到子式上！
            ### 经验证，'optional' 的 'patterns' 一定只有1个元素
            return self.all_type(data['patterns'][0], graph, self.reverse(and_not_index))
        
        return True

    def get_empty_data(self):
        return {'type': 'empty'}

    ### 等价于 且逻辑
    def group_type(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)

        if self.and_not[and_not_index] == 'and':
            for patterns_temp in data['patterns']:
                if self.all_type(patterns_temp, graph, and_not_index) == False:
                    return False
        else: ### self.and_not[and_not_index] == 'not'，非且 等于 或非，必须把非放到子式上！
            graph['or'].append([])
            for patterns_temp in data['patterns']:
                graph['or'][-1].append({})
                self.queue.append({'data': patterns_temp, 'graph': graph['or'][-1][-1], 'and_not_index': self.reverse(and_not_index)})

        return True

    def order_limit_offset(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)

        graph['order_limit_offset'] = {}
        ### 经验证，所有data['order']列表的长度均为1
        ### 如果data有键'order'，可能有键'limit'，也可能没有。可能有键'offset'，也可能没有
        graph['order_limit_offset'].update({'order': copy.deepcopy(data['order'])})
        if 'limit' in data:
            graph['order_limit_offset'].update({'limit': copy.deepcopy(data['limit'])})
        else:
            ### 如果没有指定 'limit'，默认结果的数量为1
            graph['order_limit_offset'].update({'limit': 1})
        if 'offset' in data:
            graph['order_limit_offset'].update({'offset': copy.deepcopy(data['offset'])})
        else:
            ### 如果没有指定 'offset'，默认从第0个数据开始
            graph['order_limit_offset'].update({'offset': 0})
        
        return True

    def group_having(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)

        graph['group_having'] = {}
        graph['group_having'].update({'group': copy.deepcopy(data['group'])})
        graph['group_having'].update({'having': copy.deepcopy(data['having'])})

        return True

    ### self.topic_entity 的类型是 str ， self.core_inferential_chain 的类型是 list -> str
    def set_topic_entity_and_core_inferential_chain(self, topic_entity, core_inferential_chain):
        self.topic_entity = topic_entity
        self.core_inferential_chain = core_inferential_chain

    def get_all_core_chains(self):
        return self.all_core_chains

    def get_topic_entity(self):
        if self.topic_entity == None:
            if self.graph == None:
                print('QueryGraph is None')
                return None
            
            ### 为了数据安全，做深复制
            graph_temp = copy.deepcopy(self.graph)

            ### 目前只考虑SELECT查询，
            if graph_temp['queryType'] != 'SELECT':
                print('graph_temp[\'queryType\'] == %s' % (graph_temp['queryType']))
                return None
            
            ### 目前只考虑有1个查询变量
            query_variables = graph_temp['variables'][0]['value']

            ### 目前只考虑在'and'逻辑内
            if query_variables not in graph_temp['and']:
                print('查询变量不在graph_temp[\'and\']')
                return None

            ### 基于BFS，core inference chain越短，优先级越高
            ### visited_vertex列表保存曾经已经入过队列的顶点，
            ### 保证曾经已经入过队列的顶点不再访问，防止在环子图中陷入死循环
            visited_vertex = []
            self.queue = []
            self.queue.append({'value': query_variables, 'path': []})
            visited_vertex.append(copy.deepcopy(self.queue[-1]['value'])) ### 变量名前有'?'
            self.all_core_chains = []
            while len(self.queue) > 0:
                ### copy.deepcopy() 避免修改一方的同时，无意间修改另一方
                queue_temp = copy.deepcopy(self.queue[0])
                del self.queue[0]

                for edge_temp in graph_temp['and'][queue_temp['value']]['edge']:
                    ### core_inferential_chain只考虑三元组
                    if edge_temp['type'] != 'triple':
                        continue
                    
                    ### 曾经已经入过队列的顶点不再访问，防止在环子图中陷入死循环
                    elif edge_temp['endpoint']['value'] in visited_vertex:
                        continue

                    ### topic entity必须是NamedNode，且不能是type
                    elif edge_temp['endpoint']['termType'] == 'NamedNode':
                        ### topic entity不能是 type
                        '''
                        for edge_temp_temp in graph_temp['and'][edge_temp['endpoint']['value']]['edge']:
                            if ((edge_temp_temp['type'] == 'triple') and \
                                (len(edge_temp_temp['predicate']['value']) >= 5) and \
                                (edge_temp_temp['predicate']['value'][-5:] == '#type')):
                                break
                        '''
                        if False:
                            print('false')
                        else:
                            ### self.topic_entity 的类型是 str

                            topic_entity = self.append_question_mark(copy.deepcopy(edge_temp['endpoint']['value']))
                            if self.topic_entity == None:
                                self.topic_entity = topic_entity
                            ### self.core_inferential_chain 的类型是 list -> dict，字典数据是三元组 {'subject': str, 'predicate': str, 'object': str}
                            if edge_temp['role'] == 'subject':
                                '''
                                copy.deepcopy(queue_temp['path']).append({
                                    'subject': copy.deepcopy(queue_temp['value']),
                                    'predicate': copy.deepcopy(edge_temp['predicate']['value']),
                                    'object': copy.deepcopy(edge_temp['endpoint']['value'])
                                }
                                最终结果为None，因为这里赋予的是append()的返回值，即为None，而不是列表数据本身
                                '''
                                path_temp = copy.deepcopy(queue_temp['path'])
                                path_temp.append({
                                    'subject': self.append_question_mark(copy.deepcopy(queue_temp['value'])),
                                    'predicate': copy.deepcopy(edge_temp['predicate']['value']),
                                    'object': self.append_question_mark(copy.deepcopy(edge_temp['endpoint']['value']))
                                })
                                path_temp.reverse()
                                self.all_core_chains.append({'chain':path_temp, 'topic_entity': topic_entity})
                                if self.core_inferential_chain == None:
                                    self.core_inferential_chain = path_temp
                            elif edge_temp['role'] == 'object':
                                path_temp = copy.deepcopy(queue_temp['path'])
                                path_temp.append({
                                    'subject': self.append_question_mark(copy.deepcopy(edge_temp['endpoint']['value'])),
                                    'predicate': copy.deepcopy(edge_temp['predicate']['value']),
                                    'object': self.append_question_mark(copy.deepcopy(queue_temp['value']))
                                })
                                path_temp.reverse()
                                self.all_core_chains.append({'chain': path_temp, 'topic_entity': topic_entity})
                                if self.core_inferential_chain == None:
                                    self.core_inferential_chain = path_temp
                            else: ### 其他未知情况
                                print('三元组的顶点的角色未知（非subject、非object）')

                                #return None

                            ### 原本path的顺序是 查询变量 -> topic entity。因此需要对列表进行翻转，变成 topic entity -> 查询变量

                    
                    ### 'Literal'是不能作为topic entity
                    elif edge_temp['endpoint']['termType'] == 'Literal':
                        continue

                    ### 只剩下edge_temp['endpoint']['termType'] == 'Variable' 或 
                    ### ((edge_temp_temp['type'] == 'triple') and (len(edge_temp_temp['predicate']['value']) >= 5) and (edge_temp_temp['predicate']['value'][-5:] == '#type'))
                    else:
                        if edge_temp['role'] == 'subject':
                            path_temp = copy.deepcopy(queue_temp['path'])
                            if 'value' not in edge_temp['predicate']:
                                print('predicate format strange:' + str(edge_temp['predicate']))
                                continue
                            path_temp.append({
                                'subject': self.append_question_mark(copy.deepcopy(queue_temp['value'])),
                                'predicate': copy.deepcopy(edge_temp['predicate']['value']),
                                'object': self.append_question_mark(copy.deepcopy(edge_temp['endpoint']['value']))
                            })
                        elif edge_temp['role'] == 'object':
                            path_temp = copy.deepcopy(queue_temp['path'])
                            path_temp.append({
                                'subject': self.append_question_mark(copy.deepcopy(edge_temp['endpoint']['value'])),
                                'predicate': copy.deepcopy(edge_temp['predicate']['value']),
                                'object': self.append_question_mark(copy.deepcopy(queue_temp['value']))
                            })
                        else:  ### 其他未知情况
                            print('三元组的顶点的角色未知（非subject、非object）')

                            #return None
                        self.queue.append({
                            'value': copy.deepcopy(edge_temp['endpoint']['value']), 
                            'path': path_temp
                        })
                        visited_vertex.append(copy.deepcopy(self.queue[-1]['value'])) ### 变量名前有'?'
        
            else:  ### 其他未知情况
                if self.core_inferential_chain == None:
                    print('从查询变量出发，无法搜索到合适的topic entity')
                    self.topic_entity = None
                    self.core_inferential_chain = None
                    return None
                else:
                    return self.topic_entity
        
        else:
            return self.topic_entity

    def get_core_inferential_chain(self):
        if self.core_inferential_chain == None:
            if self.graph == None:
                print('QueryGraph is None')
                return None

            ### 如果没有自行设置self.topic_entity 和 self.core_inferential_chain，
            ### 那么就随机生成
            if self.get_topic_entity() == None:
                print('因为无法生成topic entity，所以无法生成core inference chain')
                return None

            return self.core_inferential_chain
        else:
            return self.core_inferential_chain
    
    def get_constraints(self):
        if self.constraints == None:
            if self.graph == None:
                print('QueryGraph is None')
                return None

            if self.get_topic_entity() == None:
                print('因为无法生成topic entity，所以无法生成constraints')
                return None
            
            ### 为了数据安全，做深复制
            graph_temp = copy.deepcopy(self.graph)
            self.constraints = []

            ### 生成 core_inferential_chain 上的顶点依次排列的列表 core_inferential_chain_vertex_list
            ### 顺序是 topic entity -> 查询变量
            core_inferential_chain_vertex_list = []
            ### 利用self.core_inferential_chain的顺序是 topic entity -> 查询变量
            core_inferential_chain_vertex_list.append(copy.deepcopy(self.topic_entity))
            for i in range(len(self.core_inferential_chain)):
                if core_inferential_chain_vertex_list[-1] == self.remove_question_mark(self.core_inferential_chain[i]['subject']):
                    core_inferential_chain_vertex_list.append(self.remove_question_mark(copy.deepcopy(self.core_inferential_chain[i]['object'])))
                elif core_inferential_chain_vertex_list[-1] == self.remove_question_mark(self.core_inferential_chain[i]['object']): 
                    core_inferential_chain_vertex_list.append(self.remove_question_mark(copy.deepcopy(self.core_inferential_chain[i]['subject'])))
                else: ### 其他未知情况
                    print()
                    self.constraints = None
                    return None
                    
            ### 按照WebQSP的 "Constraints" 表示方法
            ### 搜索'and'逻辑中，core inference chain上的顶点连接的所有条件（1跳），这些条件排除了core inference chain包含的三元组
            ### 不等式运算符和'operator'的映射关系
            ### QALD中没有出现EXISTS和NOTEXISTS，因此这里不考虑。（在上面设计的Query Graph中，EXISTS和NOTEXISTS作为逻辑运算处理）
            operator_to_operator = {'=': 'Equal', '!=': 'NotEqual', '<': 'LessThan', '>': 'GreaterThan', '<=': 'LessOrEqual', '>=': 'GreaterOrEqual'}
            ### 不给topic entity设置条件
            for i in range(1, len(core_inferential_chain_vertex_list)):
                for core_inferential_chain_vertex_list_i_temp in graph_temp['and'][core_inferential_chain_vertex_list[i]]['edge']:
                    constraints_temp = {}
                    constraints_temp.update({'type': 'constraints'})

                    ### 边指向的顶点在core inference chain中，且在core inference chain中，当前顶点与边指向的顶点相邻。这种情况不添加在constraints中
                    if (core_inferential_chain_vertex_list_i_temp['type'] == 'triple') and \
                        (((i > 0) and (core_inferential_chain_vertex_list_i_temp['endpoint']['value'] == core_inferential_chain_vertex_list[i - 1])) or
                        ((i < (len(core_inferential_chain_vertex_list) - 1)) and (core_inferential_chain_vertex_list_i_temp['endpoint']['value'] == core_inferential_chain_vertex_list[i + 1]))):
                        continue

                    if core_inferential_chain_vertex_list_i_temp['type'] == 'triple':
                        constraints_temp.update({'Operator': 'Equal'})
                        ### 增加方向性，当前顶点做主语，'direction': 'out'；当前顶点做宾语，'direction': 'in'
                        if core_inferential_chain_vertex_list_i_temp['role'] == 'subject':
                            constraints_temp.update({'Direction': 'out'})
                        elif core_inferential_chain_vertex_list_i_temp['role'] == 'object':
                            constraints_temp.update({'Direction': 'in'})
                        else: ### 其他未知情况
                            print('三元组的顶点的角色未知（非subject、非object）')
                            self.constraints = None
                            return None
                    elif core_inferential_chain_vertex_list_i_temp['type'] == 'operation':
                        constraints_temp.update({'Operator': copy.deepcopy(operator_to_operator[core_inferential_chain_vertex_list_i_temp['operator']])})
                        ### 增加方向性，当前顶点是左操作数，'direction': 'out'；当前顶点是右操作数，'direction': 'in'
                        constraints_temp.update({'Direction': copy.deepcopy(core_inferential_chain_vertex_list_i_temp['direction'])})
                    else: ### 其他未知情况
                        print('有除了triple、operation以外的其他边')
                        self.constraints = None
                        return None

                    ### 边指向的顶点的类型是'termType': 'Variable'，这种情况不添加在constraints中
                    if core_inferential_chain_vertex_list_i_temp['endpoint']['termType'] == 'Variable':
                        ### 条件与 core inference chain 的距离大于1跳
                        print('条件与 core inference chain 的距离大于1跳')
                        self.constraints = None
                        return None
                    elif core_inferential_chain_vertex_list_i_temp['endpoint']['termType'] == 'NamedNode':
                        constraints_temp.update({'ArgumentType': 'Entity'})
                    elif core_inferential_chain_vertex_list_i_temp['endpoint']['termType'] == 'Literal':
                        constraints_temp.update({'ArgumentType': 'Value'})
                    else: ### 其他未知情况
                        print('endpoint类型未知（非Variable、非NamedNode、非Literal）')
                        self.constraints = None
                        return None
                    
                    constraints_temp.update({'Argument': self.append_question_mark(copy.deepcopy(core_inferential_chain_vertex_list_i_temp['endpoint']['value']))})
                    ### WebQSP的 "EntityName" ，未知，无法设置
                    constraints_temp.update({'EntityName': None})

                    ### core inference chain 中，从 topic entity 的下一个顶点开始标号（第1个标号为0）
                    constraints_temp.update({'SourceNodeIndex': (i - 1)})
                    ### WebQSP的 "NodePredicate" ，只有三元组才有谓语。不等式的谓语需要人为添加
                    if core_inferential_chain_vertex_list_i_temp['type'] == 'triple':
                        constraints_temp.update({'NodePredicate': copy.deepcopy(core_inferential_chain_vertex_list_i_temp['predicate']['value'])})
                    else:
                        constraints_temp.update({'NodePredicate': None})
                    ### WebQSP的 "ValueType" ，代码太难写，需要人为识别
                    constraints_temp.update({'ValueType': None})

                    self.constraints.append(copy.deepcopy(constraints_temp))

            ### 处理条件 'order_limit_offset'
            ### 按照WebQSP的 "Order" 表示方法
            if graph_temp['order_limit_offset'] != None:
                order_limit_offset = {}
                order_limit_offset.update({'type': 'order_limit_offset'})
                ### 只处理 graph_temp['order_limit_offset']['order'] 有1个表达式的情况
                ### 经统计 QALD 中所有'order'的列表的元素数量为1
                if len(graph_temp['order_limit_offset']['order']) > 1:
                    print('len(graph_temp[\'order_limit_offset\'][\'order\']) > 1')
                    self.constraints = None
                    return None

                '''
                QALD只有2种"order"
                第1种是
                "order": [
                    {
                        "expression": {
                            "termType": "Variable",
                            "value": "elevation"
                        },
                        "descending": true
                    }
                ]
                标志是 'termType' in graph_temp['order_limit_offset']['order'][0]['expression']
                用于排序的顶点是 graph_temp['order_limit_offset']['order'][0]['expression']['value']
                order_limit_offset.update({'ValueType': 'String'})
                第2种是，其中取值固定的字段是 "type": "aggregate", "aggregation": "count
                "order": [
                    {
                        "expression": {
                            "expression": {
                                "termType": "Variable",
                                "value": "film"
                            },
                            "type": "aggregate",
                            "aggregation": "count",
                            "distinct": false
                        },
                        "descending": true
                    }
                ]
                标志是 'termType' not in graph_temp['order_limit_offset']['order'][0]['expression']
                用于排序的顶点是 graph_temp['order_limit_offset']['order'][0]['expression']['expression']['value']
                order_limit_offset.update({'ValueType': 'Number'})
                '''

                ### 要么第一层'expression'就是变量，要么第二层'expression'是变量
                if 'termType' in graph_temp['order_limit_offset']['order'][0]['expression']:
                    vertex_order = graph_temp['order_limit_offset']['order'][0]['expression']['value']
                    order_limit_offset.update({'ValueType': 'String'})
                else:
                    vertex_order = graph_temp['order_limit_offset']['order'][0]['expression']['expression']['value']
                    order_limit_offset.update({'ValueType': 'Number'})

                ### 在core inference chain的顶点中，找到离topic entity最近的 且 与排序所用顶点vertex_order用三元组相连的顶点（1跳）
                for i, core_inferential_chain_vertex_list_temp in enumerate(core_inferential_chain_vertex_list):
                    for core_inferential_chain_vertex_list_temp_temp in graph_temp['and'][core_inferential_chain_vertex_list_temp]['edge']:
                        if (core_inferential_chain_vertex_list_temp_temp['type'] == 'triple') and (vertex_order == core_inferential_chain_vertex_list_temp_temp['endpoint']['value']):
                            order_limit_offset.update({'SourceNodeIndex': copy.deepcopy(i)})
                            order_limit_offset.update({'NodePredicate': copy.deepcopy(core_inferential_chain_vertex_list_temp_temp['predicate']['value'])})
                else: ### 在core inference chain的顶点中，找不到要求的顶点
                    order_limit_offset.update({'SourceNodeIndex': None})
                    order_limit_offset.update({'NodePredicate': None})

                if 'descending' not in graph_temp['order_limit_offset']['order'][0]:
                    ### 默认升序
                    order_limit_offset.update({'SortOrder': 'Ascending'})
                elif graph_temp['order_limit_offset']['order'][0]['descending'] == True:
                    order_limit_offset.update({'SortOrder': 'Descending'})
                else:
                    order_limit_offset.update({'SortOrder': 'Ascending'})
                order_limit_offset.update({'Start': copy.deepcopy(graph_temp['order_limit_offset']['offset'])})
                order_limit_offset.update({'Count': copy.deepcopy(graph_temp['order_limit_offset']['limit'])})

                self.constraints.append(copy.deepcopy(order_limit_offset))

            ### 在WebQSP中，放弃了 GROUP BY ... HAVING ... 
            ### 因此这里对 graph_temp['group_having'] != None 的QUeryGraph不作处理
            if graph_temp['group_having'] != None:
                print('有GROUP BY ... HAVING ...语句')
                self.constraints = None
                return None
            
            return self.constraints
        
        else:
            return self.constraints

    ### vertex的类型是str
    def append_question_mark(self, vertex):
        ### 为了数据安全，做深复制
        graph_temp = copy.deepcopy(self.graph)
        
        ### vertex not in graph_temp['and']，就一定不是Variable
        if (vertex in graph_temp['and']) and (graph_temp['and'][vertex]['termType'] == 'Variable') and (vertex[0] != '?'):
            return '?' + vertex
        else:
            return vertex

    ### vertex的类型是str
    def remove_question_mark(self, vertex):
        ### 为了数据安全，做深复制
        graph_temp = copy.deepcopy(self.graph)
        
        ### vertex[1:] not in graph_temp['and']，就一定不是Variable
        if (vertex[0] == '?') and (vertex[1:] in graph_temp['and']) and (graph_temp['and'][vertex[1:]]['termType'] == 'Variable'):
            return vertex[1:]
        else:
            return vertex

if __name__=="__main__":
    #data_path = 'data/QALD/train-multilingual-4-9.jsonl'
    data_path = 'data/QALD/test-multilingual-4-9.jsonl'

    do_read_SPARQL = True
    do_SPARQL_to_JSON = True
    do_JSON_to_QueryGraph = True

    data = read_SPARQL(data_path, do_read_SPARQL)
    data = SPARQL_to_JSON(data, do_SPARQL_to_JSON)
    data = JSON_to_QueryGraph(data, do_JSON_to_QueryGraph)