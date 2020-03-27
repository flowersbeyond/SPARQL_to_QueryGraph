import json
### 读取数据
f = open('data_in_JSON.json', 'r')
data = json.load(f) ### data为list->dict类型
f.close()
print('Old length: ', len(data))

### 删掉无法解析的SPARQL
for i in range(len(data) - 1, -1, -1):
    if data[i]['json'] == None:
        del data[i]
### 为了防止删除元素造成的列表长度变化，从后向前遍历。最终导致SPARQL由复杂到简单依次排列
### 最基础最重要的条件在前面
### is_first和'is_typical'的作用：保留恰好满足每个条件的第1个SPARQL
for i in range(len(data)):
    data[i].update({'is_typical': False})
### 条件：SPARQL语句的长度
upper_bound = 250
is_first = True
for i in range(len(data) - 1, -1, -1):
    if (len(data[i]['sparql']) < upper_bound) & (data[i]['is_typical'] == False):
        if is_first == True:
            data[i]['is_typical'] = True
            is_first = False
        else:
            del data[i]
### 条件：where内，三元组的数量
for upper_bound in range(4):
    is_first = True
    for i in range(len(data) - 1, -1, -1):
        for where_temp in data[i]['json']['where']:
            if where_temp['type'] == 'bgp':
                number_triple = len(where_temp['triples'])
        if (number_triple <= upper_bound) & (data[i]['is_typical'] == False):
            if is_first == True:
                data[i]['is_typical'] = True
                is_first = False
            else:
                del data[i]
### 条件：SPARQL语句内的变量数(?变量)
for upper_bound in range(3):
    ### 不严密，但是可以帮助筛选
    is_first = True
    for i in range(len(data) - 1, -1, -1):
        s = set()
        for j in range(len(data[i]['sparql'])):
            if (data[i]['sparql'][j] == '?') & (j < len(data[i]['sparql']) - 1):
                s.add(data[i]['sparql'][j + 1])
        if (len(s) <= upper_bound) & (data[i]['is_typical'] == False):
            if is_first == True:
                data[i]['is_typical'] = True
                is_first = False
            else:
                del data[i]
### 条件：where内，除了三元组以外的，其他条件的数量(例如filter)
for upper_bound in range(5):
    is_first = True
    for i in range(len(data) - 1, -1, -1):
        if (len(data[i]['json']['where']) <= upper_bound) & (data[i]['is_typical'] == False):
            if is_first == True:
                data[i]['is_typical'] = True
                is_first = False
            else:
                del data[i]
### 条件：'Constraints'的元素数量
for upper_bound in range(5):
    is_first = True
    for i in range(len(data) - 1, -1, -1):
        if (len(data[i]['Constraints']) <= upper_bound) & (data[i]['is_typical'] == False):
            if is_first == True:
                data[i]['is_typical'] = True
                is_first = False
            else:
                del data[i]

print('New length: ', len(data))
### 保存数据
f = open('WebQSP_typical_SPARQL.json', 'w')
json.dump(obj=data, fp=f, indent=4)