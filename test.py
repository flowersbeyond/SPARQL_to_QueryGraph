import json
### 读取数据
f = open('data/QALD/train-multilingual-4-9.json', 'r')
data = json.load(f) ### data为list->dict类型
f.close()
print('Old length: ', len(data))
'''
### 删掉无法解析的SPARQL
for i in range(len(data) - 1, -1, -1):
    if data[i]['json'] == None:
        del data[i]

count = 0
for i in range(len(data)):
    if 'order' in data[i]['json']:
        if len(data[i]['json']['order']) > 1:
            count += 1

print('count: ', count)
'''