import re
import urllib.parse


public_prefix = {
                    'owl': '<http://www.w3.org/2002/07/owl#>',
                    'xsd': '<http://www.w3.org/2001/XMLSchema#>',
                    'rdfs': '<http://www.w3.org/2000/01/rdf-schema#>',
                    'rdf':'<http://www.w3.org/1999/02/22-rdf-syntax-ns#>',
                    'foaf': '<http://xmlns.com/foaf/0.1/>',
                    'yago': '<http://dbpedia.org/class/yago/>',
                    'dbo': '<http://dbpedia.org/ontology/>',
                    'dbp': '<http://dbpedia.org/property/>',
                    'dbr': '<http://dbpedia.org/resource/>',
                    'dct': '<http://purl.org/dc/terms/>',
                    'dbc': '<http://dbpedia.org/resource/Category:>'
                 }


def append_missing_prefix(query):
    existing_prefixs = extract_prefixes(query)
    for pref in public_prefix:
        if query.find(pref + ':') >= 0 and pref not in existing_prefixs:
            query = ('PREFIX %s: %s ' % (pref, public_prefix[pref])) + query

    return query

def extract_prefixes(query):

    tokens = tokenize(query)
    prefixes = {}

    pos = 0
    while pos < len(tokens):
        if tokens[pos].lower() == 'prefix':
            name = tokens[pos + 1][0:-1]
            value = tokens[pos + 2]
            prefixes[name] = value
            pos = pos + 3
        else:
            pos += 1

    return prefixes




def tokenize(query):
    query = query.replace('\n', ' ')
    query = re.sub(r"\s+", " ", query)
    query = query.strip()

    tokens = []

    pos = 0

    parenthesis_layer = 0
    while pos < len(query):
        token = ''
        if query[pos] == ' ':
            pos += 1
            continue
        elif query[pos] in ['{', '}', '.', ';']:
            token = query[pos]
            pos += 1
        elif query[pos] == '(':
            parenthesis_layer = 1
            token = query[pos]

            while parenthesis_layer != 0:
                pos += 1
                if query[pos] == '(':
                    parenthesis_layer += 1
                if query[pos] == ')':
                    parenthesis_layer -= 1
                token += query[pos]
            pos += 1
        elif query[pos] == '\"':
            token = query[pos]
            pos += 1
            while query[pos] != '\"':
                token += query[pos]
                pos += 1
            while query[pos] != ' ':
                token += query[pos]
                pos += 1
        elif query[pos] == '\'':
            token = query[pos]
            pos += 1
            while query[pos] != '\'':
                token += query[pos]
                pos += 1
            while query[pos] != ' ':
                token += query[pos]
                pos += 1

        elif query[pos] == '<' and parenthesis_layer == 0:
            while query[pos] != '>':
                token += query[pos]
                pos += 1
            token += '>'
            pos += 1
        else:
            while pos < len(query) and query[pos] not in ['{', '}', '<', '>', '(', ')', ';', ' ']:
                token += query[pos]
                pos += 1
        tokens.append(token)
    return tokens

