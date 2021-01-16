import sys
import sqlparse

global input_tables
global result_table

input_tables = {}
result_table = {}

def get_tokens(s):

    l = []
    s = sqlparse.format(s, keyword_case = 'upper')
    s = sqlparse.parse(s)[0]

    for i in range(len(s.tokens)):
        if s.tokens[i].is_whitespace:
            l.append(i)

    s = [s.tokens[i] for i in range(len(s.tokens)) if i not in l]

    return s

def get_columns(s):

    tokens = []
    columns = []
    function = []
    agg = False

    if isinstance(s, sqlparse.sql.Function):
        tokens.append(s)
    else:
        tokens = [i for i in s.tokens if str(i) != ' ' and str(i) != ',']

    for i in tokens:

        if type(i) == type(sqlparse.sql.Function()):

            j = i.tokens
            function.append((str(j[0]).upper(), str(j[1])[1:len(str(j[1])) - 1]))
            columns.append(str(j[1])[1:len(str(j[1])) - 1])
            agg = True

        else:
            columns.append(str(i))

    return (columns, function, agg)

def get_table_data(s, a):

    for i in range(len(s)):

        with open('files/'+s[i]+'.csv','r') as data:
            rows = data.readlines()

        rows = [row.rstrip('\n') for row in rows]

        for row in rows:

            row = list(map(int, row.split(',')))

            for k in range(len(a[i])):
                input_tables[s[i]][a[i][k]].append(row[k])

def get_meta_tables(s):

    table_names = str(s).split(',')
    table_names = [i.replace(' ','') for i in table_names]
    attributes = [[] for i in table_names ]

    with open('files/metadata.txt','r') as meta_file:
        lines = meta_file.readlines()

    lines = [l.rstrip('\n') for l in lines]

    for i in range(len(table_names)):

        l = 0
        while l < len(lines):

            if lines[l] == table_names[i]:

                l += 1
                input_tables[table_names[i]] = {}

                while lines[l] != '<end_table>':

                    input_tables[table_names[i]][lines[l]] = []
                    attributes[i].append(lines[l])
                    l += 1

                l = len(lines)

            l += 1

    get_table_data(table_names, attributes)
    print(input_tables)

    return table_names

def extract_conditions(c):

    l = []
    flag = []

    for i in c.tokens:

        if str(i) == 'AND':
            flag.append('AND')
        if str(i) == 'OR':
            flag.append('OR')
        if type(i) == type(sqlparse.sql.Comparison()):
            l.append(str(i).split(' '))

    return (l, flag)

def error_handler(code):

    if code == 0:
        print('Invalid Query')
    elif code == 1:
        print('Table not found')
    elif code == 2:
        print('Invalid Query')

def parse_statement(s):

    tokens = get_tokens(s)
    print(tokens)
    query_expression = {}
    i = 0
    c = 0

    while i < len(tokens):

        print(str(tokens[i]))
        if i == 0:

            if str(tokens[i]) == 'SELECT':

                c = get_columns(tokens[i+1])
                print(c)

                query_expression['pi'] = c[0]

                if c[2]:
                    query_expression['agg'] = c[1]

                i += 2

            else:

                error_handler(0)
                return -1

        elif i == 2:

            if str(tokens[i]) == 'FROM':

                c = get_meta_tables(tokens[i+1])

                if c == -1:

                    error_handler(1)
                    return -1

                query_expression['tables'] = c
                i += 2

            else:

                error_handler(2)
                return -1

        elif i >= 4:

            if 'WHERE' in str(tokens[i]):

                c = extract_conditions(tokens[i])

                if c == -1:

                    error_handler(3)
                    return -1

                query_expression['selection'] = c
                i += 1

            if str(tokens[i]) == 'GROUP BY':

                c = get_columns(tokens[i+1])

                if c == -1:

                    error_handler(4)
                    return -1

                query_expression['group'] = c[0]
                i += 2

    return query_expression

def projection_handler(query):

    columns = query['pi']

    print(','.join(columns))

    for i in range(len(result_table[columns[0]])):

        s = ""
        for j in range(len(columns)):

            s += str(result_table[columns[j]][i])

            if j != len(columns) - 1:
                s += ','

        print(s)

def aggregate(func, d):

    l = []

    if func == 'MAX':
        l.append(max(d))

    if func == 'MIN':
        l.append(min(d))

    if func == 'SUM':
        l.append(sum(d))

    if func == 'COUNT':
        l.append(len(d))

    if func == 'AVG':
        l.append(sum(d) / len(d))

    return l

def aggregate_handler(query, con):

    global result_table
    ag_func = query['agg']

    if con == False:

        for i in ag_func:

            for column in result_table.keys():

                if i[1] == column:
                    val = aggregate(i[0], result_table[column])
                    result_table[column] = val
                    break

def conditional_columns(a, o, c):

    if o == '<':
        if a < c:
            return True
        else:
            return False

    if o == '>':
        if a > c:
            return True
        else:
            return False

    if o == '<=':
        if a <= c:
            return True
        else:
            return False

    if o == '>=':
        if a >= c:
            return True
        else:
            return False

    if o == '=':
        if a == c:
            return True
        else:
            return False

def group_by_handler(query):

    global result_table
    print(result_table)
    column = query['group'][0]
    print(column)
    other_columns = [i for i in result_table.keys() if i != column]
    values = set()

    for i in result_table.keys():

        if i == column:
            values.update(result_table[column])
            break

    values = list(values)

    new_table = {k:[[] for j in values] for k in result_table.keys()}

    for value in range(len(values)):

        new_table[column][value] = values[value]

        for v in range(len(result_table[column])):

            if values[value] == result_table[column][v]:

                for key in other_columns:
                    new_table[key][value].append(result_table[key][v])

    result_table = new_table

    print(result_table)

def check_int(a):

    try:

        int(a)
        return True

    except:
        return False

def selection_handler(query):

    conditions = query['selection']
    pos = -1
    result_l = []

    for condition in conditions[0]:

        if check_int(condition[2]): 
    
            for i in result_table.keys():

                l = []
                if condition[0] == i:

                    values = result_table[i]
                    operator = [condition[1] for i in range(len(values))]
                    value = [int(condition[2]) for i in range(len(values))]

                    l = list(map(conditional_columns, values, operator, value))
                    break

        else:

            for i in result_table.keys():

                l = []
                if condition[0] == i:

                    values = result_table[i]
                    operator = [condition[1] for i in range(len(values))]
                    value = result_table[condition[2]]

                    l = list(map(conditional_columns, values, operator, value))
                    break

        if pos >= 0:

            if conditions[1][pos] == 'AND':
                result_l = [result_l[i] and l[i] for i in range(len(l))]
            else:
                result_l = [result_l[i] or l[i] for i in range(len(l))]

        else:
            result_l = l

        pos += 1

    if pos != -1:
        l = result_l

    for i in result_table.keys():

        values = [result_table[i][j] for j in range(len(result_table[i])) if l[j] == True]
        result_table[i] = values

def join_table(a, d, i_t):

    d = [[row[i] for row in d] for i in range(len(d[0]))]
    result_data = []
    l = []

    for column in i_t.keys():

        a.append(column)
        l.append(i_t[column])

    l = [[row[i] for row in l] for i in range(len(l[0]))]

    for i in d:

        for j in l:

            row = list(i)
            row.extend(j)
            result_data.append(row)

    result_data = [[row[i] for row in result_data] for i in range(len(result_data[0]))]

    return a, result_data

def join_handler(query):

    print(input_tables)
    tables = query['tables']
    inserted = False

    attributes = []
    data = []

    for name in tables:

        if not inserted:

            for i in input_tables[name].keys():

                attributes.append(i)
                data.append(input_tables[name][i])

            inserted = True

        else:

            attributes, data = join_table(attributes, data, input_tables[name])

    for i in range(len(attributes)):

        result_table[attributes[i]] = data[i]

def evaluate_query(query):

    global result_table
    print(query)
    ops = query.keys()

    if 'tables' in ops:
        join_handler(query)
        #result_table = input_tables[query['tables'][0]]

    if 'selection' in ops:
        selection_handler(query)

    if 'group' in ops:
        group_by_handler(query)

    if 'agg' in ops and 'group' in ops:
        aggregate_handler(query, True)
    elif 'agg' in ops:
        aggregate_handler(query, False)

    if 'pi' in ops:
        projection_handler(query)

s = sys.argv[1]

query = parse_statement(s)
#print(input_tables)
evaluate_query(query)
#parse_tables()