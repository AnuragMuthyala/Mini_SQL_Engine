import sys
import sqlparse

global input_tables
global result_table
global table_attributes

input_tables = {}
result_table = {}
table_attributes = []

def get_tokens(s):

    l = []
    s = sqlparse.format(s, keyword_case = 'upper', identifier_case = 'lower')
    s = sqlparse.parse(s)[0]

    for i in range(len(s.tokens)):
        if s.tokens[i].is_whitespace:
            l.append(i)

    s = [s.tokens[i] for i in range(len(s.tokens)) if i not in l]

    return s

def get_column_token(s):

    tokens = str(s).split(' ')
    if len(tokens) >= 3:
        error_handler(0)
    elif len(tokens) == 2:
        if tokens[1] != 'ASC' and tokens[1] != 'DESC':
            error_handler(0)

    return tokens

def get_table_data(s, a):

    for i in range(len(s)):

        with open(s[i]+'.csv','r') as data:
            rows = data.readlines()

        rows = [row.rstrip('\n') for row in rows]

        for row in rows:

            row = row.split(',')
            for j in range(len(row)):

                row[j] = row[j].replace('"','')
                row[j] = row[j].replace("'",'')

            row = list(map(int, row))

            for k in range(len(a[i])):
                input_tables[s[i]][a[i][k]].append(row[k])
            input_tables[s[i]]['count'] += 1

def get_meta_tables(s):

    table_names = str(s).split(',')
    table_names = [i.strip(' ') for i in table_names]
    attributes = [[] for i in table_names ]

    with open('metadata.txt','r') as meta_file:
        lines = meta_file.readlines()

    lines = [l.rstrip('\n') for l in lines]

    for i in range(len(table_names)):

        l = 0
        while l < len(lines):

            if lines[l] == table_names[i]:

                l += 1
                input_tables[table_names[i]] = {}
                input_tables[table_names[i]]['count'] = 0

                while lines[l] != '<end_table>':
                    #print(lines[l].lower())
                    input_tables[table_names[i]][lines[l].lower()] = []
                    attributes[i].append(lines[l].lower())
                    l += 1

                l = len(lines)

            l += 1

    get_table_data(table_names, attributes)
    #print(input_tables)

    return table_names

def extract_conditions(c):

    l = []
    flag = []
    end = False
    found = False

    for i in c.tokens:

        if str(i) == 'AND':
            flag.append('AND')
        elif str(i) == 'OR':
            flag.append('OR')
        elif isinstance(i, sqlparse.sql.Comparison):
            ls = []
            for tok in i.tokens:
                if isinstance(tok, sqlparse.sql.Function):
                    ls.append((str(tok.tokens[0]).upper(), str(tok.tokens[1])[1:len(str(tok.tokens[1]))-1].lower()))
                elif str(tok) == ' ':
                    pass
                else:
                    ls.append(str(tok))
            l.append(ls)
        elif str(i) == 'WHERE':
            if not found:
                found = True
            else:
                error_handler(0)
        elif str(i) == ' ':
            pass
        elif str(i) == ';':
            end = True
        else:
            error_handler(4)

    if l == [] and flag == []:
        error_handler(5)

    return (l, flag, end)

def error_handler(code):

    msg = 'ERROR '
    if code == 0:
        print(msg+str(code)+': '+'Invalid Query')
    elif code == 1:
        print(msg+str(code)+': '+'Keywords cannot be used as attributes')
    elif code == 2:
        print(msg+str(code)+': '+'Incorrect columns')
    elif code == 3:
        print(msg+str(code)+': '+'Invalid table names')
    elif code == 4:
        print(msg+str(code)+': '+'Incorrect clause in where')
    elif code == 5:
        print(msg+str(code)+': '+'No where clause')
    elif code == 6:
        print(msg+str(code)+': '+'Invalid columns')
    elif code == 7:
        print(msg+str(code)+': '+'Table not found')
    elif code == 8:
        print(msg+str(code)+': '+'Unknown columns')
    elif code == 9:
        print(msg+str(code)+': '+'Grouping under unknown column')
    elif code == 10:
        print(msg+str(code)+': '+'Aggregate under unknown column')
    elif code == 11:
        print(msg+str(code)+': '+'Ordering over unknown column')
    elif code == 12:
        print(msg+str(code)+': '+'Projection upon an unknown column or rows')
    elif code == 13:
        print(msg+str(code)+': '+'Missing semicolon')
    elif code == 14:
        print(msg+str(code)+': '+'Grouping without aggregation cannot be projected')

    quit()

def find_identifier(tokens, i):

    if i+1 >= len(tokens):
        error_handler(0)
    elif isinstance(tokens[i+1],sqlparse.sql.Function):
        return i
    elif str(tokens[i+1]) == '*' or isinstance(tokens[i+1], sqlparse.sql.Identifier) or isinstance(tokens[i+1],sqlparse.sql.IdentifierList):
        return i
    elif isinstance(tokens[i+1], sqlparse.sql.Token) and str(tokens[i+1]) != '*':
        error_handler(1)

def find_tables(tokens, i):

    if i+1 >= len(tokens):
        error_handler(0)
    elif isinstance(tokens[i+1], sqlparse.sql.Identifier) or isinstance(tokens[i+1],sqlparse.sql.IdentifierList):
        return i
    else:
        error_handler(3)

def find_groups(tokens, i):

    if i+1 >= len(tokens):
        error_handler(0)
    elif isinstance(tokens[i+1], sqlparse.sql.Identifier):
        return i
    else:
        error_handler(6)

def find_order(tokens, i):

    if i+1 >= len(tokens):
        error_handler(0)
    elif isinstance(tokens[i+1], sqlparse.sql.Identifier):
        return i
    else:
        error_handler(6)

def get_columns(s):

    tokens = []
    columns = []
    function = []
    agg = False

    if isinstance(s, sqlparse.sql.Function):
        tokens.append(s)

    elif str(s) == '*':

        columns = '*'
        return (columns, function, agg)

    else:
        tokens = [i for i in s.tokens if str(i) != ' ' and str(i) != ',']

    for i in tokens:

        if type(i) == type(sqlparse.sql.Function()):

            j = i.tokens
            if str(j[0]).upper() != 'COUNT' and str(j[1])[1:len(str(j[1])) - 1] == '*':
                error_handler(2)
            function.append((str(j[0]).upper(), str(j[1])[1:len(str(j[1])) - 1],tokens.index(i)))
            if str(j[1])[1:len(str(j[1])) - 1] == '*':
                columns.append('*')
            else:
                columns.append(str(j[1])[1:len(str(j[1])) - 1])
            agg = True

        else:
            if str(i) == '*' and columns == []:
                columns = '*'
            elif str(i) == '*' or (str(i) != '*' and columns == '*'):
                error_handler(2)
            else:
                columns.append(str(i))              

    return (columns, function, agg)

def parse_statement(s):

    tokens = get_tokens(s)
    query_expression = {}
    i = 0
    c = 0

    while i < len(tokens):

        #print(str(tokens[i]))
        if i == 0:

            if str(tokens[i]) == 'SELECT':

                if str(tokens[i+1]) == 'DISTINCT':
                    query_expression['distinct'] = True
                    i += 1

                i = find_identifier(tokens, i)

                c = get_columns(tokens[i+1])

                query_expression['pi'] = c

                if c[2]:
                    query_expression['agg'] = c[1]

                i += 2

            else:

                error_handler(0)
                return -1

        elif i == 2 or i == 3:

            if str(tokens[i]) == 'FROM':

                i = find_tables(tokens, i)

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

                if 'selection' in query_expression.keys() or 'group' in query_expression.keys() or 'order' in query_expression.keys():
                    error_handler(0)
                query_expression['selection'] = (c[0], c[1])
                if c[2]:
                    query_expression['end'] = True
                    break;
                i += 1

            elif str(tokens[i]) == 'GROUP BY':

                i = find_groups(tokens, i)

                c = get_columns(tokens[i+1])

                if c == -1:

                    error_handler(4)
                    return -1

                if 'group' in query_expression.keys() or 'order' in query_expression.keys():
                    error_handler(0)
                query_expression['group'] = c[0]
                i += 2

            elif str(tokens[i]) == 'ORDER BY':

                i = find_groups(tokens, i)

                c = get_column_token(tokens[i+1])

                if c == -1:

                    error_handler(4)
                    return -1

                if 'order' in query_expression.keys():
                    error_handler(0)
                query_expression['order'] = c
                i += 2

            elif str(tokens[i]) == ';':
                query_expression['end'] = True
                i += 1
                break;

            else:
                error_handler(0)

    try:
        query_expression['end']
    except:
        error_handler(13)

    return query_expression

def projection_handler(query, distinct):
    
    global table_attributes
    columns = query['pi'][0]
    l = []
    rows = []

    if columns == '*':

        columns = list(result_table.keys())
        columns.remove('count')
        l = ['.'.join(i) for i in table_attributes]
    
    else:

        for i in range(len(columns)):

            if columns[i] == '*':
                for k in query['pi'][1]:
                    if columns[i] == k[1] and k[0] == 'COUNT':
                        l.append('count(*)')
                        columns[i] = 'count'

            else:            
                for j in table_attributes:

                    if columns[i] == j[1]:

                        found = False

                        for k in query['pi'][1]:

                            if columns[i] == k[1] and i == k[2]:
                                found = True
                                l.append(k[0].lower()+'('+'.'.join(k[1])+')')
                                if 'group' in query.keys() and columns[i] == query['group'][0]:
                                    columns[i] = 'red_col'
                                break

                        if not found:
                            l.append('.'.join(j))

    rows.append(','.join(l))

    l = []
    try:
        check = len(result_table[columns[0]])
        for i in columns:
            if check != len(result_table[i]):
                error_handler(12)

        for i in range(len(result_table[columns[0]])):

            s = []
            for j in range(len(columns)):
                
                if isinstance(result_table[columns[j]][i], list):

                    if len(result_table[columns[j]][i]) == 1:
                        result_table[columns[j]][i] = result_table[columns[j]][i][0]
                        s.append(str(result_table[columns[j]][i]))

                    else:
                        error_handler(14)

                else:

                    s.append(str(result_table[columns[j]][i]))

            s = ','.join(s)

            if distinct:

                if s not in l:

                    rows.append(s)
                    l.append(s)

            else:
                rows.append(s)

        for row in rows:
            print(row)

    except:
        error_handler(12)

def order_by_handler(query):

    column = query['order']

    try:
        order = query['order'][1]
    except:
        order = 'NULL'
    loc = -1
    val = -1

    try:

        if order == 'ASC' or order == 'NULL':

            for i in range(len(result_table[column[0]])):

                loc = i
                val = result_table[column[0]][i]

                for j in range(i+1,len(result_table[column[0]])):

                    if result_table[column[0]][j] < val:

                        loc = j
                        val = result_table[column[0]][j]

                if loc != i:

                    for key in result_table.keys():
                        if key != 'count' or len(result_table['count']) != 1:
                            val = result_table[key][loc]
                            result_table[key][loc] = result_table[key][i]
                            result_table[key][i] = val

        else:

            for i in range(len(result_table[column[0]])):

                loc = i
                val = result_table[column[0]][i]

                for j in range(i+1,len(result_table[column[0]])):

                    if result_table[column[0]][j] > val:

                        loc = j
                        val = result_table[column[0]][j]

                if loc != i:

                    for key in result_table.keys():
                        if key != 'count' or len(result_table['count']) != 1:
                            val = result_table[key][loc]
                            result_table[key][loc] = result_table[key][i]
                            result_table[key][i] = val

    except:
        error_handler(11)

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
        l.append(sum(d) // len(d))

    return l

def aggregate_handler(query, con):

    global result_table
    ag_func = query['agg']

    if not con:

        for i in ag_func:

            try:
                if i[1] != '*':
                    column = i[1]
                    val = aggregate(i[0], result_table[column])
                    result_table[column] = val
                elif i[0] == 'COUNT' and i[1] == '*':
                    pass
                else:
                    error_handler(10)

            except:
                error_handler(10)
                    
    else:

        for i in ag_func:

            try:
                if i[1] != '*':
                    column = i[1]
                    if i[1] == query['group'][0]:
                        column = 'red_col'
                    for l in range(len(result_table[column])):
                        val = aggregate(i[0], result_table[column][l])
                        result_table[column][l] = val[0]
                elif i[0] == 'COUNT' and i[1] == '*':
                    pass
                else:
                    error_handler(10)

            except:
                error_handler(10)

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
    column = query['group'][0]
    other_columns = [i for i in result_table.keys() if i != column]
    other_columns.remove('count')
    values = set()
    count_list = []

    try:
        values.update(result_table[column])
    
    except:
        error_handler(9)

    values = list(values)

    new_table = {k:[[] for j in values] for k in result_table.keys()}
    count_list = [0 for j in values]
    del new_table['count']

    for value in range(len(values)):

        new_table[column][value] = values[value]

        for v in range(len(result_table[column])):

            if values[value] == result_table[column][v]:

                for key in other_columns:
                    new_table[key][value].append(result_table[key][v])
                count_list[value] += 1

    result_table = new_table
    result_table['count'] = count_list
    result_table['red_col'] = [[values[v] for j in range(count_list[v])] for v in range(len(values))]

def check_int(a):

    try:

        int(a)
        return True

    except:
        return False

def get_values(v):

    if isinstance(v, tuple):
        return [aggregate(v[0], result_table[v[1]])[0] for i in range(len(result_table[v[1]]))]
    elif check_int(v):
        return [int(v) for i in range(len(result_table[list(result_table.keys())[0]]))]
    else:
        return result_table[v]

def selection_handler(query):

    conditions = query['selection']
    pos = -1
    count_list = result_table['count'][0]
    result_l = []

    try:

        for condition in conditions[0]:

            values = get_values(condition[0])
            operator = [condition[1] for i in range(len(values))]
            value = get_values(condition[2])

            l = list(map(conditional_columns, values, operator, value))

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

        result_table['count'] = [count_list - len([j for j in l if j == False])]

    except:
        error_handler(8)

def join_table(a, d, i_t):

    global table_attributes
    d = [[row[i] for row in d] for i in range(len(d[0]))]
    result_data = []
    l = []

    for column in i_t.keys():
        if column != 'count':
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

    global table_attributes
    tables = query['tables']
    #print(tables)
    inserted = False

    attributes = []
    data = []
    count = 0

    try:

        for name in tables:

            if not inserted:

                for i in input_tables[name].keys():

                    if i != 'count':
                        table_attributes.append((name,i))
                        attributes.append(i)
                        data.append(input_tables[name][i])

                count += input_tables[name]['count']
                inserted = True

            else:

                for i in input_tables[name].keys():
                    if i != 'count':
                        table_attributes.append((name,i))

                attributes, data = join_table(attributes, data, input_tables[name])
                count *= input_tables[name]['count']

    except:
        error_handler(7)

    for i in range(len(attributes)):
        result_table[attributes[i]] = data[i]

    result_table['count'] = [count]

def evaluate_query(query):

    global result_table
    ops = query.keys()

    if 'tables' in ops:
        join_handler(query)

    if 'selection' in ops:
        selection_handler(query)

    if 'group' in ops:
        group_by_handler(query)

    if 'agg' in ops and 'group' in ops:
        aggregate_handler(query, True)
    elif 'agg' in ops:
        aggregate_handler(query, False)

    if 'order' in ops:
        order_by_handler(query)

    if 'distinct' in ops:
        distinct = True
    else:
        distinct = False

    if 'pi' in ops:
        projection_handler(query, distinct)

try:
    s = sys.argv[1]

    query = parse_statement(s)
    evaluate_query(query)
except:
    error_handler(0)