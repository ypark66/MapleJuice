import os
import collections
import json

def parse_csv_line(line):
    if not line: return []
    list = []
    s = ''
    flag = 0
    for c in line:
        if c == '"':
            if not flag:
                flag = 1
            else:
                flag = 0
        elif c == ',':
            if flag == 1:
                s += c
            else:
                list.append(s)
                s = ''
        else:
            s += c
    return list

d = collections.defaultdict(list)
with open(os.getcwd() + '/Address_Points.csv', 'r') as f:
    while True:
        a = f.readline()
        a = f.readline()
        a = parse_csv_line(a)
        if not a:
            break
        if a[11] == "Recreation":
            d[a[5]] += [a[3]]   #

    print(d)

