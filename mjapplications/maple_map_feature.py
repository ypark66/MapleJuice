import collections


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

def maple(data):
    d = collections.defaultdict(list)
    data = data.split("\n")
    for line in data:
        a = parse_csv_line(line)
        if not a:
            continue
        if a[11] == "Recreation":
            d[a[5]] += [a[3]]  #find list of unique id of houses that is located in street with recreation feature.
    return d