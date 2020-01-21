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
    a = parse_csv_line(data)
    if a[11] == "Recreation":
        d[a[5]] += [a[3]]  #find list of unique id of houses that is located in street with recreation feature.
    return d

def juice(data):
    d = collections.defaultdict(list)
    for k,v in data.items():
        d['Unique IDs'] += v
    return d

if __name__ == "__main__":
    with open('Address_Points.csv', 'r') as f:
        d = collections.defaultdict(list)
        while True:
            l = f.readline()
            # print(l)
            if not l:
                break
            for k, v in maple(l).items():
                d[k] += v
        print(d)
        d = juice(d)
        print(d)