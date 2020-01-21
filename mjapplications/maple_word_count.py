import collections

def maple(data):
    data = data.replace("\n", " ")
    data = data.replace("\t", " ")
    data = data.split(' ')
    d = collections.defaultdict(int)
    for c in data:
        d[c] += 1
    return d
