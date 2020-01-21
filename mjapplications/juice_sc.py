import collections


def juice(data):
    d = collections.defaultdict(int)
    for k,v in data.items():
        d[k] += sum(v)
    return d