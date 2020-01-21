import collections


def juice(data):
    d = collections.defaultdict(list)
    for k,v in data.items():
        for v_i in v:
            d['Unique IDs'] += v_i
    return d