import collections


def juice(data):
    d = collections.defaultdict(int)
    for k,v in data.items():
        s = sum(v)
        d[k] += s
    return d



if __name__ == "__main__":
    data = {"socket":[5,5,5,5,5,5]}
    d = juice(data)
    print(d)