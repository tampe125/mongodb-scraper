import json

ret = []

with open('data_raw.json', 'r') as f:
    for line in f:
        parser = json.loads(line)
        ret.append(parser.get('ip_str'))

with open('data.json', 'w') as f:
    json.dump(ret, f)
