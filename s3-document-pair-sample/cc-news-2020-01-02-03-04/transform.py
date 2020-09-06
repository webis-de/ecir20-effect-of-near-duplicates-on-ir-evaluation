#!/usr/bin/env python3
import sys
import json

file_name=sys.argv[1]
with open(file_name, 'r') as f, open(file_name + '-new', 'w+') as o:
    for l in f:
        o.write(json.dumps(json.loads(l)['ids']) + '\n')

