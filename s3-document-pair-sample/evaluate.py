#!/usr/bin/env python3
import json
import numpy
from os import path, listdir
#LABELED_DIRS = ['terabyte']
#LABELED_DIRS = ['web']
LABELED_DIRS = ['core']

def report_judgment_pair(judgment_pair, labeled_dir):
    if not path.exists(path.join(labeled_dir, judgment_pair, 'label')):
        return None

    label = open(path.join(labeled_dir, judgment_pair, 'label'), 'r').read()
    s3Score =json.loads(open(path.join(labeled_dir, judgment_pair, 's3score'), 'r').read())

    return {
        'label': int(label),
        's3Score': float(s3Score['s3Score']),
        'sharedTask': labeled_dir
    }

def report_precision(data, threshold):
    data = [i for i in data if i['s3Score'] >= threshold]
    valid = [i for i in data if i['label'] >= 1]
    print(str(threshold) +': ' + str(len(valid))+ '/' + str(len(data)) + ' = ' + str(len(valid)/len(data)))

if __name__ == '__main__':
    data = []
    for labeled_dir in LABELED_DIRS:
        for judgment_pair in listdir(labeled_dir):
            report = report_judgment_pair(judgment_pair, labeled_dir)
            if report is not None:
                print(json.dumps(report))
                data += [report]

for threshold in numpy.arange(0.5, 1.0, 0.01):
    report_precision(data, threshold)

