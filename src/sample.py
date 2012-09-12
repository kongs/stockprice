#! /usr/bin/python3

import random


path = r"data-81-354.csv"

lstCode = list()

with open(path, 'r') as f:
    for l in f:
        if len(l.strip()) > 0:
            lstCode.append(l.strip())

lstSamples = list()
print(len(lstCode))
sampleV = 20
totalSamples = 6

for i in range(totalSamples):
    lstSamples.append(random.sample(lstCode, sampleV))

print(len(lstSamples))
with open("out.csv", 'w') as f:
    for i in lstSamples:
        print(', '.join(i), file = f)