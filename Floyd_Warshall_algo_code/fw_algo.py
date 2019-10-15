import sys
import csv
import math
import numpy as np
import generate_cost

INF = math.inf

'''
we could have multiple matrice that record the cost under different conditions
time: 1 2 3 4
weather good normal bad
'''

# input the cost value
subway_cost = generate_cost.get_subway_cost()
transfer_cost = generate_cost.get_transfer_cost()


#bike_cost = generate_cost.get_bike_cost(1)
bike_cost = generate_cost.get_bike_cost(1)
transfer_cost_T = transfer_cost.T

row = 850 + 469
col = row

length1 = 469
cost = np.zeros((row,row))
for i in range(row) :
    for j in range(row):
        if i < length1 and j < length1:
            cost[i][j] = subway_cost[i][j]
        if i < length1 and j >= length1:
            cost[i][j] = transfer_cost_T[i-length1][j-length1]
        if i >= length1 and j < length1:
            cost[i][j] = transfer_cost[i-length1][j]
        if i >= length1 and j >= length1:
            cost[i][j] = bike_cost[i-length1][j-length1]
            
print(cost)
            
# get the number of stations
intermediate = np.zeros((row,col))


# intermediate if no edge -> -1 else 0
for i in range(0,row):
    for j in range(0,col):
        if cost[i][j] != INF:
            intermediate[i][j] = 0
        else:
            intermediate[i][j] = -1

# calculate the shortest cost between each pair and store
for k in range(0,row):
    for i in range(0,row):
        for j in range(0,col):
            if cost[i][j] >= cost[i][k] + cost[k][j]:
                cost[i][j] = cost[i][k] + cost[k][j]
                intermediate[i][j] = k 


# traverse function get the the vertice passed
'''
all_path = []


def traverse(i,j,record):
    v = intermediate[i][j]
    if v == 0:
        record.append(v)
    else:
        traverse(i,k,record)
        traverse(k,j,record)


# 
def get_path(o, d, record):
    
    if intermediate[o][d] != -1:
        traverse(o,d,record)
        record.append(d)
    return 


for i in range(row):
    for j in range(col):
        record = []
        path = get_path(i,j,record)
        all_path.append(path)
        
print(all_path)
'''




all_path = []

def traverse_left(i,j,record):
    start = i
    end = j
    while intermediate[start][end] != 0:
        record.append(intermediate[start][end])
        start = i
        end = k
        
    

def traverse_right(i,j,record):
    start = i
    end = j
    while intermediate[start][end] != 0:
        if start != i or end != j:
            record.append(intermediate[start][end])
        start = k
        end = j


def get_path(o, d):
    record = []
    traverse_left(o,d,record)
    traverse_right(o,d.record)
    return record

'''
for i in range(row):
    for j in range(col):
        path = get_path(i,j)
        all_path.append(path)
'''

print(get_path(1,300))
        

f = open("output_csv.csv","w")
for i in all_path:
    for j in i:
        f.write(j)
        f.write(",")
    f.write("\n")