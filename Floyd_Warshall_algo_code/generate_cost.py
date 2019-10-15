import sys
import csv
import numpy as np 
import pandas as pd
import math

def get_subway_cost():
    
    val = 120 # time value for subway duration between stations
    
    # open csv file
    with open('subwaylink.csv','r') as csvfile:
        reader = csv.reader(csvfile)
        rows= [row for row in reader]

    data=np.array(rows)
    n = data.shape[0]
    subway_cost = np.zeros((n,n))
    for i in range(n):
        for j in range(n):
            subway_cost[i][j] = math.inf
            
    #get rid of ' '
    for i in range(n):
            data[i][:] = [int(value) for value in data[i]]
    int_data = np.array(data)
    
    # sort by key
    data = sorted(data, key = lambda line: line[0])
    
    #map the station id to the corresponding order
    dic = {}
    new_id = []
    old_id = []
    for i in range(n):
        new_id.append(i)
        old_id.append(data[i][0])
    
    mapping = dict(zip(old_id, new_id))
    
    # fill in the cost matrix
    for i in range(n):
        for j in range(1,len(data[i])):
            index1 = mapping[data[i][0]]
            index2 = mapping[data[i][j]]
            subway_cost[index1][index2] = val
            
    return subway_cost



'''-----------------------transfer----------------------------------'''
def get_transfer_cost():
    
    velo = 2 # walking velocity m/s
    
    # read in bike to subway file 
    data = pd.read_csv("bikesubway.csv", header = None)
    data.columns = ['bike','subway','duration']
    
    data = data.dropna()
    data = data[data['subway'] != 117]
    data = data[data['subway'] != 206]
    data = data[data['subway'] != 223]
    data = data[data['subway'] != 423]
    
    #number of rows and cols
    num_row = data.shape[0]
    num_col = data.shape[1]
    
    #sort data in ascending order
    data = data.sort_values(by = ['bike'], ascending = True)
    
    # remove duplicative values and mapping id to index
    bike_set = set(data['bike'])
    subway_set = set(data['subway'])
    
    print(len(bike_set))
    print(len(subway_set))
    
    num_bike = len(bike_set)
    num_subway = len(subway_set)
    
    old_bike_id = list(bike_set)
    old_sub_id = list(subway_set)
    new_bike_id = [value for value in range(num_bike)]
    new_sub_id = [value for value in range(num_subway)]
    bike_mapping = dict(zip(old_bike_id, new_bike_id))
    subway_mapping = dict(zip(old_sub_id, new_sub_id))
    
    
    transfer_cost = np.zeros((num_bike, num_subway))
    for i in range(num_bike):
        for j in range(num_subway):
            transfer_cost[i][j] = math.inf
    
    f = lambda x: bike_mapping[x]
    g = lambda y: subway_mapping[y]
    h = lambda z: 1000*z/velo
    col1 = list(data['bike'].apply(f))
    col2 = list(data['subway'].apply(g))
    col3 = list(data['duration'].apply(h))

    # fill in transfer cost    
    for i in range(len(col1)):
        transfer_cost[col1[i]][col2[i]] = col3[i]
    return transfer_cost

                
def get_bike_cost(case):
    
    condition = [1.5325911070318845,2.7223654958248003]
    velo = condition[case]
    
    # open csv file
    data = pd.read_csv("bikebike.csv", header = None)
    data.columns = ['bike1','bike2','dist']
    data = data.dropna()
    
    n = 850
    bike_cost = np.zeros((n,n))
    
    data = data.sort_values(by = ['bike1'], ascending = True)
    
    # remove duplicative values and mapping id to index
    bike_set = set(data['bike1'])
    
    print(len(bike_set))
    
    num_bike = len(bike_set)
    
    old_bike_id = list(bike_set)
    new_bike_id = [value for value in range(num_bike)]
    bike_mapping = dict(zip(old_bike_id, new_bike_id))
    
    
    transfer_cost = np.zeros((num_bike, num_bike))
    
    for i in range(num_bike):
        for j in range(num_bike):
            bike_cost[i][j] = math.inf
    
    f = lambda x: bike_mapping[x]
    g = lambda y: bike_mapping[y]
    h = lambda z: 1000*z/velo
    col1 = list(data['bike1'].apply(f))
    col2 = list(data['bike2'].apply(g))
    col3 = list(data['dist'].apply(h))
    
    # fill in transfer cost    
    for i in range(len(col1)):
        bike_cost[col1[i]][col2[i]] = col3[i]
            
    return bike_cost
    

