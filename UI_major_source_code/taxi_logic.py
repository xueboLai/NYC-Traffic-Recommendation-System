from os import listdir
from os.path import isfile, join
import geopy.distance
import pandas as pd
import sys
import urllib
import json
import codecs
import time
import re

# the high-level opeartions that would output the travel duration in seconds between two coordinates based on historical taxi data
def wrapper(start_coord, end_coord, area_center_data, taxi_agg, weather, time):
    distance = calculate_od_dis(start_coord, end_coord)
    print("Distance is {}".format(distance))
    pickup = map_to_area_id(area_center_data, start_coord)
    dropoff = map_to_area_id(area_center_data, end_coord)
    print("Start area is {}".format(pickup))
    print("End area is {}".format(dropoff))
    vel = get_avg_speed(taxi_agg, weather, time, pickup, dropoff)
    print("Velocity is {}".format(vel))
    duration = calculate_avg_duration(float(distance), vel)
    return duration

#calculate the distance between two geo-coordinates
def calculate_od_dis(coords_1, coords_2):
    return (geopy.distance.vincenty(coords_1, coords_2).m)

#map current coordinates to an area
def map_to_area_id(area_center_data, cur_loc):
    #check whether the passed in value is path or dataframe
    try:
        area_center_data = pd.read_csv(area_center_data)
    except:
        pass
    station_id = area_center_data["Station_id"]
    longitude = area_center_data["longitude"]
    latitude = area_center_data["latitude"]
    lat_long_merg = [(float(latitude[i]), float(longitude[i])) for i in range(len(station_id))]
    #create dictionary for each of the station as key and its corresponding coordinates as value
    id_pos_mapping = {}
    for j in range(len(station_id)):
        id_pos_mapping[int(station_id[j])] = lat_long_merg[j]

    #put input coordinates in its closet area
    smallest = sys.maxsize
    smallest_id = -1

    for i in id_pos_mapping:
        dist = calculate_od_dis(cur_loc, id_pos_mapping[i])

        if (dist < smallest):
            smallest = dist
            smallest_id = i
    #return the closet aera id
    return smallest_id

    # find the closet point

#calculate duration using distance divided by velocity
def calculate_avg_duration(distance, vel):
    return distance / vel

#query the historical data to get the average speed between two area based on weather condition, current time, etc.
def get_avg_speed(taxi_agg="/Users/xuebolai/Documents/class/BDAD/taxi_od_mapping/data/taxi_agg.csv", weather=None,
                  time=4, pickup=145, dropoff=179):
    #check whether the input is path or dataframe object
    try:
        taxi_agg = pd.read_csv(taxi_agg, header=None, names=["pickup", "dropoff", "time",
                                                             "temp", "precp", "wind", "avg_dist",
                                                             "avg_duration", "avg_vel", "avg_price"])
    except:
        pass
    # print(taxi_agg.head(5))
    #query input data
    target_row = taxi_agg[(taxi_agg["pickup"] == pickup) & (taxi_agg["dropoff"] == dropoff) &
                          (taxi_agg["time"] == time) & (taxi_agg["temp"] == weather["temp"]) &
                          (taxi_agg["precp"] == weather["precp"]) & (taxi_agg["wind"] == weather["wind"])]
    #if not found, use the total average value
    #this situation is weird and rare
    if (len(target_row) == 0):
        return 8.1
    # print("success")
    return target_row["avg_vel"].tolist()[0]
    # except:
    #    pass

#merge multiple files to one output file
def mergeFile(input_path=None, outputFile=None):
    if (input_path is None or outputFile is None):
        return
    if (not isfile(input_path)):
        # mypath = "/Users/xuebolai/Documents/class/BDAD/taxi_od_mapping/data/area_mid"
        onlyfiles = [mypath + "/" + f for f in listdir(input_path) if isfile(join(input_path, f))]
        # print(onlyfiles)

        # output file
        output = open(outputFile, "a")
        for f in onlyfiles:
            file = open(f, "r")
            for i in file:
                output.write(i.replace("(", "").replace(")", ""))

        output.close
        print("success")

    else:
        output = open(outputFile, "a")
        input_file = open(input_path, "r")
        for i in input_file:
            output.write(i.replace("(", "").replace(")", ""))

        print("success")


#test code

# mergeFile("/Users/xuebolai/Documents/class/BDAD/taxi_od_mapping/data/area_mid/","/Users/xuebolai/Documents/class/BDAD/taxi_od_mapping/data/station_info.csv")

# area_center_data = pd.read_csv("/Users/xuebolai/Documents/class/BDAD/taxi_od_mapping/data/station_info.csv")

# map_to_area_id(area_center_data,(40.76561999,-73.96011265))

""""
weather = {}
weather["temp"] = 1
weather["precp"] = "F"
weather["wind"] = "F"
get_avg_speed("/Users/xuebolai/Documents/class/BDAD/taxi_od_mapping/data/taxi_agg.csv", weather)

travel_time = wrapper((40.63908938, -73.78429578), (40.7074555, -74.01398316),
                      area_center_data="/Users/xuebolai/Documents/class/BDAD/taxi_od_mapping/data/station_info.csv",
                      taxi_agg="/Users/xuebolai/Documents/class/BDAD/taxi_od_mapping/data/taxi_agg.csv",
                      weather=weather
                      , time=4)
print("Travel Time is {}".format(travel_time))
"""

