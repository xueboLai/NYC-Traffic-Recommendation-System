from os import listdir
from os.path import isfile, join
import geopy.distance
import pandas as pd
import sys

#calculate the distance between two coordinates
def calculate_od_dis(coords_1, coords_2):
    return (geopy.distance.vincenty(coords_1, coords_2).m)


#map the cur location to stations

def map_to_area_id(area_center_data, cur_loc):
    #check whether the passed in value is path or dataframe
    try:
        area_center_data = pd.read_csv(area_center_data)
    except:
        pass
    print(area_center_data)
    station_id = area_center_data["new"]
    longitude = area_center_data["lng"]
    latitude = area_center_data["lat"]
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


