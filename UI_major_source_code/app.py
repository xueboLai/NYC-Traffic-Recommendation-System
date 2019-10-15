#import libraries
from flask import Flask, render_template, send_file, redirect, url_for,request,jsonify
import taxi_logic as tl
import convertCoor as CO
from datetime import datetime
from datetime import timedelta
import PublicTransitTime as PTT
import pandas as pd
import logic
import locateStation as ls

app = Flask(__name__)

#middle end
#path handler
#default path or home path
@app.route('/')
@app.route("/Home.html")
def home():
    #render web page based on the template home.html template file
    return render_template("home.html")

#run the application
if __name__ == '__main__':
    app.run()

#load pictures for the website
@app.route("/pictures/nyu")
def get_image():
    return send_file("pictures/nyu.jpeg")

#handle the sumbit button clicked
@app.route("/submit", methods=["GET", "POST"])
def submit():

    #obtain the form value
    value_mapping = {}
    value_mapping["startLoc"] = request.form.get('startLocation')
    value_mapping["endLoc"] = request.form.get('endLocation')
    value_mapping["temperature"] = request.form.get('temperature')
    value_mapping["windy"] = request.form.get('windy')
    value_mapping["rain"] = request.form.get('rain')
    value_mapping["route"] = request.form.get('route')
    value_mapping["sendEmail"] = request.form.get('sendEmail')
    print(value_mapping)

    #map the location to the right formatted coordinates
    startCoordDic = CO.find_location_geo_by_address(value_mapping["startLoc"])
    startCoord = (startCoordDic["lat"],startCoordDic["lng"])
    endCoordDic = CO.find_location_geo_by_address(value_mapping["endLoc"])
    endCoord = (endCoordDic["lat"],endCoordDic["lng"])


    #map the time to the right format
    hour = int(datetime.now().strftime('%H'))
    inputTime = -1
    if(hour>=6 and hour<=9):
        inputTime = 1
    elif(hour>=10 and hour<=16):
        inputTime = 2
    elif(hour>=17 and hour<=20):
        inputTime = 3
    else:
        inputTime = 4

    #map the weather condition to the right format to be processed
    weather = {}
    if(int(value_mapping["temperature"])<40):
        weather["temp"] =1
    elif(int(value_mapping["temperature"])>=40 and int(value_mapping["temperature"])<80):
        weather["temp"] = 2
    else:
        weather["temp"] = 3
    weather["precp"] = "F" if value_mapping["rain"]=="no" else "T"
    weather["wind"] = "F" if value_mapping["windy"]=="no" else "T"
    #tl.get_avg_speed("data/taxi_agg.csv", weather)

    #query the data layer to ge the travel time based on start/end areas, weather condition, current time and historical data
    travel_time = tl.wrapper(startCoord, endCoord,
                             area_center_data="data/station_info.csv",
                             taxi_agg="data/taxi_agg.csv",
                             weather=weather
                             , time=inputTime)

    print("Travel Time is {}".format(travel_time))

    output_time = logic.format_travel_time(travel_time)

    #use the formatted time to format the output text
    output_text1 =  "If you would like to take taxi, the travel Time is {}".format(output_time)

    v1 = ls.map_to_area_id("data/subwaystation.csv",startCoord)
    v2 = ls.map_to_area_id("data/subwaystation.csv", endCoord)
    print("v1+v2")
    print(v1)
    print(v2)
    sub_time = PTT.get_travel_time(str(v1),str(v2))
    output_time2 = logic.format_travel_time(sub_time)
    output_text2 = "If you would like to take subway and ride citibike, the travel Time is {}".format(output_time2)


    return render_template("Output.html", taxi = output_text1,sub = output_text2)
