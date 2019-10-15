from datetime import timedelta



def format_travel_time(travel_time):
    # format the output string
    timeStr = str(timedelta(seconds=travel_time))
    timeArr = timeStr.split(":")
    print(timeStr)
    hr = int(timeArr[0])
    mi = int(timeArr[1])
    print(timeArr[2].split(".")[0])
    se = int(timeArr[2].split(".")[0])
    if (hr != 0):
        output_time = "{}-hour,{}-minute,{}-second.".format(hr, mi, se)
    elif (mi != 0):
        output_time = "{}-minute,{}-second.".format(mi, se)
    else:
        output_time = "{}-second.".format(se)
    return output_time