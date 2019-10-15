val data = sc.textFile("spark-project/weather_data.csv")

//remove the tile of the records
val title = data.top(1)

val title_ = sc.parallelize(title)

val info = data.subtract(title_)

val total = info.map(line => line.split(','))

//filter the records the for each hour, remove the records for sum of the day
val hour = total.filter(line=>line(2).contains("FM"))

//select the data fields that we need, we only need four columns(DateTime: String, Temperature: Int, Precipitation: Double, WindSpeed: Int)
val selectdata = hour.map(line => (line(1),line(43),line(44),line(56)))

//extract each field
val date = selectdata.map(line => line._1)
val temp = selectdata.map(line => line._2)
val prep = selectdata.map(line => line._3)
val wind = selectdata.map(line => line._4)

//collect the distinct data for each columns to see whether there has some exception data for each field
col1.distinct().collect()
col2.distinct().collect()
col3.distinct().collect()
col4.distinct().collect()

//there are some exception data in the fileds, we need to remove the records that have null values or have bad data
def removenull(line: String):Double = {
       if (line=="") -1.0
       else if (line=="\"T\"") 0.0
       else if (line.length==7||line.length==5) line.substring(1,line.length-2).toDouble
       else line.substring(1,line.length-1).toDouble
    }

//remove the marks for the date fields
def removemarks(s:String):String={
	s.substring(1,s.length-1)
}

val thedata= selectdata.map(fields=>(removemarks(fields._1),removenull(fields._2),removenull(fields._3),removenull(fields._4)))

val date = selectdata.map(line => line._1.length)
val temp = selectdata.map(line => line._2.toInt)
val prep = selectdata.map(line => line._3.toDouble)
val wind = selectdata.map(line => line._4.toInt)

//get the max and min length for DateTime
date.max
date.min

//get the max and min value for temp
temp.max
temp.min

//get the max and min value for prep
prep.max
prep.min

//get the max and min value for wind
wind.max
wind.min





