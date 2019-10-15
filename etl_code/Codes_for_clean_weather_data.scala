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

//there are some exception data in the fileds, we need to remove the records that have null values or have bad data
def removenull(line: String):Double = {
       if (line=="") -1.0
       else if (line=="\"T\"") 0.0
       else if (line.length==7||line.length==5) line.substring(1,line.length-2).toDouble
       else line.substring(1,line.length-1).toDouble
    }

//remove the marks for datetime
def removemarks(s:String):String={
	s.substring(1,s.length-1)
}

//remove the records that have null values or have bad data
val clean= selectdata.map(fields=>(removemarks(fields._1),removenull(fields._2),removenull(fields._3),removenull(fields._4)))

val stringdata=clean.map(_.toString)

val savedata=stringdata.map(s=>s.substring(1,s.length-1))

savedata.saveAsTextFile("spark-project/cleanweather")
