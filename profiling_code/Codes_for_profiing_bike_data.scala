val data = sc.textFile("Spark_project/citibike")

//remove the tile of the records
val title = data.top(1)

val title_ = sc.parallelize(title)

val info = data.subtract(title_)

val bikedata = info.map(line => line.split(','))

//select the data fields that we need, we only need four columns(Duration: Int, StartTime: String, StopTime: String, StartLat:Double, StartLong: Double,EndLat:Double, EndLong: Double)
val selectdata = bikedata.map(line => (line(0),line(1),line(2),line(5),line(6),line(9),line(10)))

//remove records that has null value
val info2 = selectdata.filter(line => (line._1 != null && line._2 != null && line._3 != null && line._4 != null && line._5 != null && line._6 != null && line._7 != null))

//extract each field
val col1 = selectdata.map(line => line._1.toInt)
val col2 = selectdata.map(line => line._2.length)
val col3 = selectdata.map(line => line._3.length)
val col4 = selectdata.map(line => line._4.toDouble)
val col5 = selectdata.map(line => line._5.toDouble)
val col6 = selectdata.map(line => line._6.toDouble)
val col7 = selectdata.map(line => line._7.toDouble)

//calculate the max and min values for duration
col1.max
col1.min

//calculate the max and min length for StartTime and StopTime
col2.max
col2.min

col3.max
col3.min

//calculate the max and min values for start and end latitude ans longitude
col4.max
col4.min

col5.max
col5.min

col6.max
col6.min

col7.max
col7.min


