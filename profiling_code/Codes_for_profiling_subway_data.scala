val data = sc.textFile("file:///home/zh1272/subway.csv")

val title = data.first()

//remove the title for records
val info = data.filter(line => line != title)

val info1 = info.map(line => line.split(','))
val info2 = info1.map(line => (line(2),line(3),line(4)))

//extract each field
val col1 = info2.map(line => line._1)
val col2 = info2.map(line => line._2)
val col3 = info2.map(line => line._3)

//filter out the null values
val filt_col1 = col1.filter(line => line != null)
val filt_col2 = col2.filter(line => line != null)
val filt_col3 = col3.filter(line => line != null)

//using distinct detect whether there has error data
filt_col1.distinct().collect()
filt_col2.distinct().collect()
filt_col3.distinct().collect()   

//get the max and min length
val col1_len_max = filt_col1.map(line => line.length()).max()
val col1_len_min = filt_col1.map(line => line.length()).min()
val col3_len_max = filt_col3.map(line => line.length()).max()
val col3_len_min = filt_col3.map(line => line.length()).min()

val col2_1 = filt_col2.map(_.split(' ')).map(line => line(1)).map(line=>line.substring(1))
val col2_2 = filt_col2.map(_.split(' ')).map(line => line(2)).map(line=>line.substring(0,line.length-1))
val col2_1d = col2_1.map(_.split('.')).map(line =>line(1).length)
val col2_2d = col2_2.map(_.split('.')).map(line =>line(1).length)
val col2_lat = col2_1.map(_.toDouble)
val col2_lon = col2_2.map(_.toDouble)

//get the max and min value of latitude and longitude
col2_lat.max
col2_lat.min

col2_lon.max
col2_lat.min
