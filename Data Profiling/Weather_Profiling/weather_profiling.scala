val data = sc.textFile("file:///home/zh1272/weather.csv")
val title = data.top(1)
val title_ = sc.parallelize(title)
val field_name = title.map(line => line.split(',')).map(line => (line(0),line(34),line(43),line(44),line(56)))
val info = data.subtract(title_)
val info1 = info.map(line => line.split(','))
val info2 = info1.map(line => (line(1),line(34),line(43),line(44),line(56)))

// extract each field
val col1 = info2.map(line => line._1)
val col2 = info2.map(line => line._2)
val col3 = info2.map(line => line._3)
val col4 = info2.map(line => line._4)
val col5 = info2.map(line => line._5)





val col1_ = col1.filter(line => line != null)
col1.distinct().collect()
col1_.count
val col1_max = col1_.map(line => line.length).max()
val col1_min = col1_.map(line => line.length).min()


col2.distinct().collect()
val col2_fil = col2.filter(line => line != "\"T\"" )
def func2(line: String) : Double = {
    if (line.length == 0)
		0.0
    else
    	line.substring(1,line.length-1).toDouble
}
val col2_ = col2_fil.map(func2)

col2_.distinct().collect()
(col2_.max(), col2_.min())
col2_.count()



col3.distinct().collect()
val col3_fil = col3.filter(line => line.length != 0 )
def func3(line: String): Int = {
    if (line.length == 5)
    	line.substring(1,line.length-2).toInt
    else
    	line.substring(1,line.length-1).toInt
}
val col3_ = col3_fil.map(func3)
col3_.distinct().collect()
(col3_.max(), col3_.min())
col3_.count()



col4.distinct().collect()
val col4_fil = col4.filter(line => line.length != 0 ).filter(line => line.length != 3)
def func4(line: String): Double = { 
    if (line.length == 7)  line.substring(1,line.length-2).toDouble 
    else  line.substring(1,line.length-1).toDouble 
}
val col4_ = col4_fil.map(func4)
col4_.distinct().collect()
(col4_.max(), col4_.min())
col4_.count()


col5.distinct().collect()
val col5_fil = col5.filter(line => line.length != 0 )
def func5(line: String): Int = line.substring(1,line.length-1).toInt
val col5_ = col5_fil.map(func5)
col5_.distinct().collect()
(col5_.max(), col5_.min())
col5_.count()


