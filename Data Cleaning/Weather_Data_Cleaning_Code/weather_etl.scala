val data = sc.textFile("file:///home/zh1272/weather.csv")
val title = data.top(1)
val title_ = sc.parallelize(title)
val info = data.subtract(title_)
val info1 = info.map(line => line.split(','))
val info2 = info1.map(line => (line(1),line(34),line(43),line(44),line(56)))


val col1 = info2.map(line => line._1)
val col2 = info2.map(line => line._2)
val col3 = info2.map(line => line._3)
val col4 = info2.map(line => line._4)
val col5 = info2.map(line => line._5)



col2.distinct().collect()
def func2(line: String) : Any = {
	if (line.length == 0)  0.0
    else if (line == "\"T\"")  null
    else  line.substring(1,line.length-1).toDouble
}
val col2_ = col2.map(func2)
col2_.distinct().collect()



col3.distinct().collect()
def func3(line: String): Any = {
	if (line.length == 0) null
    else if (line.length == 5) line.substring(1,line.length-2).toInt
    else line.substring(1,line.length-1).toInt
}
val col3_ = col3.map(func3)
col3_.distinct().collect()



col4.distinct().collect()
def func4(line: String): Any = { 
	if (line.length == 0 || line.length == 3)  null 
    else if (line.length == 7)  line.substring(1,line.length-2).toDouble 
    else  line.substring(1,line.length-1).toDouble 
}
val col4_ = col4.map(func4)
col4_.distinct().collect()


col5.distinct().collect()
def func5(line: String): Any = 
{   
	if (line.length == 0)  null 
	else  line.substring(1,line.length-1).toInt
}
val col5_ = col5.map(func5)
col5_.distinct().collect()



def func(line:(String, String, String, String, String)): (String, String ,Any,Any,Any,Any) =
{
	val date_time: String = line._1.substring(1, line._1.length()-1)
	val date: String = date_time.split('T')(0)
	val time: String = date_time.split('T')(1)
	(date, time,func2(line._2), func3(line._3), func4(line._4), func5(line._5))
}


val info3 = info2.map(func)
val info4 = info3.map(line => line.toString()).filter(line => !(line.contains("null")))
val clean_data = info4.map(line => line.toString().substring(1,line.length-1))
clean_data.saveAsTextFile("/user/zh1272/Spark_project/weather")
