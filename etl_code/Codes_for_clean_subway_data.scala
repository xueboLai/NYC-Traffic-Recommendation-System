val data = sc.textFile("file:///home/zh1272/subway.csv")

val title = data.first()

//remove the title in the records
val info = data.filter(line => line != title).filter(line => line != null)

val info1 = info.map(line => line.split(','))

val info2 = info1.map(line => (line(2),line(3),line(4)))

//select the fields we need
val info3 = info2.map(line => (line._1, line._2.split(' ')(1), line._2.split(' ')(2), line._3.split(' ')(0)))

//change the format of RDD
val info4 = info3.map(line => (line._1, line._2.substring(1), line._3.substring(1,line._3.length-1), line._4))

val info5 = info4.map(line => (line._1, line._2, line._3, line._4.split('-').toSet))

val clean_data = info5.map(line=> line.toString).map(line => line.substring(1,line.length-1))

info5.saveAsTextFile("/user/zh1272/Spark_project/subwaydata")