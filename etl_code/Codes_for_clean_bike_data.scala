val data = sc.textFile("Spark_project/citibike")

val title = data.first()

//remove the title
val info = data.filter( line => line != title).map(line => line.split(','))

//remove marks
def func( str: String) : String = {
str.substring(1,str.length-1);
}

//get select fields
val info1 = info.map(line => (line(0),func(line(1)),func(line(2)),line(5), line(6),line(9), line(10)))

//filter out all the null values
val info2 = info1.filter(line => (line._1 != null && line._2 != null && line._3 != null && line._4 != null && line._5 != null && line._6!= null && line._7 != null))

val info3 = info2.map(line => line.toString())

val info4 = info3.map(line =>line.substring(1,line.length-1))

info4.saveAsTextFile("Spark_Project/cleanedbike")

