val data = sc.textFile("Spark_Project/taxi_zones")

def clean(str : String): String = {
	var line = str
	if (str.contains(")")){
		line = str.split(')')(0)
	}
	line
}
val data1 = data.map(_.split("MULTIPOLYGON")).map(l => l(1)).map(l => l.substring(4,l.length-3)).map(line => clean(line))
val loc = data1.map(l => l.split(", "))
loc.persist()


// to alleviate the computing load =>
// select one from every five points to form the polygon
val loc_len = loc.map(line => line.length).collect // number of points define each polygon
val order_arr = (1 to 263).toArray
val order = sc.parallelize(order_arr, loc.getNumPartitions).zip(loc)


// flatMap helper
def generate_pair(line : (Int, Array[String])): Array[(Int, String)] = {
	
	val arr_len = line._2.length
	val pair = new Array[(Int,String)](arr_len)
			for (i <- 0 to arr_len-1){
			    val each: String = line._2(i)
			    pair(i) = (line._1,each)
			}
	pair
}
val pair_loc = order.map(generate_pair).flatMap(line => line)


// remove some points to speed
def select(line :Array[String]): Array[String] = {
	val len = line.length
	val loc_arr = line
	var num_ele: Int = 0
	if (len <= 100)
		num_ele = len
	if (len > 100 && len <= 500)
		num_ele = len/5
	if (len > 500 && len <= 1000)
		num_ele = len/10
	if (len > 1000)
		num_ele = len/20

	var selected_p: Array[String]= Array(loc_arr(0))
	val div: Int = len/num_ele
	var res1: Double = 0.0
	var res2: Int = 0
	for(i <- 0 to len-1){
		res1 = (i.toDouble)/(div.toDouble)
		res2 = i/div
		if(res1 - res2 == 0)
			selected_p :+= loc_arr(i)
	}

	selected_p
}


val sele_loc = loc.map(select)
val check = sele_loc.map(line =>(line.length, 1)).sortByKey()


// import esri library
import com.esri.core.geometry.Geometry
import com.esri.core.geometry.GeometryEngine
import com.esri.core.geometry._


/*----------get pair from string----------*/
def getpair( str: String):(Double, Double) = {
	val str1 = str.split(' ')
	(str1(0).toDouble, str1(1).toDouble)
}

/*------------turn to polygon -------------*/
def getPolygon(pointset: Array[String]): Polygon = {
	val len = pointset.length
	val first = getpair(pointset(0))	
	val poly: Polygon = new Polygon()
	
	poly.startPath(first._1, first._2)
	for(i <- 1 to len-1){
		var pair = getpair(pointset(i))
		poly.lineTo(pair._1, pair._2)
	}
	poly
}

/*------------- compare a point with each area-------------*/
val loc_arr = sele_loc.collect()
def func(lat: Double, long:Double): Int = {
	val area_num = 263
	var area:Int = 0
	for (i <- 0 to area_num-1){
		var poly: Polygon = getPolygon(loc_arr(i))
		var p: Point  = new Point(lat, long)
		var check: Boolean = GeometryEngine.contains(poly, p, SpatialReference.create(4326))
		if (check == true){
			area = i
		}
    }
    area
}

/*---------------map bike station---------------------*/
val bike = sc.textFile("Spark_Project/bikestation/part-000**").map(line => line.split(','))
val od_loc = bike.map(line => (line(0),line(1),line(2), line(5).toDouble, line(6).toDouble, line(9).toDouble, line(10).toDouble, line(11), line(12).toInt, line(13).toInt))
// map loc to area
val od_area = od_loc.map(line => (line._1, line._2, line._3, line._4, line._5, func(line._5, line._4), line._6, line._7, func(line._7, line._6), line._8, line._9, line._10))
//save
val file = od_area.map(line => line.toString).map(line => line.substring(1, line.length-1))
//val title = ("duration", "starttime","endtime","lat1", "long1","startarea","lat2","long2","endarea","usetype","birth","sex")
file.saveAsTextFile("Bike/bike_OD")


/*----------------map crime data to area-----------------------*/
val crime = sc.textFile("Spark_Project/crime/part-00000").map(l =>l.substring(1,l.length-1)).map(_.split(","))
val crime_pair = crime.map(line =>(func(line(3).toDouble, line(2).toDouble),1))
val crime_count = crime_pair.reduceByKey((v1,v2)=>v1+v2)
crime_count.saveAsTextFile("Spark_Project/crime_count1")
