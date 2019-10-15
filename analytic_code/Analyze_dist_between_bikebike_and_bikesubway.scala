val bikestation=sc.textFile("spark-project/bikestation").map(_.split(','))

/*
bikestation.take(3)
res0: Array[Array[String]] = Array(Array(72, 40.76727216, -73.99392888), Array(79, 40.71911552, -74.00666661), Array(82, 40.71117416, -74.00016545))
*/

val subwaystation=sc.textFile("spark-project/subwaystation").map(_.split(','))

/*
subwaystation.take(3)
res1: Array[Array[String]] = Array(Array(1, -73.99106999861966, 40.73005400028978), Array(2, -74.00019299927328, 40.71880300107709), Array(3, -73.98384899986625, 40.76172799961419))
*/

//calculate the cartesian for bike stations and subway stations
val cartesian = bikestation.cartesian(subwaystation)

/*
cartesian.take(3)
res3: Array[(Array[String], Array[String])] = Array((Array(72, 40.76727216, -73.99392888),Array(1, -73.99106999861966, 40.73005400028978)), (Array(72, 40.76727216, -73.99392888),Array(2, -74.00019299927328, 40.71880300107709)), (Array(72, 40.76727216, -73.99392888),Array(3, -73.98384899986625, 40.76172799961419)))
*/

// functions to calculate distance using latitude and longitude
def rad(d: Double): Double = d * Math.PI / 180.0

def distance(lat1: Double, lng1: Double, lat2: Double, lng2: Double): Double = {
  val EARTH_RADIUS = 6378.137
  val radLat1 = rad(lat1)
  val radLat2 = rad(lat2)
  val a = rad(lat1) - rad(lat2)
  val b = rad(lng1) - rad(lng2)
  val distance = EARTH_RADIUS * 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin((b) / 2), 2)))
  distance
}

//calculate distance bewteen each bike station and subway station using function distance
val bikesubway = cartesian.map(tp=>(tp._1(0),tp._2(0),distance(tp._1(1).toDouble,tp._1(2).toDouble,tp._2(2).toDouble,tp._2(1).toDouble)))

/*
bikesubway.take(3).foreach(println)
(72,1,4.15011578931334)
(72,2,5.421366215937114)
(72,3,1.0503233556066842)
*/

//same as above, just changethe order of bike station and subway station
val subwaybike = cartesian.map(tp=>(tp._2(0),tp._1(0),distance(tp._1(1).toDouble,tp._1(2).toDouble,tp._2(2).toDouble,tp._2(1).toDouble)))

/*
subwaybike.take(3).foreach(println)
(1,72,4.15011578931334)
(2,72,5.421366215937114)
(3,72,1.0503233556066842)
*/

bikesubway.map(_.toString).map(s=>s.substring(1,s.length-1)).saveAsTextFile("spark-project/bikesubwaydist")

subwaybike.map(_.toString).map(s=>s.substring(1,s.length-1)).saveAsTextFile("spark-project/subwaybikedist")

//the below calculate the distance between each bike stations just like above
val bikestation2=sc.textFile("spark-project/bikestation").map(_.split(','))

val cartesian2 = bikestation.cartesian(bikestation2)

val bikebike = cartesian2.map(tp=>(tp._1(0),tp._2(0),distance(tp._1(1).toDouble,tp._1(2).toDouble,tp._2(1).toDouble,tp._2(2).toDouble)))

/*
bikebike.take(3).foreach(println)
(72,72,0.0)
(72,79,5.467358988249316)
(72,82,6.266916333423275)
*/

bikebike.map(_.toString).map(s=>s.substring(1,s.length-1)).saveAsTextFile("spark-project/bikebikedist")
