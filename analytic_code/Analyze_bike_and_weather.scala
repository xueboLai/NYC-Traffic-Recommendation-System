val cleanbike=sc.textFile("spark-project/cleanedbike")

/*
cleanbike.take(1).foreach(println)
970,2018-01-01 13:50:57,2018-01-01 14:07:08,40.76727216,-73.99392888,40.74901271,-73.98848395,Subscriber,1992
*/

//map the time to four period
def maptime(s:String):Int={
if (s.toInt>=6 && s.toInt<=9) 1
else if (s.toInt>=10 && s.toInt<=16) 2
else if (s.toInt>=17 && s.toInt<=20) 3
else  4
}

//change the record format, map the time to 1-4 using maptime
val formatbike=cleanbike.map(_.split(',')).map(values=>(values(0),values(1).split(' ')(0),maptime(values(1).split(' ')(1).split(':')(0)),values(3),values(4),values(5),values(6),values(7),values(8)))

/*
formatbike.take(1).foreach(println)
(970,2018-01-01,2,40.76727216,-73.99392888,40.74901271,-73.98848395,Subscriber,1992)
*/

//change the duration from seconds to minutes, add the price column(3 dollars per 30 mins)
val newbike=formatbike.map(tp=>(tp._1.toDouble/60,((tp._1.toInt/1800)+1)*3,tp._2, tp._3,tp._4,tp._5,tp._6,tp._7,tp._8,tp._9))

/*
newbike.take(1).foreach(println)
(16.166666666666668,3,2018-01-01,2,40.76727216,-73.99392888,40.74901271,-73.98848395,Subscriber,1992)
*/

//change the result to string and remove the parentheses
val savedata = newbike.map(_.toString).map(s=>s.substring(1,s.length-1))

/*
savedata.take(1).foreach(println)
16.166666666666668,3,2018-01-01,2,40.76727216,-73.99392888,40.74901271,-73.98848395,Subscriber,1992
*/
savedata.saveAsTextFile("spark-project/finalbike")

val bikedata= sc.textFile("spark-project/finalbike")

val weatherdata= sc.textFile("spark-project/finalweather")

/*
weatherdata.take(1).foreach(println)
2018-01-20,3,2,F,F
*/

//make (date,time) as the key
val bikemap= bikedata.map(_.split(',')).map(values=>((values(2),values(3)),(values(0),values(1),values(4),values(5),values(6),values(7),values(8),values(9))))

/*
bikemap.take(1).foreach(println)
((2018-01-01,2),(16.166666666666668,3,40.76727216,-73.99392888,40.74901271,-73.98848395,Subscriber,1992))
*/

//make (date,time) as the key
val weathermap=weatherdata.map(_.split(',')).map(values=>((values(0),values(1)),(values(2),values(3),values(4))))

/*
weathermap.take(1).foreach(println)
((2018-01-20,3),(2,F,F))
*/

//join bike and weather data
val joindata = bikemap.join(weathermap)

/*
joindata.take(1).foreach(println)
((2018-01-14,1),((5.2,3,40.72243797,-74.00566443,40.718822,-73.99596,Subscriber,1968),(1,F,F)))
*/

//after join the two datasets, make pair Rdd to a tuple
val newdata=joindata.map(tp=>(tp._1._1,tp._1._2,tp._2._1._1,tp._2._1._2,tp._2._1._3,tp._2._1._4,tp._2._1._5,tp._2._1._6,tp._2._1._7,tp._2._1._8,tp._2._2._1,tp._2._2._2,tp._2._2._3))

/*
newdata.take(1).foreach(println)
(2018-01-14,1,5.433333333333334,3,40.76727216,-73.99392888,40.76030096,-73.99884222,Subscriber,1965,1,F,F)
*/

val savedata = newdata.map(_.toString).map(s=>s.substring(1,s.length-1))

/*
savedata.take(1).foreach(println)
2018-01-14,1,5.2,3,40.72243797,-74.00566443,40.718822,-73.99596,Subscriber,1968,1,F,F
*/

savedata.saveAsTextFile("spark-project/bikeJoinWeather")

val joindata = sc.textFile("spark-project/bikeJoinWeather").map(_.split(','))

//calculate the distance from longitude and latitude
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

//calculate the distance from longitude and latitude using distance()
val newjoindata= joindata.map(values=>(values(0),values(1),values(2),values(3),distance(values(4).toDouble,values(5).toDouble,values(6).toDouble,values(7).toDouble),values(10),values(11),values(12)))

/*
newjoindata.take(1).foreach(println)
(2018-01-14,1,5.2,3,0.9123513942665453,1,F,F)
*/

//using distance and duration to calculate velocity
val addvelocity = newjoindata.map(tp=>(tp._1,tp._2,tp._3,tp._4,tp._5,tp._5*1000/(tp._3.toDouble*60),tp._6,tp._7,tp._8))

/*
addvelocity.take(1).foreach(println)
(2018-01-14,1,5.2,3,0.9123513942665453,2.9242031867517477,1,F,F)
*/

//some latitude and longitude has error, remove the records with distance that values "NaN" 
val filterdata = addvelocity.filter(tp=>tp._5.toString.contains('.'))

//change the format to array
val join=filterdata.map(tp=>Array(tp._1,tp._2,tp._3.toDouble,tp._4.toInt,tp._5,tp._6,tp._7,tp._8,tp._9))

case class X(startDate:String,startTime:String,duration:Double,price:Int, distance: Double, velocity:Double, temp:String,prep:String,wind:String)

val df_join = join.map{case Array(startDate:String,startTime:String,duration:Double,price:Int,distance: Double,velocity:Double,temp:String,prep:String,wind:String)=>X(startDate,startTime,duration,price,distance,velocity,temp,prep,wind)}.toDF()

df_join.show(3)
/*
+----------+---------+-----------------+-----+------------------+------------------+----+----+----+
| startDate|startTime|         duration|price|          distance|          velocity|temp|prep|wind|
+----------+---------+-----------------+-----+------------------+------------------+----+----+----+
|2018-01-14|        1|              5.2|    3|0.9123513985256244|2.9242032004026424|   1|   F|   F|
|2018-01-14|        1|             4.95|    3|1.1621608965547416|3.9129996516994665|   1|   F|   F|
|2018-01-14|        1|9.316666666666666|    3| 1.650876806553045|2.9532679902558945|   1|   F|   F|
+----------+---------+-----------------+-----+------------------+------------------+----+----+----+
*/

df_join.registerTempTable("bikeWeather") 

//select average velocity
sqlContext.sql("""SELECT AVG(velocity) AS avg_vel FROM bikeWeather""").show()
/*
+-----------------+                                                             
|          avg_vel|
+-----------------+
|2.469360812809856|
+-----------------+
*/

//select avg_dist, avg_duration, avg_vel, avg_price group by startTime
sqlContext.sql("""SELECT startTime, AVG(distance) AS avg_dist,AVG(duration) AS avg_duration,AVG(velocity) AS avg_vel,AVG(price) AS avg_price FROM bikeWeather GROUP BY startTime """).show()
/*
+---------+------------------+------------------+------------------+------------------+
|startTime|          avg_dist|      avg_duration|           avg_vel|         avg_price|
+---------+------------------+------------------+------------------+------------------+
|        1|1.8970223164609383|12.296438037499346|2.7438059770457337| 3.189483095996304|
|        2|1.7005979839833851| 14.90948458840202|2.3057757769371805|3.3750584338047536|
|        3|1.8479048057931162| 13.82405559768766| 2.468751454948881| 3.274956838219649|
|        4|1.7182997114067997|12.927153258556045|2.5374683030957486|3.2525090377718175|
+---------+------------------+------------------+------------------+------------------+
*/

//select avg_dist, avg_duration, avg_vel, avg_price group by temp
sqlContext.sql("""SELECT temp,AVG(distance) AS avg_dist,AVG(duration) AS avg_duration,AVG(velocity) AS avg_vel,AVG(price) AS avg_price FROM bikeWeather GROUP BY temp """).show()
/*
+----+------------------+------------------+------------------+------------------+
|temp|          avg_dist|      avg_duration|           avg_vel|         avg_price|
+----+------------------+------------------+------------------+------------------+
|   1|1.6060379505234024|11.235677275541057|2.6029003145246437|3.1543519797147686|
|   2|1.8091342871612142|13.980702508904693|2.4702611997862296|3.2995140564789596|
|   3| 1.824683120190276| 15.32808407576151| 2.347473346958047|3.3856432286563107|
+----+------------------+------------------+------------------+------------------+
*/

////select avg_dist, avg_duration, avg_vel, avg_price group by prep
sqlContext.sql("""SELECT prep,AVG(distance) AS avg_dist,AVG(duration) AS avg_duration,AVG(velocity) AS avg_vel,AVG(price) AS avg_price FROM bikeWeather GROUP BY prep """).show()
/*
+----+-----------------+------------------+------------------+------------------+
|prep|         avg_dist|      avg_duration|           avg_vel|         avg_price|
+----+-----------------+------------------+------------------+------------------+
|   F| 1.78883480936089|13.849900687206476| 2.469384264639185| 3.294514382900235|
|   T|1.715716668399982|13.298767636620157|2.4676743199594315|3.2682235258411736|
+----+-----------------+------------------+------------------+------------------+
*/

//select avg_dist, avg_duration, avg_vel, avg_price group by wind
sqlContext.sql("""SELECT wind,AVG(distance) AS avg_dist,AVG(duration) AS avg_duration,AVG(velocity) AS avg_vel,AVG(price) AS avg_price FROM bikeWeather GROUP BY wind """).show()
/*
+----+------------------+------------------+------------------+------------------+
|wind|          avg_dist|      avg_duration|           avg_vel|         avg_price|
+----+------------------+------------------+------------------+------------------+
|   F|1.8243021899900413|14.229954673234507|2.4578153702108008|3.3145215277457623|
|   T|1.6974869578996914| 12.88213616258726|2.4979615238494954| 3.243698280415517|
+----+------------------+------------------+------------------+------------------+
*/

//select avg_dist, avg_duration, avg_vel, avg_price group by temp,prep
sqlContext.sql("""SELECT temp,prep,AVG(distance) AS avg_dist,AVG(duration) AS avg_duration,AVG(velocity) AS avg_vel,AVG(price) AS avg_price FROM bikeWeather GROUP BY temp,prep """).show()
/*
+----+----+------------------+------------------+------------------+------------------+
|temp|prep|          avg_dist|      avg_duration|           avg_vel|         avg_price|
+----+----+------------------+------------------+------------------+------------------+
|   1|   F|1.6062629255885472|  11.2332978776911|2.6036036810357497|3.1542464377254653|
|   1|   T|1.2636945493521443|14.856394763343381|1.5325911070318845|3.3149546827794563|
|   2|   F|1.8108102615126846|  13.9934324929583| 2.470213475918267|3.3000956464489244|
|   2|   T|1.7182186323620536|13.290146096272496|2.4728500500878834| 3.267964866661371|
|   3|   F| 1.824683120190276|15.328084075761506|2.3474733469580458|3.3856432286563107|
+----+----+------------------+------------------+------------------+------------------+
*/

//select avg_dist, avg_duration, avg_vel, avg_price group by temp,prep,wind
sqlContext.sql("""SELECT temp,prep,wind,AVG(distance) AS avg_dist,AVG(duration) AS avg_duration,AVG(velocity) AS avg_vel,AVG(price) AS avg_price, Count(*) FROM bikeWeather GROUP BY temp,prep,wind """).show()
/*
+----+----+----+------------------+------------------+------------------+------------------+-------+
|temp|prep|wind|          avg_dist|      avg_duration|           avg_vel|         avg_price|    _c7|
+----+----+----+------------------+------------------+------------------+------------------+-------+
|   2|   T|   F|1.7364816646228163|13.826226912156441|2.4278212149334033| 3.299464332757662| 170815|
|   2|   T|   T|1.6726024444983632|11.951159072741822| 2.585320081042327|3.1892875943147923|  68388|
|   1|   T|   T|1.2636945493521443|14.856394763343381|1.5325911070318845|3.3149546827794563|   1324|
|   3|   F|   F|1.8242523701820959|15.264140330150285|2.3530115955341935| 3.381251663080491|2221029|
|   3|   F|   T|1.8358901235981346| 16.99173529193563|2.2033824137305347|3.4999004299085126|  85367|
|   2|   F|   F|1.8438707369990537|14.266123017731239|2.4681623737810066|3.3137705481126227|9327864|
|   2|   F|   T|1.7262764136635023|13.296177568704074|2.4754580331559777| 3.265129662457933|3648049|
|   1|   F|   F|1.6080819896318357| 10.91684544995875|2.6404045109525853| 3.135480971396395| 774308|
|   1|   F|   T|1.6051274066648737|11.430837772704212| 2.580631406402688| 3.165960451977401|1240416|
+----+----+----+------------------+------------------+------------------+------------------+-------+
*/

sqlContext.sql("""SELECT startTime,temp,prep,wind,AVG(velocity) As Avg_velocity FROM bikeWeather GROUP BY startTime,temp,prep,wind """).show(100)
/*
+---------+----+----+----+------------------+                                   
|startTime|temp|prep|wind|      Avg_velocity|
+---------+----+----+----+------------------+
|        4|   2|   T|   F|2.4765044892512096|
|        2|   1|   F|   F|2.4907817457196666|
|        4|   2|   T|   T| 2.747594015644973|
|        2|   1|   F|   T| 2.436768960334458|
|        3|   2|   F|   F|2.4610921546273117|
|        1|   2|   T|   F|2.6915844025018383|
|        3|   2|   F|   T| 2.488845241569071|
|        4|   3|   F|   F|  2.44862733954025|
|        1|   2|   T|   T|  2.73238817189107|
|        3|   1|   F|   F|2.6158490277218056|
|        3|   1|   F|   T| 2.545048493783494|
|        4|   2|   F|   F|2.5043991962451315|
|        1|   3|   F|   F|2.7223654958248003|
|        2|   2|   T|   F| 2.332034445203353|
|        4|   2|   F|   T|2.5752111256650623|
|        2|   2|   T|   T|2.3949505881292654|
|        4|   1|   F|   F|2.7051531130539472|
|        1|   2|   F|   F| 2.749400998026502|
|        4|   1|   F|   T| 2.647109086128926|
|        1|   2|   F|   T|2.7385223582066534|
|        2|   3|   F|   F|2.2471107348936536|
|        3|   2|   T|   F| 2.454509682446299|
|        1|   1|   F|   F| 2.748529316852665|
|        2|   3|   F|   T|2.2033824137305347|
|        3|   2|   T|   T|2.5630741812969156|
|        1|   1|   F|   T| 2.739111995444019|
|        2|   2|   F|   F|2.2888743943391945|
|        2|   2|   F|   T| 2.331423352185823|
|        3|   1|   T|   T|1.5325911070318845|
|        3|   3|   F|   F|2.3934825981126746|
+---------+----+----+----+------------------+
*/

val output = sqlContext.sql("""SELECT startTime,temp,prep,wind,AVG(velocity) As Avg_velocity FROM bikeWeather GROUP BY startTime,temp,prep,wind """)

output.rdd

val rows = output.rdd

rows.saveAsTextFile("spark-project/velocityresult")



