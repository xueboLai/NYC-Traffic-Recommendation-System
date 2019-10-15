//read in data
val yellow_taxi = sc.textFile("/user/xl1638/final_proj/yellow/*")

val yellow_taxi_no_header = yellow_taxi.filter(line=>line.length>=1).filter(line=>line.charAt(0)!='V')

val yellow_taxi_csv = yellow_taxi_no_header.map(line=>line.replace(" ",",")).map(line=>line.split(","))

val yellow_taxi_needed_column = yellow_taxi_csv.map(line=>Array(line(1),line(2),line(3),line(4),line(6),line(9),line(10),line(18)))

//case class X(startDate:String,startTime:String,endDate:String,endTime:String,tripDist:Double,pickup:Int,dropoff:Int,amount:Double)

case class X(startDate:String,startTime:String,endDate:String,endTime:String,tripDist:String,pickup:String,dropoff:String,amount:String)

val df = yellow_taxi_needed_column.map{case Array(s0,s1,s2,s3,s4,s5,s6,s7)=>X(s0,s1,s2,s3,s4,s5,s6,s7)}.toDF()

//use udf
import org.apache.spark.sql.functions._

//change type
val toInt    = udf[Int, String]( _.toInt)
val toDouble = udf[Double, String]( _.toDouble)

val df_1 = df.withColumn("tripDist_c",toDouble(df("tripDist"))).drop("tripDist").withColumnRenamed("tripDist_c","tripDist").withColumn("amount_c",toDouble(df("amount"))).drop("amount").withColumnRenamed("amount_c","amount");



//calculate travel duration
import org.joda.time.DateTime
import org.joda.time.Interval
import org.joda.time.Duration

def calculateTime = udf((startDate:String, startTime: String,endDate:String,endTime:String) => {

  if((startDate.trim()+startTime.trim()).compareTo((endDate.trim()+endTime.trim()))>0){
  	0;
  }else{
  try{
  val startDateArr:Array[String] = startDate.trim().split("-");
  val startYear:Int = startDateArr(0).toInt;
  val startMonth:Int = startDateArr(1).toInt;
  val startDay:Int = startDateArr(2).toInt;
  val startTimeArr:Array[String] = startTime.trim().split(":");
  val startHour:Int = startTimeArr(0).toInt;
  val startMinute:Int = startTimeArr(1).toInt;
  val startSecond:Int = startTimeArr(2).toInt;
  val startT:org.joda.time.DateTime = new DateTime(startYear,startMonth,startDay,startHour,startMinute,startSecond);
  
  val endDateArr:Array[String]  = endDate.trim().split("-");
  val endYear:Int = endDateArr(0).toInt;
  val endMonth:Int = endDateArr(1).toInt;
  val endDay:Int = endDateArr(2).toInt;
  val endTimeArr:Array[String]  = endTime.trim().split(":");
  val endHour:Int = endTimeArr(0).toInt;
  val endMinute:Int = endTimeArr(1).toInt;
  val endSecond:Int  = endTimeArr(2).toInt;
  val endT = new DateTime(endYear,endMonth,endDay,endHour,endMinute,endSecond);
  
  val interval = new Interval(startT, endT);
  
  val duration = interval.toDuration();
  
  val duration_in_sec =  duration.getStandardSeconds();
  
  duration_in_sec;
  }
  
  catch{
     case x: Exception   => 0;
  }
  }
} )

val df_2 = df_1.withColumn("tripDuration",calculateTime(df("startDate"),df("startTime"),df("endDate"),df("endTime")))



//time mapping
def maptime (s:String):String={
val s_1 = s.substring(0,2);
if (s_1.toInt>=6 && s_1.toInt<=9) 1.toString
else if (s_1.toInt>=10 && s_1.toInt<=16) 2.toString
else if (s_1.toInt>=17 && s_1.toInt<=20) 3.toString
else  4.toString
}

//transform time
val transform = udf[String, String](maptime(_))

val df_3 = df_2.withColumn("startTime_c",transform(df("startTime"))).drop("startTime").withColumnRenamed("startTime_c","startTime")
val df_4_1 = df_3.withColumn("endTime_c",transform(df("endTime"))).drop("endTime").withColumnRenamed("endTime_c","endTime");



def calculateVelocity = udf((tripDist:Double, tripDuration:Long) => {
val zero:Long = 0
if(tripDuration==zero){
0;}else{
val vel = tripDist*(1609.344)/(tripDuration.toDouble);vel;
}
} )

//def calculateVelocity(tripDist:Double, tripDuration:Long) ={
//val vel = tripDist/(tripDuration.toDouble);vel;
//} 

//drop all the row that contains at least one null/empty entry
val df_valid_taxi = df_4_1.na.drop()


df_valid_taxi.registerTempTable("taxi") 

val df_4 = sqlContext.sql("""SELECT * FROM taxi WHERE (tripDuration>0 AND amount>0 AND tripDist>0)""")



val df_5 = df_4.withColumn("velocity",calculateVelocity(df_4("tripDist"),df_4("tripDuration")))

//check data
df_5.show

//regain taxi 
val df_taxi = df_5

//convert the data back to rdd
val rows = df_5.rdd

//output the data to hdfs
rows.saveAsTextFile("/user/xl1638/spark_final_proj/yellow_taxi_final_version")





//read in weather data
val weather_raw = sc.textFile("/user/xl1638/spark_final_proj/weatherd1/data1.txt")

val weather_csv = weather_raw.filter(line=>line.length>10).map(line=>line.split(","))

val weather_arr = weather_csv.map(line=>Array(line(0),line(1),line(2),line(3),line(4)))


case class X(date:String,time:String,temp:String,precp:String,wind:String)

val df_weather = weather_arr.map{case Array(s0,s1,s2,s3,s4)=>X(s0,s1,s2,s3,s4)}.toDF()


//combine query
df_taxi.registerTempTable("taxi") 
df_weather.registerTempTable("weather")

val df_combined = sqlContext.sql("""SELECT * FROM taxi,weather WHERE (weather.date = taxi.startDate AND taxi.startTime = weather.time)""")


val df_combined_clean = df_combined.na.drop()
df_combined_clean.registerTempTable("taxiWeather")




#get the avg speed/amount/duration based on different weather/time/temp/precp/wind/pickup/dropoff
val agg_table = sqlContext.sql("""SELECT pickup,dropoff,time,temp,precp,wind,AVG(tripDist) AS avg_dist,AVG(tripDuration) AS avg_duration,AVG(velocity) AS avg_vel,AVG(amount) AS avg_price FROM taxiWeather GROUP BY pickup,dropoff,time,temp,precp,wind """)
agg_table.persist
val rows = agg_table.rdd

#save the data
rows.saveAsTextFile("/user/xl1638/spark_final_proj/taxi_weather_agg_table_output")

/*
+------+-------+----+----+-----+----+------------------+------------------+------------------+------------------+
|pickup|dropoff|time|temp|precp|wind|          avg_dist|      avg_duration|           avg_vel|         avg_price|
+------+-------+----+----+-----+----+------------------+------------------+------------------+------------------+
|   237|    234|   3|   1|    F|   F|2.4653122326775017|1208.5072711719417|3.8996201590201065|16.501804961505556|
|    68|    137|   3|   1|    F|   F|1.7248993288590606| 877.3333333333334|3.3420432495696053|13.775794183445194|
|   132|     87|   3|   1|    F|   F|21.145729729729734| 3459.889189189189|11.860010907221218| 66.17245945945945|
|   125|     50|   3|   1|    F|   F| 3.318478260869565|1020.5326086956521|5.6823841138234075|18.237499999999997|
|   100|    125|   3|   1|    F|   F|2.4217197452229295|1496.7324840764331|4.3025452744931565|15.161592356687901|
|   234|    112|   3|   1|    F|   F| 5.066691176470589|1464.8088235294117| 5.843622385139809|28.226985294117643|
|   229|     79|   3|   1|    F|   F|2.5029043183742585| 836.6782387806943| 5.134781031636641|14.796723116003385|
|   161|    151|   3|   1|    F|   F|3.4843459915611814|1075.9852320675107| 5.396919435009334| 18.62481012658228|
|    50|    107|   3|   1|    F|   F|2.9005319148936173| 1207.622340425532| 4.102884007366008|  18.0968085106383|
|   162|    231|   3|   1|    F|   F| 4.381483007209063|1682.0978372811535| 4.911888978427007|23.627785787847575|
|    48|    234|   3|   1|    F|   F|1.9560848601735776|1019.1764705882352|3.9740735326010466|13.742719382835102|
|   246|    261|   3|   1|    F|   F| 3.581428571428571|1097.3690476190477| 5.506288813960221|  19.2472619047619|
|   186|    262|   3|   1|    F|   F| 4.043827433628319|1614.6438053097345| 4.737704637148271|21.929712389380533|
|   144|     13|   3|   1|    F|   F|1.8261651917404131|  752.811209439528| 4.083583733101931|12.937846607669618|
|   236|    132|   3|   1|    F|   F|19.009236111111115|          2886.875|11.524251855331013| 69.56284722222223|
|   263|    138|   3|   1|    F|   F|           8.35625| 1416.419642857143|10.599894466097714| 39.17991071428571|
|    90|    224|   3|   1|    F|   F|1.3646349206349209| 728.5079365079365|3.1742360151464233|12.119714285714286|
|    13|    261|   3|   1|    F|   F|0.7903517587939699| 380.9246231155779|3.8157033606594566| 8.707386934673366|
|    90|      4|   3|   1|    F|   F| 2.061854838709677| 1318.258064516129|3.4645733321169523| 15.09467741935484|
|   229|    213|   3|   1|    F|   F|             11.11|            2305.5| 8.696262489280098|             39.93|
+------+-------+----+----+-----+----+------------------+------------------+------------------+------------------+
*/



agg_table.registerTempTable("agg")



#get the overall average speed 
sqlContext.sql("""SELECT AVG(velocity) AS avg_vel FROM taxiWeather""").show()
/*
+-----------------+                                                             
|          avg_vel|
+-----------------+
|5.867289859328089|
+-----------------+
*/



Giving all the area the same weight:

//nyc different time's speed
//Agg_table_by_time, show the difference for time

val agg_speed_by_time = sqlContext.sql("""SELECT time,AVG(avg_dist) AS avg_dist,AVG(avg_duration) AS avg_duration,AVG(avg_vel) AS avg_vel,AVG(avg_price) AS avg_price FROM agg GROUP BY time """)
agg_speed_by_time.persist
val rows = agg_speed_by_time.rdd
rows.saveAsTextFile("/user/xl1638/spark_final_proj/taxi_weather_agg_speed_by_time")
/*
+----+-----------------+------------------+------------------+------------------+
|time|         avg_dist|      avg_duration|           avg_vel|         avg_price|
+----+-----------------+------------------+------------------+------------------+
|   1|8.310702518410558| 2030.656879499956|  8.89981733542986|  33.1080036151164|
|   2|8.966825598049981|2305.2223975107418| 8.470138407059276| 36.68992753547898|
|   3|8.437382261131765|2209.5996459789385| 8.143843109287866|36.139820553343434|
|   4|8.813156699415684|1687.8027090881326|11.856754882057936|40.330001154961494|
+----+-----------------+------------------+------------------+------------------+
*/


//agg_table_by_temp:
#nyc different temperature's speed
//Agg_table_by_time, show the difference for time
val agg_speed_by_temp = sqlContext.sql("""SELECT temp,AVG(avg_dist) AS avg_dist,AVG(avg_duration) AS avg_duration,AVG(avg_vel) AS avg_vel,AVG(avg_price) AS avg_price FROM agg GROUP BY temp """)
agg_speed_by_temp.persist
val rows = agg_speed_by_temp.rdd
rows.saveAsTextFile("/user/xl1638/spark_final_proj/taxi_weather_agg_speed_by_temp")
agg_speed_by_temp.show
/*
+----+-----------------+------------------+-----------------+------------------+
|temp|         avg_dist|      avg_duration|          avg_vel|         avg_price|
+----+-----------------+------------------+-----------------+------------------+
|   1|8.433260652248853|2011.1708313101176|9.708598389058764| 34.41044724770314|
|   2|8.911589873899493| 2103.744753058433|9.320050800043314|38.785954064523175|
|   3|8.194596119983334|2029.2039578891122| 8.76731525767587| 33.68735004966567|
+----+-----------------+------------------+-----------------+------------------+
*/



//agg_table_by_percp:
#nyc different temperature's speed
//Agg_table_by_time, show the difference for time
val agg_table_by_precp = sqlContext.sql("""SELECT precp,AVG(avg_dist) AS avg_dist,AVG(avg_duration) AS avg_duration,AVG(avg_vel) AS avg_vel,AVG(avg_price) AS avg_price FROM agg GROUP BY precp """)
agg_table_by_precp.persist
val rows = agg_table_by_precp.rdd
rows.saveAsTextFile("/user/xl1638/spark_final_proj/taxi_weather_agg_speed_by_percp")
agg_table_by_precp.show
/*
+-----+-----------------+------------------+-----------------+------------------+
|precp|         avg_dist|      avg_duration|          avg_vel|         avg_price|
+-----+-----------------+------------------+-----------------+------------------+
|    F|8.965411178504262|2073.5115738590284|9.557733103919457|35.694988430292554|
|    T|6.768885235333588| 2009.830241437861|8.101741239803063|42.971064586906536|
+-----+-----------------+------------------+-----------------+------------------+
*/

//agg_table_by_wind:
//nyc different speed based on wind
//Agg_table_by_time, show the difference for time
val agg_table_by_wind = sqlContext.sql("""SELECT wind,AVG(avg_dist) AS avg_dist,AVG(avg_duration) AS avg_duration,AVG(avg_vel) AS avg_vel,AVG(avg_price) AS avg_price FROM agg GROUP BY wind """)
agg_table_by_wind.persist
val rows = agg_table_by_wind.rdd
rows.saveAsTextFile("/user/xl1638/spark_final_proj/taxi_weather_agg_speed_by_wind")
agg_table_by_wind.show
/*
+----+----------------+------------------+-----------------+------------------+
|wind|        avg_dist|      avg_duration|          avg_vel|         avg_price|
+----+----------------+------------------+-----------------+------------------+
|   F|8.68457086893617|2016.6794416762625|9.318842514908072|37.912269999063234|
|   T| 8.6297390026973| 2125.702433746377|9.402292639671353| 35.17016180545505|
+----+----------------+------------------+-----------------+------------------+
*/

//agg_table_by_temperature and precp:
//nyc different speed based on temp and precp
//Agg_table_by_time, show the difference for time
val agg_table_by_temp_precp = sqlContext.sql("""SELECT temp,precp,AVG(avg_dist) AS avg_dist,AVG(avg_duration) AS avg_duration,AVG(avg_vel) AS avg_vel,AVG(avg_price) AS avg_price FROM agg GROUP BY temp,precp """)
agg_table_by_temp_precp.persist
val rows = agg_table_by_temp_precp.rdd
rows.saveAsTextFile("/user/xl1638/spark_final_proj/taxi_weather_agg_speed_by_temp_precp_real")
agg_table_by_temp_precp.show
/*
+----+-----+-----------------+------------------+-----------------+------------------+
|temp|precp|         avg_dist|      avg_duration|          avg_vel|         avg_price|
+----+-----+-----------------+------------------+-----------------+------------------+
|   1|    F|8.511428860414155| 1958.487893891502|9.858824056603499| 34.17128477325226|
|   1|    T|5.434161619007589|4032.4701598857737|3.944852937087714|43.586453232084374|
|   2|    F|9.559638558038882| 2170.431548082336|9.626015084262756| 37.48355605068903|
|   2|    T|6.847101911296705|1891.3007053180322|8.345340724678696|42.935001949172005|
|   3|    F|8.194596119983332|2029.2039578891104|8.767315257675865|33.687350049665675|
+----+-----+-----------------+------------------+-----------------+------------------+
*/

//output the aggregated result to hdfs
val rows = df_combined_clean.rdd
rows.saveAsTextFile("/user/xl1638/spark_final_proj/taxi_weather_agg_clean")


//agg_speed_by_temp_precp_wind:
//nyc different speed based on temp and precp
//Agg_table_by_time, show the difference for time
val agg_table_by_temp_precp_wind = sqlContext.sql("""SELECT temp,precp,wind,AVG(avg_dist) AS avg_dist,AVG(avg_duration) AS avg_duration,AVG(avg_vel) AS avg_vel,AVG(avg_price) AS avg_price FROM agg GROUP BY temp,precp,wind """)
agg_table_by_temp_precp_wind.persist
val rows = agg_table_by_temp_precp_wind.rdd
rows.saveAsTextFile("/user/xl1638/spark_final_proj/taxi_weather_agg_speed_by_temp_precp_wind")
agg_table_by_temp_precp_wind.show
/*
+----+-----+----+-----------------+------------------+-----------------+------------------+
|temp|precp|wind|         avg_dist|      avg_duration|          avg_vel|         avg_price|
+----+-----+----+-----------------+------------------+-----------------+------------------+
|   2|    T|   F|7.099846190234474|1866.3369641717954|8.335847103661777|53.461506042766224|
|   2|    T|   T|6.521856627629071| 1923.425424835473| 8.35755764002751| 29.38891566647304|
|   1|    T|   T|5.434161619007588| 4032.470159885775|3.944852937087716|43.586453232084416|
|   3|    F|   F| 8.36708440061024|2027.4729493478396| 8.79745289200849|34.004073241327006|
|   3|    F|   T|6.986814548498685|2041.3246666620778|8.556288286899683|31.469620052242096|
|   2|    F|   F|9.746566832572318| 2162.048624734459|9.549592705094467|38.031789312596814|
|   2|    F|   T|9.340723050613855|2180.2489602581395|9.715514888163526| 36.84150891781176|
|   1|    F|   F|8.063841240005596|1840.2367939857306|9.993895134213686| 32.81112707820123|
|   1|    F|   T|8.867944364137083|2052.6780558316705| 9.75123633282187|35.254686734528796|
+----+-----+----+-----------------+------------------+-----------------+------------------+
*/



/*
//output table
df_combined_clean:/user/xl1638/spark_final_proj/taxi_weather_agg_clean
agg_table: /user/xl1638/spark_final_proj/taxi_weather_agg_table_output
agg_speed_by_time: /user/xl1638/spark_final_proj/taxi_weather_agg_speed_by_time
agg_speed_by_temp: /user/xl1638/spark_final_proj/taxi_weather_agg_speed_by_temp
agg_speed_by_percp: /user/xl1638/spark_final_proj/taxi_weather_agg_speed_by_percp
agg_speed_by_temp_percp: /user/xl1638/spark_final_proj/taxi_weather_agg_speed_by_temp_precp_real
*/

