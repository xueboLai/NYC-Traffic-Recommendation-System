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
} )

val df_2 = df_1.withColumn("tripDuration",calculateTime(df("startDate"),df("startTime"),df("endDate"),df("endTime")))



//transform time
val transform = udf[String, String](maptime(_))
//time mapping
def maptime (s:String):String={
val s_1 = s.substring(0,2);
if (s_1.toInt>=6 && s_1.toInt<=9) 1.toString
else if (s_1.toInt>=10 && s_1.toInt<=16) 2.toString
else if (s_1.toInt>=17 && s_1.toInt<=20) 3.toString
else  4.toString
}

val df_3 = df_2.withColumn("startTime_c",transform(df("startTime"))).drop("startTime").withColumnRenamed("startTime_c","startTime")
val df_4 = df_3.withColumn("endTime_c",transform(df("endTime"))).drop("endTime").withColumnRenamed("endTime_c","endTime");



