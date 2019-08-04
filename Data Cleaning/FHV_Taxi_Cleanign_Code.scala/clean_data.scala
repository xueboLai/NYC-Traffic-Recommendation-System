Profiling:
//specify data path
val input_path = "/user/xl1638/spark_final_project/fhv/*"
//read in data
val fhv_data = sc.textFile(input_path)

//check the sample of the data
fhv_data.take(5).foreach(println)
/*
"Pickup_DateTime","DropOff_datetime","PUlocationID","DOlocationID","SR_Flag","Dispatching_base_number","Dispatching_base_num"
"2018-01-30 21:06:50","2018-01-30 21:15:34","56","129","","B02884",""
"2018-01-30 21:20:36","2018-01-30 21:35:29","129","112","","B02884",""
"2018-01-30 21:04:45","2018-01-30 21:16:34","47","42","","B02884",""
"2018-01-30 21:11:51","2018-01-30 21:40:35","49","131","","B02884",""
*/
//filter out header
val fhv_data_without_header = fhv_data.filter(line=>line.charAt(1)=='2')

//val replace “” with empty string
val clean_data = fhv_data_without_header.map(line=>line.replace("\"",""))

//csv_data
val csv_data = clean_data.map(line=>line.replace(" ",","))

//split the data
val splitted_data = csv_data.map(line=>line.split(","))

case class X(startDate: String, startTime:String,endDate:String,endTime:String,startLoc:String,endLoc:String,nouse1:String,nouse2:String)

val df = splitted_data.map{case Array(s0,s1,s2,s3,s4,s5,s6,s7)=>X(s0,s1,s2,s3,s4,s5,s6,s7)}.toDF()

df.show()
//sample data
/*
+----------+---------+----------+--------+--------+------+------+------+
| startDate|startTime|   endDate| endTime|startLoc|endLoc|nouse1|nouse2|
+----------+---------+----------+--------+--------+------+------+------+
|2018-01-30| 21:06:50|2018-01-30|21:15:34|      56|   129|      |B02884|
|2018-01-30| 21:20:36|2018-01-30|21:35:29|     129|   112|      |B02884|
|2018-01-30| 21:04:45|2018-01-30|21:16:34|      47|    42|      |B02884|
|2018-01-30| 21:11:51|2018-01-30|21:40:35|      49|   131|      |B02884|
|2018-01-30| 21:43:39|2018-01-30|21:49:59|      98|   121|      |B02884|
|2018-01-30| 21:36:53|2018-01-30|21:44:55|     235|   235|      |B02884|
|2018-01-30| 21:48:30|2018-01-30|21:51:30|     169|   235|      |B02884|
|2018-01-30| 21:55:18|2018-01-30|22:15:28|     235|   208|      |B02884|
|2018-01-30| 21:21:25|2018-01-30|21:46:49|     231|   265|      |B02884|
|2018-01-30| 21:24:37|2018-01-30|21:35:23|     123|    29|      |B02884|
|2018-01-30| 21:39:49|2018-01-30|21:49:52|      29|    21|      |B02884|
+----------+---------+----------+--------+--------+------+------+------+
*/
//clean the data

//drop the two no use columns
val df_clean = df.drop("nouse1").drop("nouse2")
//drop all the row that contains at least one null/empty entry
df_full = df_clean.na.drop()
//convert the data back to rdd
val rows = df.rdd
//output the data to hdfs
rows.saveAsTextFile("/user/xl1638/spark_final_proj/fhv_df_clean")














/*
Second way (standard but much slower):

val input_path = "/user/xl1638/spark_final_project/fhv/*"

val fhv_data = sc.textFile(input_path)

//check the sample of the data
fhv_data.take(5).foreach(println)
/*
"Pickup_DateTime","DropOff_datetime","PUlocationID","DOlocationID","SR_Flag","Dispatching_base_number","Dispatching_base_num"
"2018-01-30 21:06:50","2018-01-30 21:15:34","56","129","","B02884",""
"2018-01-30 21:20:36","2018-01-30 21:35:29","129","112","","B02884",""
"2018-01-30 21:04:45","2018-01-30 21:16:34","47","42","","B02884",""
"2018-01-30 21:11:51","2018-01-30 21:40:35","49","131","","B02884",""
*/
//filter out header
val fhv_data_without_header = fhv_data.filter(line=>line.charAt(1)=='2')

//val replace “” with empty string
val clean_data = fhv_data_without_header.map(line=>line.replace("\"",""))

//csv_data
val csv_data = clean_data.map(line=>line.replace(" ",","))

//split the data
val splitted_data = csv_data.map(line=>line.split(","))

//see the whether the data are what we intended to get
splitted_data.take(5).foreach(arr=>println(arr.toList))
/*
List(2018-01-30, 21:06:50, 2018-01-30, 21:15:34, 56, 129, , B02884)
List(2018-01-30, 21:20:36, 2018-01-30, 21:35:29, 129, 112, , B02884)
List(2018-01-30, 21:04:45, 2018-01-30, 21:16:34, 47, 42, , B02884)
List(2018-01-30, 21:11:51, 2018-01-30, 21:40:35, 49, 131, , B02884)
List(2018-01-30, 21:43:39, 2018-01-30, 21:49:59, 98, 121, , B02884)
*/

//retain only the first 6 columns’ values
val retained_data = splitted_data.map(value=>(value(0),value(1),value(2),value(3),value(4),value(5)))

//get the size for the retained data 
retained_data.count
res19: Long = 215331840   

//save the clean data to text file
retained_data.saveAsTextFile("/user/xl1638/spark_final_proj/fhv_clean")



*/











