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

df.printSchema()
//schema type:
/*
root
 |-- startDate: string (nullable = true)
 |-- startTime: string (nullable = true)
 |-- endDate: string (nullable = true)
 |-- endTime: string (nullable = true)
 |-- startLoc: string (nullable = true)
 |-- endLoc: string (nullable = true)
 |-- nouse1: string (nullable = true)
 |-- nouse2: string (nullable = true)
*/

df.show()
//sample data
/*
+----------+---------+----------+--------+--------+------+
| startDate|startTime|   endDate| endTime|startLoc|endLoc|
+----------+---------+----------+--------+--------+------+
|2018-01-30| 21:06:50|2018-01-30|21:15:34|      56|   129|
|2018-01-30| 21:20:36|2018-01-30|21:35:29|     129|   112|
|2018-01-30| 21:04:45|2018-01-30|21:16:34|      47|    42|
|2018-01-30| 21:11:51|2018-01-30|21:40:35|      49|   131|
|2018-01-30| 21:43:39|2018-01-30|21:49:59|      98|   121|
|2018-01-30| 21:36:53|2018-01-30|21:44:55|     235|   235|
|2018-01-30| 21:48:30|2018-01-30|21:51:30|     169|   235|
|2018-01-30| 21:55:18|2018-01-30|22:15:28|     235|   208|
|2018-01-30| 21:21:25|2018-01-30|21:46:49|     231|   265|
|2018-01-30| 21:24:37|2018-01-30|21:35:23|     123|    29|
|2018-01-30| 21:39:49|2018-01-30|21:49:52|      29|    21|
+----------+---------+----------+--------+--------+------+
*/

//find the max and min for each of the column:
val selectedColumnName = df.columns(0)
df.agg(min(selectedColumnName)).show
/*
+--------------+                                                                 
|min(startDate)|
+--------------+
|2018-01-01    |
+--------------+
*/

df.agg(max(selectedColumnName)).show
/*
+--------------+                                                                 
|max(startDate)|
+--------------+
|2018-12-31    |
+--------------+
*/

val selectedColumnName = df.columns(2)
df.agg(min(selectedColumnName)).show
/*
+-------------+                                                                 
|min(endDate) |
+-------------+
|2018-01-01   |
+-------------+
*/

df.agg(max(selectedColumnName)).show
/*
+-------------+                                                                 
|max(endDate) |
+-------------+
|2018-12-31   |
+-------------+
*/

val selectedColumnName = df.columns(1)
df.agg(min(selectedColumnName)).show
/*
+--------------+                                                                 
|min(startTime)|
+--------------+
|00:00:00      |
+--------------+
*/

df.agg(max(selectedColumnName)).show
/*
+--------------+                                                                 
|max(startTime)|
+--------------+
|23:59:59      |
+--------------+
*/

val selectedColumnName = df.columns(3)
df.agg(min(selectedColumnName)).show
/*
+--------------+                                                                 
|min(endTime)  |
+--------------+
|00:00:00      |
+--------------+
*/

df.agg(max(selectedColumnName)).show
/*
+--------------+                                                                 
|max(endTime)  |
+--------------+
|23:59:59      |
+--------------+
*/
val selectedColumnName = df.columns(4)
df.agg(min(selectedColumnName)).show
/*
+--------------+                                                                 
|min(startLoc) |
+--------------+
|1             |
+--------------+
*/

df.agg(max(selectedColumnName)).show
/*
+-------------+                                                                 
|max(startLoc)|
+-------------+
|265          |
+-------------+
*/

val selectedColumnName = df.columns(5)
df.agg(min(selectedColumnName)).show
/*
+-------------+                                                                 
|min(endLoc)  |
+-------------+
|1            |
+-------------+
*/

df.agg(max(selectedColumnName)).show
/*
+-------------+                                                                 
|max(endLoc)  |
+-------------+
|265          |
+-------------+
*/











