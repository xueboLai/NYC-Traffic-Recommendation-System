//read cleaned subway station data，use the first field as the key
//the format of subway station is trainname, stationId, stationId,stationId,stationId......
val subway = sc.textFile("spark-project/subway/*").keyBy(line=>line.split(",")(0))

//remove the trainname from stationId
val station = subway.map(tp=>(tp._2.substring(2,tp._2.length))).map(_.split(","))

import scala.collection.mutable.ArrayBuffer

//for each station, get the linked stations(one station before it, one station after it)
def recordsubway(a:Array[String])={
 var slist= ArrayBuffer[Array[String]]()
 var i=1
 for (i<-1 to a.length-2){
   val stations= Array(a(i),a(i-1),a(i+1))
     slist += stations
 }
 slist += Array(a(0),a(1),"#")
 slist += Array(a(a.length-1),a(a.length-2),"#")
 slist
}

//get the linked stations for all stations
val stationslist = station.flatMap(recordsubway(_))

/*
stationslist.take(3)
res47: Array[Any] = Array(Array(59, 265, 263), Array(263, 59, 20), Array(20, 263, 171))
*/

//make the station as the key, the linked stations as values
val mapstations=stationslist.map(line=>(line(0),(line(1),line(2))))

/*
mapstations.take(3).foreach(println)
(59,(265,263))
(263,(59,20))
(20,(263,171))
*/

//remove parentheses
def remove(str:String)={
  str.substring(1,str.length-1)
}

//for each subway station, group by key, which menas we can get all the connected stations for this station
val groupstations = mapstations.map(tp=>(tp._1,remove(tp._2.toString))).groupByKey().map(tp=>(tp._1,tp._2.toList))

/*
groupstations.take(3).foreach(println)
(202,List(91,30))                                                               
(394,List(376,375, 376,375))
(231,List(240,230))
*/

//Remove the duplicate in the linked station List
def removeduplicate(sts:List[String])={
  var result= ArrayBuffer[Int]()
  var temp =""
  for(temp<-sts){
   var twovalue=temp.split(",")
   if (twovalue(0)!="#" && (!result.contains(twovalue(0).toInt))){ 
      result+=twovalue(0).toInt
   }
   if (twovalue(1)!="#" && (!result.contains(twovalue(1).toInt))){ 
      result+=twovalue(1).toInt
   }
  }
  result.toList
}

//Remove the duplicate in the linked station List
val thestations = groupstations.map(tp=>(tp._1,removeduplicate(tp._2)))

/*
thestations.take(3).foreach(println)
(202,List(91, 30))
(394,List(376, 375))
(231,List(240, 230))
*/

//get the station Ids between parentheses
def format(str:String)={
var a =str.indexOf("(")
var b= str.indexOf(")")
str.substring(a+1,b)
}

//change the format of result
val changeformat=thestations.map(tp=>(tp._1,format(tp._2.toString)))

/*
changeformat.take(3).foreach(println)
(202,91, 30)
(394,376, 375)
(231,240, 230)
*/

changeformat.saveAsTextFile("spark-project/subwaylinks")



