<h1 align=center> NYC-Traffic-Recommendation-System</h1>

<h2 align = "center">Overview</h2>
<p>
The aim of this project is to develop a NYC multimodal transportation recommendation system based on exploring the travelling pattern of citizens taking various public transportation tools in NYC. At the stage of exploring user group’s preference and travelling patterns, multiple datasets such as taxi data, bike data, subway data and weather data are collected. During the analytics, Spark and Scala are used to fully understand these data and to find useful insights important for building the recommendation model. For the model, a multi-objective optimization model considering the time and weather is built. In this project, the multi-objective model is transferred into single objective optimization problem using weighted sum method. Floyd-Warshall algorithm is utilized to find the optimal path. Finally, based on the model we built a recommendation system. On the front-end web application built by Python Flask, users can choose the origin and destination location and the website would provide the “best route” (lowest cost route) consisted of different transportation modes to the user.  
 
</p>
<h2 align = "center">Detailed Data Description</h2>
<p>
As discussed above, we mainly made use of four different kinds of data: Taxi Data, Subway Data, Bike Data, and Weather Data. 
</p>
<h4 align = "center"> Taxi Data Description:</h4>

Yellow Taxi and For-Hired Vehicle data were extracted from New York City TLC trip record data. There are in total 19 fields in the raw data including break-down of the total trip fee, trip distance, start location, ending location, etc. The time range for taxi data that we will be using for this project is from 01/01/2018 to 12/31/2018. After processing the data, eights fields that are relevant to the research are retained. Information about these columns are listed below.



|Columns .  |Type    |Max         |Min         |
|:---------:|:------:|:----------:|:----------:|
|start Date |	String | 2018-12-31 | 2018-01-01 |
|start Time	|String	 |4           |	1          |
|end Date	  |String	 |2018-12-31	|2018-01-01  |
|end Time	  |String  |  	4	      |1           |
|trip Dist	|Double	 |99.95	      |     0      |
|pickup	    |String	 |265	        |   0        |
|dropoff  	|String	 |265	        |0           |
|amount   	|Double	 |999.56	    |0           |






For more detailed description of the project, please kindly read our paper [NYC_Traffic_Model.pdf](/NYC_Traffic_Model.pdf) which contains all of our findings, project demonstrations, result visualization and details explanation.





