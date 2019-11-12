<h1 align=center> NYC-Traffic-Recommendation-System</h1>
<p>Keywords: Spark, Python, Data Analysis and Visualization, Tableau, Full-Stack Application</p>
<h2 align = "center">Overview</h2>
<p>

The aim of this project is to develop a NYC multimodal transportation recommendation system based on exploring the travelling pattern of citizens taking various public transportation tools in NYC. At the stage of exploring user group’s preference and travelling patterns, multiple datasets such as taxi data, bike data, subway data and weather data are collected. 
 
 During the analytics, Spark and Scala are used to fully understand these data and to find useful insights important for building the recommendation model. For the model, a multi-objective optimization model considering the time and weather is built. In this project, the multi-objective model is transferred into single objective optimization problem using weighted sum method. Floyd-Warshall algorithm is utilized to find the optimal path. Finally, based on the model we built a recommendation system. On the front-end web application built by Python Flask, users can choose the origin and destination location and the website would provide the “best route” (lowest cost route) consisted of different transportation modes to the user.  
</p>

<p>The Project Design Diagram is shown below, </p>

<p align="center">
	<image src="./graphs/design.png">
</p>
<details><summary><b>Please click on the dropdown to see the detailed Application Design</b></strong></summary>
<p>
	At the first stage of the project, data including taxi, subway, citibike and weather from different sources is collected and stored in Hadoop HDFS. In Spark, the travelling patterns of taxi, subway and citibikes are analyzed under different weather conditions and time periods. Average velocity and cost of each transportation mode under different weather conditions and time period are generated. With the result of analysis, a multimodal transportation recommendation model minimizing both duration and cost is built and Floyd-Warshall algorithm is utilized to find the optimal path.
</p>
<p>
After preforming the previous steps, we would gather enough information from the algorithm and analytics to compile the data layers. Data layers has the highly aggregated metadata which are much smaller than original data and running result from the Floyd Washer Algorithm. We created this layer to accelerate the program speed by avoiding running the back-end spark data query code from the beginning and Floyd-Warshall algorithm implementation every time when a user query for the best route information, since the cost for running either spark data query code and Floyd Warshall Algorithm code is very high. Therefore, data layer can be considered as caching the output from data analysis and the algorithm in a sense. However, to keep the data layers up to date, we plan to automate the process of data layers updating itself from the newest data in Spark in the future.
</p>
<p>
With the data layer constructed as described above, we have the options to build numerous applications on top on it. Because of the limitation of time, we would build a web application as demonstration for the project.
</p>

</details>



<h2 align = "center">Data Analysis</h2>








<details><summary><b>Please click on the dropdown to see the raw data description</b></strong></summary>
 <h2 align = "center">Detailed Data Description</h2>
<p>
As discussed above, we mainly made use of four different kinds of data: Taxi Data, Subway Data, Bike Data, and Weather Data. 
</p>
<h3 align = "center"> Taxi Data Description</h3>

Yellow Taxi and For-Hired Vehicle data (10GB) were extracted from New York City TLC trip record data. There are in total 19 fields in the raw data including break-down of the total trip fee, trip distance, start location, ending location, etc. The time range for taxi data that we will be using for this project is from 01/01/2018 to 12/31/2018. After processing the data, eights fields that are relevant to the research are retained. Information about these columns are listed below.



|Columns    |Type    |Max         |Min         |
|:---------:|:------:|:----------:|:----------:|
|start Date |	String | 2018-12-31 | 2018-01-01 |
|start Time	|String	 |4           |	1          |
|end Date	  |String	 |2018-12-31	|2018-01-01  |
|end Time	  |String  |  	4	      |1           |
|trip Dist	|Double	 |99.95	      |     0      |
|pickup	    |String	 |265	        |   0        |
|dropoff  	|String	 |265	        |0           |
|amount   	|Double	 |999.56	    |0           |

<p>
Pick up and drop off locations are the number mapped by the TLC taxi zone in New York City. The amount is the aggregated taxi fee for a trip. 
</p>

<h3 align = "center"> Subway Data Description</h3>
Subway data (100KB) was downloaded from NYC Open Data. This dataset includes the location of subway stations in NYC. The schema of the data is as follows.

|Columns    |Type    |	Max(value length)|Min(value length)|
|:---------:|:------:|:----------:|:----------:|
|Name |	String	| 34         |	 5         |
|Latitude	  |Double  |	 14	       |12          |
|Longitude	 |Double	 |11 	        |15          |
|Line       |	String	|15	         |1           |

<h3 align = "center"> Citibike Dataset Description</h3>
Citibike data (5GB) is accessed from Citibike official website. Citibike record data in the New York City is collected from 1/1/2018 to 31/12/2018. The schema of Citibike dataset is shown as follows.

|Columns|Type|Max(value/value length)|	Min(value/value length)
|:---------:|:------:|:----------:|:----------:|
|Duration|	Int|	19510049|	61|
|Start_time|	String|	19	|19|
|Stop_time	|String	|19	|19|
|Latitude|	Double|	45.506|	40.647|
|Longitude|	Double|	-73.569|	-74.025|


<h3 align = "center"> Weather Description</h3>
Weather data (4.8MB) was downloaded from National Centers For Environmental Information. We collected the New York City weather data from 1/1/2018 to 31/12/2018. The size of this dataset is 4.8 MB. It contains lots of useful columns, such as windspeed, temperature and precipitation. The schema of this dataset is as follows.

|Columns|Type|Max|Min|
|:---------:|:------:|:----------:|:----------:|
|Date|String|21|21|
|Temperature|Double|	95|	5|
|Precipitation	|Double	|1.69	|0.0|
|Windspeed	|Double	|21	|0|

</details>

<h3 align = "center">Data Analysis using Spark and Tableau</h3>
<details open><summary><b>Taxi Data Analysis</b></strong></summary>
<p>The first analysis for Taxi Data was the pickup and drop off location density analytics.</p>
<p align="center">
	<image src="./graphs/pickup.png">
	<br>
	<span>Figure 6.2.1 Taxi pickup by taxi zones</span>
</p>
<p align="center">
	<image src="./graphs/dropoff.png">
	<br>
	<span>Figure 6.2.2 Taxi drop off by taxi zones</span>
</p>
<details><summary><b>Description for pick-up and drop-off graphs</b></strong></summary>
<p>
In first pickup heatmap, the more counts an area have, the darker red that area is. As the graph indicates, Manhattan area and the area along Manhattan island have the most counts of pickup. Also, what worth noticing is the area at bottom right of the graph. It has an unusual darker red compared with the areas around it, which is the JFK airport area. Apparently, many people (possibly a good portion of them is tourists) opt to take taxi to airport. 
</p>
<p>
The second graph is drop off heatmap. The darker blue an area is, the more drop off counts that area has. The densest areas for drop off counts are also in Manhattan and the area along it, as well as JFK and LaGuardia airport. This is a surprising result under the assumption that people would take taxi one-way from one location to another. Here, hypothesis could be raised that a portion of the people tends to take taxi more and use taxi as their regular commuting tools, because the similarity of pickup and drop off heatmap indicates there are many a trip happening between certain areas. This hypothesis will not be the focus point for this paper.
</p>
</details>

<p align="center">
	<image src="./graphs/taxi_usage.png">
	<br>
	<span>Figure 6.2.3 Usage of taxi </span>
</p>
<details><summary><b>Description for Taxi Use Fluctuation</b></strong></summary>
<p>
The graph above shows the usage of taxi data across 2018. Taxi data is split based on time during a day and different colors are used for different time during a day. Period 1 is the time from 6 am to 9 am which is considered as the morning traffic peak hours. Period 2 is the time from 10am to 4pm which is considered as the regular hours during daytime. Period 3 is the time from 5 pm to 8 pm, which is considered as the evening traffic peak. Finally, period 4 is from 9 pm to 5 am which is considered as nighttime. The count number for different time period in a day is mapped against each month to get the graph above. As can be seen, the total taxi usage reached the peak around February, May and October, and the month of March and April have the least taxi records. It is very intriguing to see sudden surge of taxi records from least records in April to most records in May. Also, as can be seen from the graph, the time period in a day at which people tend to use taxi is fixed across the year. People most likely to use the taxi service at Period 2, which is from 10 am to 4 pm at a day. They are least likely to use taxi for period 1 which is 6 am to 9 am at a day, possibly due to the morning traffic.
</p>
<p align="center">
	<image src="./graphs/info_under_wea.png">
	<br>
	<span>Figure 6.2.4 Information under different weather conditions</span>
</p>
	
<details><summary><b>Description of Information under different weather conditions</b></strong></summary>
<p>
	Figure 6.2.4 upper describes the average travel distance, average traveling velocity and average price mapping against each of the weather conditions. The weather conditions in the graph are divided to 3 dimensions, including temperature, rainfall or snowfall and wind speed. Temperature can be mapped to cold weather (temp1), regular weather (temp2) and hot weather(temp3). Rainfall or snowfall condition can be mapped to raining or snowing(PrepT), no precipitation(PrepF). Wind condition can be divided to strong wind(WindT) and no strong wind(WindF). 
</p>
<p>
	Although not very obvious, it can be seen that taxi’s speed is slowest during the time when there is precipitation, and the price reaches the peak during precipitation, which fits into empirical experience since the traffic is usually not good during raining or snowing. From the graph, we can also see that the temperature and whether it is windy independently would barely affect the taxi traveling velocity and prices too much.
</p>
<details><summary><b>Description for Taxi Use Fluctuation</b></strong></summary>

</details>




</details>


<br>
<br>
<br>
(To be continued)

We are currently in progress of writing the demo and project explanation in README.md. Because this project is very lengthy, it would take us some time to rewrite our findings from paper to Github. For all our findings, please kindly read our paper [NYC_Traffic_Model.pdf](/NYC_Traffic_Model.pdf) which contains all the information about project demonstrations, result visualization and details explanation for now. We really appreciate your patience. 





