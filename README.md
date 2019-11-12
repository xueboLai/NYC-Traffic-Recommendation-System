<h1 align=center> NYC-Traffic-Recommendation-System</h1>
<p>Keywords: Spark, Python, Data Analysis and Visualization, Tableau, Full-Stack Application</p>
<h2 align = "center">Overview</h2>
<p>

The aim of this project is to develop a NYC multimodal transportation recommendation system based on exploring the travelling pattern of citizens taking various public transportation tools in NYC. At the stage of exploring user group’s preference and travelling patterns, multiple datasets such as taxi data, bike data, subway data and weather data are collected. 
 
 During the analytics, Spark and Scala are used to fully understand these data and to find useful insights important for building the recommendation model. For the model, a multi-objective optimization model considering the time and weather is built. In this project, the multi-objective model is transferred into single objective optimization problem using weighted sum method. Floyd-Warshall algorithm is utilized to find the optimal path. Finally, based on the model we built a recommendation system. On the front-end web application built by Python Flask, users can choose the origin and destination location and the website would provide the “best route” (lowest cost route) consisted of different transportation modes to the user.  
</p>

<p>The Project Design Diagram is shown below, </p>
<details><summary><b>Project Design Diagram</b></strong></summary>
<p align="center">
	<image src="./graphs/design.png">
</p>
</details>
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







<h3 align = "center">Data Source and Data Format Explanation</h2>
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
<details open><summary><b>1, Taxi Data Analysis</b></strong></summary>
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
<details><summary>Description for pick-up and drop-off graphs</strong></summary>
<p>
In first pickup heatmap, the more counts an area have, the darker red that area is. As the graph indicates, Manhattan area and the area along Manhattan island have the most counts of pickup. Also, what worth noticing is the area at bottom right of the graph. It has an unusual darker red compared with the areas around it, which is the JFK airport area. Apparently, many people (possibly a good portion of them is tourists) opt to take taxi to airport. 
</p>
<p>
The second graph is drop off heatmap. The darker blue an area is, the more drop off counts that area has. The densest areas for drop off counts are also in Manhattan and the area along it, as well as JFK and LaGuardia airport. This is a surprising result under the assumption that people would take taxi one-way from one location to another. Here, hypothesis could be raised that a portion of the people tends to take taxi more and use taxi as their regular commuting tools, because the similarity of pickup and drop off heatmap indicates there are many a trip happening between certain areas. This hypothesis will not be the focus point for this paper.
</p>
</details>

<p align="center">
	<image src="./graphs/taxi_usage.png" height="350" width="490">
	<br>
	<span>Figure 6.2.3 Usage of taxi </span>
</p>
<details><summary>Description for Taxi Use Fluctuation</strong></summary>
<p>
The graph above shows the usage of taxi data across 2018. Taxi data is split based on time during a day and different colors are used for different time during a day. Period 1 is the time from 6 am to 9 am which is considered as the morning traffic peak hours. Period 2 is the time from 10am to 4pm which is considered as the regular hours during daytime. Period 3 is the time from 5 pm to 8 pm, which is considered as the evening traffic peak. Finally, period 4 is from 9 pm to 5 am which is considered as nighttime. The count number for different time period in a day is mapped against each month to get the graph above. As can be seen, the total taxi usage reached the peak around February, May and October, and the month of March and April have the least taxi records. It is very intriguing to see sudden surge of taxi records from least records in April to most records in May. Also, as can be seen from the graph, the time period in a day at which people tend to use taxi is fixed across the year. People most likely to use the taxi service at Period 2, which is from 10 am to 4 pm at a day. They are least likely to use taxi for period 1 which is 6 am to 9 am at a day, possibly due to the morning traffic.
</p>


</details>


<p align="center">
	<image src="./graphs/info_under_wea.png" height="350" width="490">
	<br>
	<span>Figure 6.2.4 Information under different weather conditions</span>
</p>
	
<details><summary>Description of Information under different weather conditions</strong></summary>
<p>
	Figure 6.2.4 upper describes the average travel distance, average traveling velocity and average price mapping against each of the weather conditions. The weather conditions in the graph are divided to 3 dimensions, including temperature, rainfall or snowfall and wind speed. Temperature can be mapped to cold weather (temp1), regular weather (temp2) and hot weather(temp3). Rainfall or snowfall condition can be mapped to raining or snowing(PrepT), no precipitation(PrepF). Wind condition can be divided to strong wind(WindT) and no strong wind(WindF). 
</p>
<p>
	Although not very obvious, it can be seen that taxi’s speed is slowest during the time when there is precipitation, and the price reaches the peak during precipitation, which fits into empirical experience since the traffic is usually not good during raining or snowing. From the graph, we can also see that the temperature and whether it is windy independently would barely affect the taxi traveling velocity and prices too much.
</p>
</details>

<p align="center">
	<image src="./graphs/vd_under_wea.png" height="350" width="490">
	<br>
	<span>Figure 6.2.5 Taxi velocity and duration under different weather conditions</span>
</p>
	
<details><summary>Description of Taxi velocity and duration under different weather conditions</strong></summary>
<p>
After conducting aggregation of taxi data and weather conditions shown in Figure 6.2.4, we look into the taxi data aggregated by all different weather conditions simultaneously as demonstrated in Figure 6.2.5. The left chart in Figure 6.2.5 denotes the travel velocity and the right chart denotes the average traveling duration. In the Figure 6.2.5, three characters represents the weather conditions: the first character denoting temperatures (1-low, 2-regular, 3-high); the second character means the whether it is raining/snowing; the third character signaling whether it is raining. From the graph above, we can clearly see that when it is cold, raining and windy, the average travel duration reaches the maximum and average travel velocity reaches the minimum. This weather condition causes the most significant difference in taxi traveling data than other weather conditions. What’s more, from the chart, we can easily see that raining and low temperature would cause the taxi to slow down traveling speed significantly. From this analysis, we can conclude that multiple weather factors combined have more predicting power and determinacy than independent weather condition by comparing Figure 6.2.5 to Figure 6.2.4. Despite the need to combine multiple weather conditions to determine taxi traveling data, the low-temperature and raining/snowing weather conditions would worsen taxi traveling duration and speed in general.
</p>
</details>

<!--end of taxi-->
</details>



<details><summary><b>2, Bike Data Analysis</b></strong></summary>

<p align="center">
	<image src="./graphs/bikepickup.png" height="350" width="490">
	<image src="./graphs/bikedropoff.png" height="350" width="490">
	<br>
	<span>Figure 6.3.2 Citibike pickup and drop off</span>
</p>
<details><summary>Description of Bike Pick-up and Drop-off Graph</strong></summary>
<p>
Figure 6.3.2 shows the pickup and drop off locations for Citibikes in NYC during 2018. The color deepens with the increase of the usage rate of the site. This figure shows that Citibikes are evenly distributed in NYC, especially in Manhattan. Thus, users can basically reach their destination through Citibike in Manhattan. It can be seen that darker spots are mostly concentrated in Midtown Manhattan. Therefore, in these sites with high demand and utilization rate, citibike can increase the number of bicycles appropriately to better meet the needs of customers.
</p>
</details>
	
	


<p align="center">
	<image src="./graphs/usageperiod.png" height="350" width="490">
	<br>
	<span>Figure 6.3.3 Usage counts during four time periods</span>
</p>
<details><summary>Description of Citi Bike Usage over 24 hours</strong></summary>
<p>
Figure 6.3.3 shows the bike usage counts grouped by four time periods. It is clear that the peak usage of Citibikes appears during May to October. Part of the reason lies in that the weather conditions are mild during this time. </p>
<p>
	In the same month, the usage of citibike is also very different in different time periods. The time period with the highest usage rate is time period 2(10am-4pm), followed by time period 3(5pm-8pm), then time period 1(6am-9am), and finally time period 4(9pm-5am).
</p>
</details>

<p align="center">
	<image src="./graphs/usagecount.png" height="350" width="490">
	<br>
	<span>Figure 6.3.4 Usage count in different time period</span>
</p>
<details><summary>Description of Citi Bike Usage count in different time period </strong></summary>
<p>
The above figure shows the usage count grouped by four time period on each day. The lines change periodically. The most obvious is the yellow line chart, and basically every peak is on Saturday. For the comparison of the four time periods, the same as above. Less people ride bicycles at night.</p>
<p>
	For the purpose of implementing a multimodal transportation recommendation system, which takes the impact of weather condition into consideration, the relationship between the speed of Citibike and weather condition can be further explored.
</p>
</details>
	

<p align="center">
	<image src="./graphs/infounderweac.png" height="350" width="490">
	<br>
	<span>Figure 6.3.5 Information under different weather conditions</span>
</p>
<details><summary>Description of Citi Bike in terms of different weather conditions</strong></summary>
<p>
The above figure shows the average distance, average velocity and average price for each single weather. First of all, in the three cases of low temperature, rainfall and strong winds, the average distance is low, probably because people will use other modes of transportation under the bad weather conditions. For the average speed, the average speed is slower when the temperature is higher, and this is a reasonable finding. For the average price, it is positively correlated with riding time. The average price is higher at high temperatures, which means the low riding speed makes price higher. Therefore, it is not a good choice for cycling when the temperature is hot.
</p>
</details>
	
<p align="center">
	<image src="./graphs/singlewea.png" height="350" width="490">
	<br>
	<span>Figure 6.3.5 Information under different weather conditions</span>
</p>
<details><summary>Description of single weather conditions influences on CitiBike</strong></summary>
<p>
The above analysis of the impact of a single weather factor on Citibike, the following analysis of the impact of the combination of the three weather factors on the speed and duration of cycling. From the chart on the left, we can clearly see that the speed is the slowest in the case of 1-T-T (the weather is cold and rainy and windy, the three characters had the same meaning as the figure in taxi analysis.) Next, the slower weather combination is 3-F-T and 3-F-F. This shows that in high temperature weather, whether it is windy or not, the speed will be slow. From the chart on the right, we can see that under the combination of weather that makes cycling very slow, the duration time are longer.
</p>
</details>

<p>
Conclusion:
<br>
From all the analysis of Citibike, we find that both the time period and the weather conditions have significant impact on the usage and speed of Citibike. Thus, we need to know the speed of Citibike under any combination of time and weather. Figure 6.3.7 below shows the result.
</p>	
<p align="center">
	<image src="./graphs/conclusion.png" height="600" width="280">
	<br>
	<span>Figure 6.3.7 Average speed under each weather condition</span>
</p>







</details>





<br>
<br>
<br>
(To be continued)

We are currently in progress of writing the demo and project explanation in README.md. Because this project is very lengthy, it would take us some time to rewrite our findings from paper to Github. For all our findings, please kindly read our paper [NYC_Traffic_Model.pdf](/NYC_Traffic_Model.pdf) which contains all the information about project demonstrations, result visualization and details explanation for now. We really appreciate your patience. 

<details><summary><b>Reference</b></strong></summary>
1.	T. White. Hadoop: The Definitive Guide. O’Reilly Media Inc., Sebastopol, CA, May 2012.
2.	Liu, Y., & Wei, L. (2018, April). The optimal routes and modes selection in multimodal transportation networks based on improved A∗ algorithm. In 2018 5th International Conference on Industrial Engineering and Applications (ICIEA) (pp. 236-240). IEEE.
3.	Lele Liu, Jie Liu. Study on Multimodal Transport Route Under Low Carbon Background AIP Conference Proceedings 1971 050001(2018)
4.	Luo, H., Yang, J., & Nan, X. (2018, October). Path and Transport Mode Selection in Multimodal Transportation with Time Window. In 2018 IEEE 3rd Advanced Information Technology, Electronic and Automation Control Conference (IAEAC) (pp. 162-166). IEEE.
5.	Wang Haiying,Huang Qiang,Li Chuantao, et al. Graph theory algorithm and its matlab implementation[M].Bei Jing: Beihang University press,2010:28-35.
6.	Liu, Y., Chen, J., Wu, W., & Ye, J. (2019). Typical Combined Travel Mode Choice Utility Model in Multimodal Transportation Network. Sustainability, 11(2), 549.
7.	Lucas, K., Phillips, I., Mulley, C., & Ma, L. (2018). Is transport poverty socially or environmentally driven? Comparing the travel behaviours of two low-income populations living in central and peripheral locations in the same city. Transportation Research Part A: Policy and Practice, 116, 622-634.
8.	SI B F, YANG X B, GAO L, et al. Urban multimodal traffic assignment model based on travel demand[J]. China Journal of Highway & Transport, 2010, 23(6): 85–91. 
Urban multimodal traffic assignment model based on travel demand. China journal of Highway 
9.	SI B F, YANG X B, GAO L, et al. Urban multimodal traffic assignment model based on travel demand[J]. China Journal of Highway & Transport, 2010, 23(6): 85–91. 
10.	Liu, H., Li, T., Hu, R., Fu, Y., Gu, J., & Xiong, H. (2019). Joint Representation Learning for Multi-Modal Transportation Recommendation. AAAI, to appear.
</details>



