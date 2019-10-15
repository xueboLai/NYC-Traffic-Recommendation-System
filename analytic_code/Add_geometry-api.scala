/*-----intall esri geometry api-------*/

// comiple api jar file in Dumbo 
download geometry-api-java-master.zip from github
upload file into Dumbo
unzip geometry-api-java-master.zip
mvn compile
mvn package


// in Dumbo command line 
cd geometry-api-java-master
cd target
spark-shell --jars esri-geometry-api-2.2.3-SNAPSHOT.jar .jar



