# README #

This software supports the BIGBIOCL project. Please, refer to the [Wiki page](https://github.com/fcproj/BIGBIOCL/wiki) for a complete description.

### REQUIREMENTS ###
* [JAVA 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* [Apache Spark](http://spark.apache.org/downloads.html) (better if >= 2.1.1)
* [maven](http://maven.apache.org/download.cgi) >= 3.0.3 (to edit the code and build a new jar)

### The Maven project

The folder `maven-source` contains the Maven source project. You can use it to edit the code or to build the "bigbiocl" JAR file:

`cd maven-source/`   
`mvn clean install`  

You will find the created zip file in the directory `executable`, together with the javadoc. 


### License

GNU General Public License version 3 (GPL-3.0). 
