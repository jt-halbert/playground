name := "Playground"

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.2"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.4.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.4.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-examples" % "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.1"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.0.1"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

