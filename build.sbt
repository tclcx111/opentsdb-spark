name := "opentsdb-spark"

version := "0.1"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.0.1"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "0.98.3-hadoop2"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "0.98.3-hadoop2"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.2.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.2.0"
