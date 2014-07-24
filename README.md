opentsdb-spark
==============

Module for accessing OpenTSDB data through Spark.

#Installation.
##On your client (SparkDriver)
  >Execute the following in your terminal
  1. wget https://github.com/achak1987/opentsdb-spark/archive/master.zip
  2. unzip master.zip
  3. cd opentsdb-spark-master
  4. sbt eclipse (if you dont have sbt, it can be installed from www.scala-sbt.org)
  5. The project can be now imported into eclipse

##On your cluster.
  >For each node in the cluster add the following into your com file
  1. nano 
  2. copy the following to the end of the file. Before echo "$CLASSPATH"\\
      export HBASE_HOME=/path/to/your/hbase/dir (if you haven't already defined the $HBASE_HOME env var)

      for f in $HBASE_HOME/lib/*.jar
        do
    	CLASSPATH="$CLASSPATH:$f"
      done

#System Info
  Apache Hadoop 2.2.0, Apache Hbase 0.98.3-hadoop2, Apache Spark 1.0.1, Scala 2.10.4


