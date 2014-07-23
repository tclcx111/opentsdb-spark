package uis.cipsi.rdd.opentsdb

object TestQuery extends App {       

  val sparkMaster = "spark://152.94.1.168:7077" //"local"
  val zookeeperQuorum = "152.94.1.168" //"localhost"
  val zookeeperClientPort = "2181"

  val sparkTSDB = new SparkTSDBQuery(sparkMaster, zookeeperQuorum, zookeeperClientPort)

  val startdate = "*" //"1507201408:00" //"*"
  val enddate = "*" //"1807201408:00" //"*"

  val metricName = "metric.safer.actual.temperature"
  val tagKeyValue = "tag.loc->stavanger"

  val RDD = sparkTSDB.generateRDD(metricName, tagKeyValue, startdate, enddate)
  //Output: ((timestamp, value), ...)
  //val RDDCollect = RDD.collect()
  println(RDD.count)
  //RDDCollect.foreach(println)
}