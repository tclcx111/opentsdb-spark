package uis.cipsi.rdd.opentsdb

import org.apache.spark.SparkConf
import scala.tools.nsc.io.Jar
import scala.tools.nsc.io.File
import scala.tools.nsc.io.Directory
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.PairRDDFunctions
 

object TestQuery {

  def main(args: Array[String]) {
  val sparkMaster = "spark://152.94.1.168:7077" //"local"
  val zookeeperQuorum = "152.94.1.168" //"localhost"
  val zookeeperClientPort = "2181"

  val sparkTSDB = new SparkTSDBQuery(sparkMaster, zookeeperQuorum, zookeeperClientPort)
  
  val startdate = "1507201414:00"
  val enddate = "1807201408:00" 

  val metricName = "metric.safer.actual.temperature"
  val tagKeyValue = "tag.loc->stavanger"//, tag.date->1407201414"
  	
  //creating spark context
    val sparkConf = new SparkConf()
    sparkConf.setAppName("opentsdb-spark")
    sparkConf.setMaster(sparkMaster)
    if (!SparkContext.jarOfClass(this.getClass).isEmpty) {
      //If we run from eclipse, this statement doesnt work!! Therefore the else part
      sparkConf.setJars(SparkContext.jarOfClass(this.getClass).toSeq)
    } else {
      val jar = Jar
      val classPath = this.getClass.getResource("/" + this.getClass.getName.replace('.', '/') + ".class").toString()
      val sourceDir = classPath.substring("file:".length, classPath.indexOf("uis/cipsi/rdd/opentsdb")).toString()
      jar.create(File("/tmp/opentsdb-spark-0.01.jar"), Directory(sourceDir), "opentsdb-spark")
      sparkConf.setJars(Seq("/tmp/opentsdb-spark-0.01.jar"))
    }

  val sc = new SparkContext(sparkConf)  
  
  val ForecastTempRDD = sparkTSDB.generateRDD("metric.safer.forecast.temperature", 
      "tag.loc->stavanger, tag.date->1407201414", "*", "*", sc)
      
  val ForecastWindDRDD = sparkTSDB.generateRDD("metric.safer.forecast.windDirection", 
      "tag.loc->stavanger, tag.date->1407201414", startdate, enddate, sc)
      
  val ForecastPressureRDD = sparkTSDB.generateRDD("metric.safer.forecast.pressure", 
      "tag.loc->stavanger, tag.date->1407201414", startdate, enddate, sc)
  val rdd = new PairRDDFunctions(new PairRDDFunctions(ForecastTempRDD).join(ForecastWindDRDD)).join(ForecastPressureRDD)
  rdd.map(kv => (kv._1, (kv._2._1._1, kv._2._1._2, kv._2._2))).collect.foreach(println)
  }
  
}