package base

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/Users/liangjie09/workspace/baidu/scala_spark_maven/test.txt" // Should be some file on your system
    var conf = new SparkConf().setAppName("SimpleApp")
    //.setMaster("spark://localhost:7077")
    //.setJars(Array[String]("/Users/admin/workspace/scala_workspace/scala_maven_test/out/artifacts/scala_maven_test_jar/scala_maven_test.jar"))
//    conf = conf.setJars(Array[String]("/Users/liangjie09/workspace/baidu/scala_spark_maven/target/test-1.0-with-dependencies.jar"))

    val sc = new SparkContext(conf)
    //创建spark session
    val spark = SparkSession.builder().appName("SimpleApp").config(conf).getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
//    val logData2 = spark.read.
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")

    spark.stop()
  }
}