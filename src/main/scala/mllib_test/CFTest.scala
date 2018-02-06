package mllib_test

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

//website ---http://spark.apache.org/docs/latest/ml-collaborative-filtering.html
object CFTest {

  case class Rating(userId:Int,movieId:Int,rating:Float,timesstamp:Long)
  //定义全局的spark对象
  //als最小交替二乘法，基于模型的协同过滤
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CFTest")

    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()


    import spark.implicits._
    def parseRating(str:String):Rating={

      val fields = str.split("::")
      assert(fields.size==4)
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
    }

    val ratings = spark.read.textFile("file:///Users/admin/workspace/test2.txt").map(parseRating).toDF()

    //val ratings = spark.read.textFile("sample_movielens_ratings.txt").map(parseRating).toDF()

    val Array(training,test)=ratings.randomSplit(Array(0.8,0.2))
    training.head(4)

    //使用als开始构建算法
    val als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId").setRatingCol("rating")

    val model = als.fit(training)
    //
    //model.setColdStartStrategy("drop")

    val predictions  = model.transform(test)

    val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)

    println(s"Root-mean-square error = $rmse")

    // Generate top 10 movie recommendations for each user
    //新版本的api才有
    //val userRecs = model.recommendForAllUsers(10)
    // Generate top 10 user recommendations for each movie
    //val movieRecs = model.recommendForAllItems(10)

  }






}
