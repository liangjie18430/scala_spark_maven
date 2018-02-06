package mllib_test
import org.apache.spark.{SparkConf, SparkContext}
object Mllib1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      //.set("spark.driver.port","50491")
      //.setMaster("spark://localhost:7077")
      .setAppName("Mllib1")
    //尝试将jar文件上传到docker相关的位置
    //sparkConf.setJars(Seq("/Users/admin/workspace/scala_workspace/scala_maven_test/out/artifacts/scala_maven_test_jar/scala_maven_test.jar"))
    val sc = new SparkContext(sparkConf)
    //./spark-submit --master spark://sandbox:7077 --deploy-mode client --class mllib_test.Mllib1 /test-1.0.jar --executor-memory 500M --num-executors 2
    //  ./spark-submit --master spark://sandbox:7077 --deploy-mode client --class mllib_test.Mllib1 /test-1.0-with-dependencies.jar
    //需要保证提交的spark的local中有该文件
    /**
      * 经测试，指定为本地文件时，不仅spark的docker中需要有该文件和目录,
      * 在本地也需要有该文件,不然都会抛出找不到文件异常,最终统计的是docker中spark环境中的文件大小
      * 所以测试方案为:
      * 本机建立一个同名文件且大小为0的即可
      * 将真正要执行的放入到docker spark容器中
      */
    val textFile = sc.textFile("file:///Users/admin/workspace/test.txt")
    //val textFile = sc.textFile("/Users/admin/workspace/test.txt")
    val count = textFile.count()
    println("count is %d".format(count))
    val first = textFile.first()
    println("first is %s".format(first))
    textFile.take(4).foreach(println(_))
    val a_count = textFile.filter(line=>line.contains("a")).count()
    println("the str a count is %d".format(a_count))
  }
}
