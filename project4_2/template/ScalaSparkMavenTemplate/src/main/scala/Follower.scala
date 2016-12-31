import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


object Follower {
  /*def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    sc.textFile("hdfs:///TwitterGraph.txt")
      .distinct()
      .map { l => (l.split("\t")(1), 1) }
      .reduceByKey(_+_)
      .map{case(k,v) => s"$k\t$v"}
      .saveAsTextFile("hdfs:///follower-output")
    sc.stop()
  }*/
}