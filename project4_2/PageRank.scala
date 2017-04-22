import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object PageRank {
   
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    
    val txt = sc.textFile("hdfs:///TwitterGraph.txt").distinct()
    
    
    val followerId = txt.map{ s =>
      val parts = s.split("\\s+")
      parts(0)
    }
    
    val followeeId = txt.map{ s =>
      val parts = s.split("\\s+")
      parts(1)
    }
    
    val allIds = followerId.union(followeeId).distinct()
    
    val danglingIds = allIds.subtract(followerId)
    val danglingLink = danglingIds.map { id => (id, "") }
    
    val userCount = 2315848
    
    val links = txt.map { s => {
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }}.union(danglingLink).groupByKey().cache()
    
    var ranks = links.mapValues { v => 1.0 }
    

    for (i <- 1 to 10) {
      
      var danglingVar = sc.accumulator(0.0)
      
      val contribs = links.join(ranks).values.flatMap{ case (followees, rank) =>
        val size = followees.size
        
        if (size == 1 && followees.count { s => s == "" } == 1) {
          danglingVar += rank
          List()
        }
        else {
          followees.map(followee => {
            (followee, rank / size)
          })
        }
      }
      contribs.count() // ??
      val danglV = danglingVar.value
      ranks = contribs.reduceByKey(_ + _).mapValues(contrib =>  0.15 + 0.85 * (contrib + danglV / userCount))
    }
    ranks.reduceByKey(_+_).map{case(k, v) => s"$k\t$v"}.saveAsTextFile("hdfs:///pagerank-output")
    
    sc.stop()
  }
}