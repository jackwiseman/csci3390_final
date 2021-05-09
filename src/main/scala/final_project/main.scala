package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)
  
  def filterGraph(g_in: Graph[(Int, Int), (Long, Long)]): Graph[(Int, Int), (Long, Long)] = {
    //filters the graph by removing all vertices and edges from vertices with that are inactive (where randNum._2 != 1)
    val g_out = g_in.subgraph(vpred = (active, randNum) => randNum._1 == -1)
    return g_out
  }

   def IsraeliItai(g: Graph[(Int, Int), (Long, Long)]) = {
     var remaining_edges= 2 
     while (remaining_edges >= 1){
      val msg = g.aggregateMessages[(Int, Int)] (
     //creates vertices for the new graph. 
        triplet => {
           triplet.sendToDst((triplet.srcId.toInt, 1))
            triplet.sendToSrc((triplet.dstId.toInt, 1))
        }, (a, b) => 
            if (r.nextFloat() > a._2.toFloat / (a._2.toFloat + b._2.toFloat)) {(a._1, (a._2 + b._2))} else {(b._1, b._2 + a._2)}
      )
    
      val joinedGraph: Graph[(Int, Int), (Long, Long)] = g.joinVertices(msg) { (_, oldAttr, newAttr) => ((newAttr._1, r.nextInt(2)))}

      //Joins with the original graph, completing the message sending phase.

       val returnMessage = joinedGraph.aggregateMessages[(Int, Int)] (
      //Send back message to source, so that each node has sent and arbitrarily collected if it can
        triplet => {
          if(triplet.srcId.toInt == triplet.dstAttr._1) {triplet.sendToSrc(triplet.dstId.toInt, 1)} else {triplet.sendToDst(-1, 1)}
          if(triplet.dstId.toInt == triplet.srcAttr._1) {triplet.sendToDst(triplet.srcId.toInt, 1)} else {triplet.sendToSrc(-1, 1)}
        }, (a, b) =>
        if (a._1 == -1) {b}
        else if (b._1 == -1) {a}
        else if (r.nextFloat() > a._2.toFloat / (a._2.toFloat + b._2.toFloat)) {(a._1, (a._2 + b._2))} else {(b._1, b._2 + a._2)}
      )

    val joinedGraph2: Graph[(Int, Int), (Long, Long)] = g.joinVertices(returnMessage) { (_, oldAttr, newAttr) => ((newAttr._1, r.nextInt(2)))}

    val anotherMessage = joinedGraph2.aggregateMessages[Int] (
      triplet => {
        if (triplet.dstAttr._1 == triplet.srcId.toInt && triplet.dstAttr._2 == 1 && triplet.srcAttr._2 == 0) {
          triplet.sendToSrc((1))
          triplet.sendToDst((1))
        }
        else if (triplet.srcAttr._1 == triplet.dstId.toInt && triplet.srcAttr._2 == 1 && triplet.srcAttr._2 == 0) {
          triplet.sendToSrc((1))
          triplet.sendToDst((1))
        }
        else {
          triplet.sendToSrc((-1))
          triplet.sendToDst((-1))
        }}, (a, b) => if(a > b) a else b
    )  
   }
 }
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("final_project")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
    /* You can either use sc or spark */

    val r = scala.util.Random

    if(args.length != 2) {
      println("Usage: [path_to_graph] [output_path]")
      sys.exit(1)
      
    }

    val startTimeMillis = System.currentTimeMillis()
    val edges = sc.textFile(args(0)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
    val g = Graph.fromEdges[Int, (Int, Int)](edges, (1, r.nextInt(2), edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
    val graph = g.mapVertices((active, randNum) => (1, r.nextInt(2)))
    // functions are called here, passing input graph g and returning g_out


    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println("==================================")
    println("Runtime: " + durationSeconds + "s.")
    println("==================================")
    
    // uncomment the following lines out when g_out is assigned
    // val g2df = spark.createDataFrame(g_out.vertices)
    // g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(1))
    sys.exit(1)
  }
}
