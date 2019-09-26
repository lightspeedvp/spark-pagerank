package com.soundcloud.spark.pagerank

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.apache.spark.storage.StorageLevel

class GraphBuilderAppTest
  extends FunSuite
  with BeforeAndAfter
  with Matchers
  with GraphTesting
  with SparkTesting {

  val path = "target/test/GraphBuilderAppTest"
  val graphPath = "/Users/eric/Desktop/GraphBuilderAppTest"

  before {
    FileUtils.deleteDirectory(new File(path))
  }

  // TODO(jd): design a better integration test as this just runs the app without assertions
  test("integration test") {
    val options = new GraphBuilderApp.Options()
    options.output = path
    options.numPartitions = 1

    val input = spark.sparkContext.parallelize(Seq(
      (1, 5, 1.0),
      (2, 1, 1.0),
      (3, 1, 1.0),
      (4, 2, 1.0),
      (4, 3, 1.0),
      (5, 3, 1.0),
      (5, 4, 1.0)
    ).map(_.productIterator.toSeq.mkString("\t")))

    GraphBuilderApp.runFromInputs(options, spark, input)
  }

  test("test build graph from file"){
      val options = new GraphBuilderApp.Options()
      options.output = graphPath
      // "Input directory containing edges in TSV format (source, destination, weight)"
      options.input = "/Users/eric/Desktop/edgeGraph.tsv"
      options.numPartitions = 1

      GraphBuilderApp.runFromInputs(
        options, spark, spark.sparkContext.textFile(options.input, minPartitions = options.numPartitions)
      )
  }

  test("run pageRank from graph file"){
    val options = new PageRankApp.Options()
    options.input = graphPath
    options.output = "/Users/eric/Desktop/pageRankAppRankingsTest/"
    val graph = PageRankGraph.load(
      spark,
      options.input,
      edgesStorageLevel = StorageLevel.MEMORY_AND_DISK_2,
      verticesStorageLevel = StorageLevel.MEMORY_AND_DISK_2
    )

    PageRankApp.runFromInputs(
      spark,
      options,
      graph,
      priorsOpt = None
    )
//    spark.sparkContext.setCheckpointDir(spark.conf.get("spark.local.dir"))
//    val rankings = PageRank.run(graph = graph)
//    rankings.map{case(a, b) =>
//      var line = a.toString + "," + b.toString
//      line
//    }.saveAsTextFile(options.output + "rankings.csv");
  }
}
