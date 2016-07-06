package alluxio.benchmarks

import java.util.Calendar

import org.apache.spark._
import org.apache.spark.storage.StorageLevel

object PersistBenchmark {
  def saveAsBenchmark(spark: SparkContext, inputFile: String, checkpointFile: String, iterations: Int): Unit = {
    val a = spark.textFile(inputFile)

    // SaveAs** in local disk
    var b = a.map(x => (x, 1))
    b.saveAsObjectFile(checkpointFile)
    b = spark.objectFile(checkpointFile)

    var start = Calendar.getInstance().getTimeInMillis()
    for (i <- 1 until iterations) {
      b.reduce((x, y) => (x._1, x._2 + y._2 + i))
    }
    var end = Calendar.getInstance().getTimeInMillis()
    println(s"SaveAs** $checkpointFile ${end - start} ms.")
  }

  def persistBenchmark(spark: SparkContext, inputFile: String, level: StorageLevel, iterations: Int): Unit = {
    val a = spark.textFile(inputFile)

    // SaveAs** in local disk
    var b = a.map(x => (x, 1))
    b.persist(level)

    var start = Calendar.getInstance().getTimeInMillis()
    for (i <- 1 until iterations) {
      b.reduce((x, y) => (x._1, x._2 + y._2 + i))
    }
    var end = Calendar.getInstance().getTimeInMillis()
    println(s"Persist $level ${end - start} ms.")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PersistBenchmark")
    val spark = new SparkContext(conf)

    val now = Calendar.getInstance().getTime()
    println("Loading ", now)


    saveAsBenchmark(spark, args(0), "/tmp/PersistBenchmark", args(1).toInt)

    saveAsBenchmark(spark, args(0), "alluxio://localhost:19998/PersistBenchmark", args(1).toInt)

    persistBenchmark(spark, args(0), StorageLevel.MEMORY_ONLY_SER, args(1).toInt)

    persistBenchmark(spark, args(0), StorageLevel.MEMORY_ONLY, args(1).toInt)

    persistBenchmark(spark, args(0), StorageLevel.DISK_ONLY, args(1).toInt)

    persistBenchmark(spark, args(0), StorageLevel.OFF_HEAP, args(1).toInt)
    spark.stop()
  }
}
