package edu.osu.cse.fathi.spark.ssb

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Runs a set of selections on a CPU with 10 percent selectivity.
 */
object CpuPerformanceTest {

  def main(args: Array[String]) {
    val sparkMasterUrl = args(0)
    val extraJars = Array(args(1))
    val config = new SparkConf().setMaster(sparkMasterUrl).setAppName("CPU Performance Test").setJars(extraJars)
    config.set("spark.worker.memory", "24g").set(
      "spark.executor.memory", "24g").set(
        "spark.driver.memory", "4g")
    val sc = new SparkContext(config)

    val SIZE_OF_INTEGER = 20
    (10 to 24).foreach { size =>
      val TEST_DATA_SIZE = (1 << size) / SIZE_OF_INTEGER
      val selectivity = 10 //percent
    val value = 1

      val testData = (0 until TEST_DATA_SIZE).map(x => if (x % 10 == 0) value else 0)

      var totalTime = 0L
      val cores = 1

      val rdd = sc.parallelize(testData, cores)

      val iterations = 10
      rdd.cache

      rdd.filter(_ == 1).count

      (0 until iterations).foreach { x =>
        val startTime = System.nanoTime
        rdd.filter(_ == 1).collect()
        val endTime = System.nanoTime
        totalTime += endTime - startTime
      }

      println(f"${selectivity}d%% selection (1 CPU core) ${TEST_DATA_SIZE}%,d integer elements "
        + f" took ${totalTime / iterations}%,d nano seconds")
    }
  }
}
