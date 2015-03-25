package edu.osu.cse.fathi.spark.ssb

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Converts row-wise files generated with dbGen to Parquet format.
 */
object SsbToParquet {
  def main(args: Array[String]) {
    val sparkMasterUrl = args(0)
    val extraJars = Array(args(1))
    val config = new SparkConf().setMaster(sparkMasterUrl).setAppName("SSB Parquet Format").setJars(extraJars)
    config.set("spark.worker.memory", "24g").set(
      "spark.executor.memory", "24g").set(
        "spark.driver.memory", "4g")
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)
    import sqlContext.createDataFrame

    val homeDir = System.getProperty("user.home")

    val dbGenDir = new File(homeDir, "workspace/gpudb/test/dbgen")
    val parquetDir = new File(homeDir, "workspace/gpudb/database/SSB/scale-10")

    val tableNames = Seq("customer", "ddate", "lineorder", "part", "supplier")

    tableNames.foreach { tableName =>
      val tableRawDataFile = new File(dbGenDir, f"${tableName}.tbl")
      assert(tableRawDataFile.exists(), f"Raw file for table ${tableName}%s does not exist at ${tableRawDataFile}%s")
      val rawRDD = sc.textFile(tableRawDataFile.getPath).map(_.split(raw"\|"))
      val result = tableName match {
        case "customer" =>
          val objectedRdd: RDD[Customer] = rawRDD.map(SsbConversionFunctions.toCustomer)
          val parquetOutputFile = new File(parquetDir, f"00-${tableName}%s.parquet")
          sqlContext.createDataFrame(objectedRdd).saveAsParquetFile(parquetOutputFile.getPath)
        case "ddate" =>
          val objectedRdd: RDD[DDate] = rawRDD.map(SsbConversionFunctions.toDDate)
          val parquetOutputFile = new File(parquetDir, f"00-${tableName}%s.parquet")
          sqlContext.createDataFrame(objectedRdd).saveAsParquetFile(parquetOutputFile.getPath)
        case "lineorder" =>
          val objectedRdd: RDD[LineOrder] = rawRDD.map(SsbConversionFunctions.toLineOrder)
          val parquetOutputFile = new File(parquetDir, f"00-${tableName}%s.parquet")
          sqlContext.createDataFrame(objectedRdd).saveAsParquetFile(parquetOutputFile.getPath)
        case "part" =>
          val objectedRdd: RDD[Part] = rawRDD.map(SsbConversionFunctions.toPart)
          val parquetOutputFile = new File(parquetDir, f"00-${tableName}%s.parquet")
          sqlContext.createDataFrame(objectedRdd).saveAsParquetFile(parquetOutputFile.getPath)
        case "supplier" =>
          val objectedRdd: RDD[Supplier] = rawRDD.map(SsbConversionFunctions.toSuplier)
          val parquetOutputFile = new File(parquetDir, f"00-${tableName}%s.parquet")
          sqlContext.createDataFrame(objectedRdd).saveAsParquetFile(parquetOutputFile.getPath)
        case _ => throw new RuntimeException(f"Invalid table name ${tableName}")
      }
    }
    sc.stop()
  }
}
