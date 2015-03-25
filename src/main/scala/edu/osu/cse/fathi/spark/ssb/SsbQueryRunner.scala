package edu.osu.cse.fathi.spark.ssb

import java.io.File

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Runs SSB all queries on parquet formats. Before executing queries, loads all data into memory.
 */
object SsbQueryRunner {
  def main(args: Array[String]) {
    val config = new SparkConf().setAppName("SSB Queries")
    //      .set(
    //      "spark.worker.memory", "20g").set(
    //        "spark.executor.memory", "20g").set(
    //        "spark.driver.memory", "20g")
    //      .set(
    //        "spark.driver.maxResultSize", "2G"
    //      )
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)

    val homeDir = System.getProperty("user.home")

    val parquetDir = new File(homeDir, "workspace/gpudb/database/SSB/scale-10")

    val tableNames = Seq("customer", "ddate", "lineorder", "part", "supplier")

    tableNames.foreach { tableName =>
      val parquetFile = new File(parquetDir.getPath, s"00-${tableName}.parquet")
      assert(parquetFile.exists)
      assert(parquetFile.isDirectory)

      val ssbTable = sqlContext.parquetFile(parquetFile.getPath)
      ssbTable.registerTempTable(tableName)
      // make sure tables are cached
      sqlContext.cacheTable(tableName)
    }
    val ssbQueries = Seq(
      ("SSBQ1.1", """
                    |select sum(lo_extendedprice*lo_discount) as revenue
                    |from lineorder,ddate
                    |where lo_orderdate = d_datekey
                    |and d_year = 1993 and lo_discount>=1
                    |and lo_discount<=3
                    |and lo_quantity<25
                    | """.stripMargin),

      ("SSBQ1.2", """
                    |select sum(lo_extendedprice*lo_discount) as revenue
                    |from lineorder,ddate
                    |where lo_orderdate = d_datekey
                    |and d_yearmonthnum = 199401
                    |and lo_discount>=4
                    |and lo_discount<=6
                    |and lo_quantity>=26
                    |and lo_quantity<=35
                    | """.stripMargin),

      ("SSBQ1.3", """
                    |select sum(lo_extendedprice*lo_discount) as revenue
                    |from lineorder,ddate
                    |where lo_orderdate = d_datekey
                    |and d_weeknuminyear = 6
                    |and d_year = 1994
                    |and lo_discount>=5
                    |and lo_discount<=7
                    |and lo_quantity>=26
                    |and lo_quantity<=35
                    | """.stripMargin),

      ("SSBQ2.1", """
                    |select sum(lo_revenue),d_year,p_brand1
                    |from lineorder,part,supplier,ddate
                    |where lo_orderdate = d_datekey
                    |and lo_partkey = p_partkey
                    |and lo_suppkey = s_suppkey
                    |and p_category = 'MFGR#12'
                    |and s_region = 'AMERICA'
                    |group by d_year,p_brand1
                    |order by d_year,p_brand1
                    | """.stripMargin),

      ("SSBQ2.2", """
                    |select sum(lo_revenue),d_year,p_brand1
                    |from lineorder, part, supplier,ddate
                    |where lo_orderdate = d_datekey
                    |and lo_partkey = p_partkey
                    |and lo_suppkey = s_suppkey
                    |and p_brand1 >= 'MFGR#2221'
                    |and p_brand1 <= 'MFGR#2228'
                    |and s_region = 'ASIA'
                    |group by d_year,p_brand1
                    |order by d_year,p_brand1
                    | """.stripMargin),

      ("SSBQ2.3", """
                    |select sum(lo_revenue),d_year,p_brand1
                    |from lineorder,part,supplier,ddate
                    |where lo_orderdate = d_datekey
                    |and lo_partkey = p_partkey
                    |and lo_suppkey = s_suppkey
                    |and p_brand1 = 'MFGR#2239'
                    |and s_region = 'EUROPE'
                    |group by d_year,p_brand1
                    |order by d_year,p_brand1
                    | """.stripMargin),

      ("SSBQ3.1", """
                    |select c_nation,s_nation,d_year,sum(lo_revenue) as revenue
                    |from lineorder,customer, supplier,ddate
                    |where lo_custkey = c_custkey
                    |and lo_suppkey = s_suppkey
                    |and lo_orderdate = d_datekey
                    |and c_region = 'ASIA'
                    |and s_region = 'ASIA'
                    |and d_year >=1992 and d_year <= 1997
                    |group by c_nation,s_nation,d_year
                    |order by d_year asc,revenue desc
                    | """.stripMargin),

      ("SSBQ3.2", """
                    |select c_city,s_city,d_year,sum(lo_revenue) as revenue
                    |from lineorder,customer,supplier,ddate
                    |where lo_custkey = c_custkey
                    |and lo_suppkey = s_suppkey
                    |and lo_orderdate = d_datekey
                    |and c_nation = 'UNITED STATES'
                    |and s_nation = 'UNITED STATES'
                    |and d_year >=1992 and d_year <= 1997
                    |group by c_city,s_city,d_year
                    |order by d_year asc,revenue desc
                    | """.stripMargin),

      ("SSBQ3.3", """
                    |select c_city,s_city,d_year,sum(lo_revenue) as revenue
                    |from lineorder,customer,supplier,ddate
                    |where lo_custkey = c_custkey
                    |and lo_suppkey = s_suppkey
                    |and lo_orderdate = d_datekey
                    |and (c_city = 'UNITED KI1' or c_city = 'UNITED KI5')
                    |and (s_city = 'UNITED KI1' or s_city = 'UNITED KI5')
                    |and d_year >=1992 and d_year <= 1997
                    |group by c_city,s_city,d_year
                    |order by d_year asc,revenue desc
                    | """.stripMargin),

      ("SSB 0.1 simple scan", """
                                |select lo_revenue
                                |from lineorder
                                | """.stripMargin),

      ("SSB 0.2 simple scan_filter ", """
                                        |select lo_revenue
                                        |from lineorder
                                        |where lo_quantity<25
                                        | """.stripMargin),

      ("SSB 0.3 one join", """
                             |select c_city
                             |from lineorder,customer
                             |where lo_custkey = c_custkey
                             |and (c_city = 'UNITED KI1' or c_city = 'UNITED KI5')
                             | """.stripMargin)

      ("SSBQ3.4", """
                    |select c_city,s_city,d_year,sum(lo_revenue) as revenue
                    |from lineorder,customer,supplier,ddate
                    |where lo_custkey = c_custkey
                    |and lo_suppkey = s_suppkey
                    |and lo_orderdate = d_datekey
                    |and (c_city = 'UNITED KI1' or c_city = 'UNITED KI5')
                    |and (s_city = 'UNITED KI1' or s_city = 'UNITED KI5')
                    |and d_yearmonth = 'Dec1997'
                    |group by c_city,s_city,d_year
                    |order by d_year asc,revenue desc
                    | """.stripMargin),

      ("SSBQ3.4 Hand Optimized", """
                                   |select c_city,s_city,d_year,sum(lo_revenue) as revenue
                                   |from customer,supplier,ddate,lineorder
                                   |where lo_custkey = c_custkey
                                   |and lo_suppkey = s_suppkey
                                   |and lo_orderdate = d_datekey
                                   |and (c_city = 'UNITED KI1' or c_city = 'UNITED KI5')
                                   |and (s_city = 'UNITED KI1' or s_city = 'UNITED KI5')
                                   |and d_yearmonth = 'Dec1997'
                                   |group by c_city,s_city,d_year
                                   |order by d_year asc,revenue desc
                                   | """.stripMargin)

            ("SSBQ4.1", """
                          |select d_year,c_nation,sum(lo_revenue-lo_supplycost) as profit
                          |from lineorder,supplier,customer,part, ddate
                          |where lo_custkey = c_custkey
                          |and lo_suppkey = s_suppkey
                          |and lo_partkey = p_partkey
                          |and lo_orderdate = d_datekey
                          |and c_region = 'AMERICA'
                          |and s_region = 'AMERICA'
                          |and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
                          |group by d_year,c_nation
                          |order by d_year,c_nation
                          | """.stripMargin),

            ("SSBQ4.2", """
                          |select d_year,s_nation,p_category,sum(lo_revenue-lo_supplycost) as profit
                          |from lineorder,customer,supplier,part,ddate
                          |where lo_custkey = c_custkey
                          |and lo_suppkey = s_suppkey
                          |and lo_partkey = p_partkey
                          |and lo_orderdate = d_datekey
                          |and c_region = 'AMERICA'
                          |and s_region = 'AMERICA'
                          |and (d_year = 1997 or d_year = 1998)
                          |and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
                          |group by d_year,s_nation, p_category
                          |order by d_year,s_nation, p_category
                          | """.stripMargin),

            ("SSBQ4.3", """
                          |select d_year,s_city,p_brand1,sum(lo_revenue-lo_supplycost) as profit
                          |from lineorder,supplier,customer,part,ddate
                          |where lo_custkey = c_custkey
                          |and lo_suppkey = s_suppkey
                          |and lo_partkey = p_partkey
                          |and lo_orderdate = d_datekey
                          |and c_region = 'AMERICA'
                          |and s_nation = 'UNITED STATES'
                          |and (d_year = 1997 or d_year = 1998)
                          |and p_category = 'MFGR#14'
                          |group by d_year,s_city,p_brand1
                          |order by d_year,s_city,p_brand1
                          | """.stripMargin)
    )

    val ITERATIONS = 20
    ssbQueries.foreach { case (queryName, sqlString) =>
      val queryResults = sqlContext.sql(sqlString)
      (1 to ITERATIONS).foreach { iterationNumber =>
        val startTime = System.nanoTime()
        queryResults.foreachPartition(partition => println("Running the partition"))
        val endTime = System.nanoTime()
        val executionTime = endTime - startTime;

        println(f"ITERATION #${iterationNumber}%3d: ${queryName}%s: Exec time: ${executionTime}%,15d ns")

      }
    }
  }
}
