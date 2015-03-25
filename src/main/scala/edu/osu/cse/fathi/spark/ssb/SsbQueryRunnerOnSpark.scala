package edu.osu.cse.fathi.spark.ssb

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

//PART
// 0 P_PARTKEY:INTEGER
// 1 P_NAME:TEXT
// 2 P_MFGR:TEXT
// 3 P_CATEGORY:TEXT
// 4 P_BRAND1:TEXT
// 5 P_COLOR:TEXT
// 6 P_TYPE:TEXT
// 7 P_SIZE:INTEGER
// 8 P_CONTAINER:TEXT

//SUPPLIER
// 0 S_SUPPKEY:INTEGER
// 1 S_NAME:TEXT
// 2 S_ADDRESS:TEXT
// 3 S_CITY:TEXT
// 4 S_NATION:TEXT
// 5 S_REGION:TEXT
// 6 S_PHONE:TEXT

//CUSTOMER
// 0 C_CUSTKEY:INTEGER
// 1 C_NAME:TEXT
// 2 C_ADDRESS:TEXT
// 3 C_CITY:TEXT
// 4 C_NATION:TEXT
// 5 C_REGION:TEXT
// 6 C_PHONE:TEXT
// 7 C_MKTSEGMENT:TEXT

//LINEORDER
// 0 LO_ORDERKEY:INTEGER
// 1 LO_LINENUMBER:INTEGER
// 2 LO_CUSTKEY:INTEGER
// 3 LO_PARTKEY:INTEGER
// 4 LO_SUPPKEY:INTEGER
// 5 LO_ORDERDATE:DATE
// 6 LO_ORDERPRIORITY:TEXT
// 7 LO_SHIPPRIORITY:TEXT
// 8 LO_QUANTITY:INTEGER
// 9 LO_EXTENDEDPRICE:DECIMAL
//10 LO_ORDTOTALPRICE:DECIMAL
//11 LO_DISCOUNT:INTEGER
//12 LO_REVENUE:DECIMAL
//13 LO_SUPPLYCOST:DECIMAL
//14 LO_TAX:INTEGER
//15 L_COMMITDATE:DATE
//16 L_SHIPMODE:TEXT

//DDATE
// 0 D_DATEKEY:DATE
// 1 D_DATE:TEXT
// 2 D_DAYOFWEEK:TEXT
// 3 D_MONTH:TEXT
// 4 D_YEAR:INTEGER
// 5 D_YEARMONTHNUM:INTEGER
// 6 D_YEARMONTH:TEXT
// 7 D_DAYNUMINWEEK:INTEGER
// 8 D_DAYNUMINMONTH:INTEGER
// 9 D_DAYNUMINYEAR:INTEGER
//10 D_MONTHNUMINYEAR:INTEGER
//11 D_WEEKNUMINYEAR:INTEGER
//12 D_SELLINGSEASON:TEXT
//13 D_LASTDAYINWEEKFL:TEXT
//14 D_LASTDAYINMONTHFL:TEXT
//15 D_HOLIDAYFL:TEXT
//16 D_WEEKDAYFL:TEXT
//

/**
 * Runs SSB all queries on parquet formats. Before executing queries, loads all data into memory.
 */
object SsbQueryRunnerOnSpark {
  def main(args: Array[String]) {
    val config = new SparkConf().setAppName("SSB Queries on Spark [Row Format]")
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)

    if (args.length != 1) {
      Console.print("USAGE: SsbQueryRunnerOnSpark <db home dir>")
      throw new RuntimeException("Not enough number of parameters")
    }
    val dbDir = args(0)


    val allQueries = Seq[Function2[SparkContext, String, Tuple2[RDD[_], String]]](ssb_1_1, hand_opt_ssb_1_1, ssb_1_2, hand_opt_ssb_1_2, ssb_1_3, hand_opt_ssb_1_3, ssb_2_1, hand_opt_ssb_2_1, ssb_2_2, hand_opt_ssb_2_2, ssb_2_3, hand_opt_ssb_2_3, ssb_3_1, hand_opt_ssb_3_1, ssb_3_2, hand_opt_ssb_3_2, ssb_3_3, hand_opt_ssb_3_3, /* ssb_3_4, */ ssb_4_1, hand_opt_ssb_4_1, ssb_4_2, hand_opt_ssb_4_2, ssb_4_3, hand_opt_ssb_4_3)
    val ITERATIONS = 5
    println(f"[PROFILING RESULTS]:ITERATION,QueryName,Exectime")

    allQueries.foreach { (query: (SparkContext, String) => (RDD[_], String)) =>
      val (resultRdd: RDD[_], queryName) = query(sc, dbDir)
      (1 to ITERATIONS).foreach { iterationNumber =>
        val startTime = System.nanoTime()
        resultRdd.foreachPartition(partition => println("Running the partition"))
        val endTime = System.nanoTime()
        val executionTime = endTime - startTime

        println(f"[PROFILING RESULTS]:${iterationNumber}%d,${queryName}%s,${executionTime}%d")

      }
    }
  }

  /**
   * select sum(lo_extendedprice*lo_discount) as revenue
   * from lineorder,ddate
   * where lo_orderdate = d_datekey
   * and d_year = 1993 and lo_discount>=1
   * and lo_discount<=3
   * and lo_quantity<25
   */
  val hand_opt_ssb_1_1 = (sc: SparkContext, dbDir: String) => {
    val rdd000 = sc.textFile(dbDir + "/lineorder*")
    val rdd001: RDD[(String, Int, Float, Int)] = rdd000.map(line => {
      val columns = line.split("\\|")
      (columns(5), columns(8).toInt, columns(9).toFloat, columns(11).toInt)
    })
    val rdd002: RDD[(String, Int, Float, Int)] = rdd001.filter(x => ((x._4 >= 1) && (x._4 <= 3) && (x._2 < 25)))
    val rdd003 = rdd002.map(x => (x._1, x._2 * x._3))
    val rdd004 = sc.textFile(dbDir + "/ddate*").map(line => {
      val columns = line.split("\\|")
      val dateYear = columns(4).toInt
      if (dateYear == 1993) {
        (columns(0), columns(0))
      } else {
        null
      }
    }).filter(_ != null)
    val rdd009: RDD[(Int, Float)] = rdd003.join(rdd004).map(x => (1, x._2._1))
    val rdd011 = rdd009.reduceByKey(_ + _)
    (rdd011, "hand_opt_ssb_1_1")
  }

  val ssb_1_1 = (sc: SparkContext, dbDir: String) => {
    val rdd000 = sc.textFile(dbDir + "/lineorder*")
    val rdd001 = rdd000.map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1).toInt, columns(2).toInt, columns(3).toInt, columns(4).toInt, columns(5), columns(6), columns(7), columns(8).toInt, columns(9).toFloat, columns(10).toFloat, columns(11).toInt, columns(12).toFloat, columns(13).toFloat, columns(14).toInt, columns(15), columns(16))
    })
    val rdd002 = rdd001.filter(x => ((x._12 >= 1) && (x._12 <= 3) && (x._9 < 25)))
    val rdd003 = rdd002.map(x => (x._10, x._12, x._6))
    val rdd004 = sc.textFile(dbDir + "/ddate*").map(line => {
      val columns = line.split("\\|")
      (columns(0), columns(1), columns(2), columns(3), columns(4).toInt, columns(5).toInt, columns(6), columns(7).toInt, columns(8).toInt, columns(9).toInt, columns(10).toInt, columns(11).toInt, columns(12), columns(13), columns(14), columns(15), columns(16))
    })
    val rdd005 = rdd004.filter(x => (x._5 == 1993))
    val rdd006 = rdd005.map(x => Tuple1(x._1))
    val rdd007 = rdd003.map(x => (x._3, x))
    val rdd008 = rdd006.map(x => (x._1, x))
    val rdd009 = rdd007.join(rdd008).map(x => x._2)
    val rdd010 = rdd009.map(x => (1, x._1._1, x._1._2))
    val rdd011 = rdd010.groupBy(x => Tuple1(1))
    val rdd012 = rdd011.map(x => (1, x._2.map(x => (x._2 * x._3)).sum))
    (rdd012, "ssb_1_1")
  }

  /**
   * code{{
   * select sum(lo_extendedprice*lo_discount) as revenue
   * from lineorder,ddate
   * where lo_orderdate = d_datekey
   * and d_yearmonth = '199401'
   * and lo_discount>=4
   * and lo_discount<=6
   * and lo_quantity>=26
   * and lo_quantity<=35
   * }}
   */
  val hand_opt_ssb_1_2 = (sc: SparkContext, dbDir: String) => {

    val rdd000 = sc.textFile(dbDir + "/lineorder*")
    val rdd001: RDD[(String, Int, Float, Int)] = rdd000.map(line => {
      val columns = line.split("\\|")
      (columns(5), columns(8).toInt, columns(9).toFloat, columns(11).toInt)
    })
    val rdd002 = rdd001.filter(x => ((x._4 >= 4) && (x._4 <= 6) && (x._2 >= 26) && (x._2 <= 35)))
    val rdd003: RDD[(String, Float)] = rdd002.map(x => (x._1, x._2 * x._3))
    val rdd004: RDD[(String, String)] = sc.textFile(dbDir + "/ddate*").map(line => {
      val columns = line.split("\\|")
      val dateYearMonth = columns(6)
      if (dateYearMonth == "Jan1994") {
        (columns(0), columns(0))
      } else {
        null
      }
    }).filter(_ != null)
    val rdd009 = rdd003.join(rdd004).map(x => (1, x._2._1))
    val rdd012 = rdd009.reduceByKey(_ + _)
    (rdd012, "hand_opt_ssb_1_2")
  }

  val ssb_1_2 = (sc: SparkContext, dbDir: String) => {

    val rdd001 = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1).toInt, columns(2).toInt, columns(3).toInt, columns(4).toInt, columns(5), columns(6), columns(7), columns(8).toInt, columns(9).toFloat, columns(10).toFloat, columns(11).toInt, columns(12).toFloat, columns(13).toFloat, columns(14).toInt, columns(15), columns(16))
    })
    val rdd002 = rdd001.filter(x => ((x._12 >= 4) && (x._12 <= 6) && (x._9 >= 26) && (x._9 <= 35)))
    val rdd003 = rdd002.map(x => (x._10, x._12, x._6))
    val rdd004 = sc.textFile(dbDir + "/ddate*").map(line => {
      val columns = line.split("\\|")
      (columns(0), columns(1), columns(2), columns(3), columns(4).toInt, columns(5).toInt, columns(6), columns(7).toInt, columns(8).toInt, columns(9).toInt, columns(10).toInt, columns(11).toInt, columns(12), columns(13), columns(14), columns(15), columns(16))
    })
    val rdd005 = rdd004.filter(x => (x._7 == "Jan1994"))
    val rdd006 = rdd005.map(x => Tuple1(x._1))
    val rdd007 = rdd003.map(x => (x._3, x))
    val rdd008 = rdd006.map(x => (x._1, x))
    val rdd009 = rdd007.join(rdd008).map(x => x._2)
    val rdd010 = rdd009.map(x => (1, x._1._1, x._1._2))
    val rdd011 = rdd010.groupBy(x => Tuple1(1))
    val rdd012 = rdd011.map(x => (1, x._2.map(x => (x._2 * x._3)).sum))
    (rdd012, "ssb_1_2")
  }


  /**
   * code{{
   * select sum(lo_extendedprice*lo_discount) as revenue
   * from lineorder,ddate
   * where lo_orderdate = d_datekey
   * and d_weeknuminyear = 6
   * and d_year = 1994
   * and lo_discount>=5
   * and lo_discount<=7
   * and lo_quantity>=26
   * and lo_quantity<=35
   * }}
   */
  val hand_opt_ssb_1_3 = (sc: SparkContext, dbDir: String) => {

    val rdd000 = sc.textFile(dbDir + "/lineorder*")
    val rdd001: RDD[(String, Int, Float, Int)] = rdd000.map(line => {
      val columns = line.split("\\|")
      (columns(5), columns(8).toInt, columns(9).toFloat, columns(11).toInt)
    })
    val rdd002 = rdd001.filter(x => ((x._4 >= 5) && (x._4 <= 7) && (x._2 >= 26) && (x._2 <= 35)))
    val rdd003: RDD[(String, Float)] = rdd002.map(x => (x._1, x._2 * x._3))
    val rdd004: RDD[(String, String)] = sc.textFile(dbDir + "/ddate*").map(line => {
      val columns = line.split("\\|")
      val dateYear = columns(4).toInt
      val weekNumInYear = columns(11).toInt
      if ((dateYear == 1994) && (weekNumInYear == 6)) {
        (columns(0), columns(0))
      } else {
        null
      }
    }).filter(_ != null)
    val rdd009 = rdd003.join(rdd004).map(x => (1, x._2._1))
    val rdd012 = rdd009.reduceByKey(_ + _)
    (rdd012, "hand_opt_ssb_1_3")
  }

  val ssb_1_3 = (sc: SparkContext, dbDir: String) => {
    val rdd001 = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1).toInt, columns(2).toInt, columns(3).toInt, columns(4).toInt, columns(5), columns(6), columns(7), columns(8).toInt, columns(9).toFloat, columns(10).toFloat, columns(11).toInt, columns(12).toFloat, columns(13).toFloat, columns(14).toInt, columns(15), columns(16))
    })
    val rdd002 = rdd001.filter(x => ((x._12 >= 5) && (x._12 <= 7) && (x._9 >= 26) && (x._9 <= 35)))
    val rdd003 = rdd002.map(x => (x._10, x._12, x._6))
    val rdd004 = sc.textFile(dbDir + "/ddate*").map(line => {
      val columns = line.split("\\|")
      (columns(0), columns(1), columns(2), columns(3), columns(4).toInt, columns(5).toInt, columns(6), columns(7).toInt, columns(8).toInt, columns(9).toInt, columns(10).toInt, columns(11).toInt, columns(12), columns(13), columns(14), columns(15), columns(16))
    })
    val rdd005 = rdd004.filter(x => ((x._12 == 6) && (x._5 == 1994)))
    val rdd006 = rdd005.map(x => Tuple1(x._1))
    val rdd007 = rdd003.map(x => (x._3, x))
    val rdd008 = rdd006.map(x => (x._1, x))
    val rdd009 = rdd007.join(rdd008).map(x => x._2)
    val rdd010 = rdd009.map(x => (1, x._1._1, x._1._2))
    val rdd011 = rdd010.groupBy(x => Tuple1(1))
    val rdd012 = rdd011.map(x => (1, x._2.map(x => (x._2 * x._3)).sum))
    (rdd012, "ssb_1_3")
  }

  /**
   *
   * select sum(lo_revenue),d_year,p_brand1
   * from lineorder,ddate,part,supplier
   * where lo_orderdate = d_datekey
   * and lo_partkey = p_partkey
   * and lo_suppkey = s_suppkey
   * and p_category = 'MFGR#12'
   * and s_region = 'AMERICA'
   * group by d_year,p_brand1
   * order by d_year,p_brand1
   *
   */

  val hand_opt_ssb_2_1 = (sc: SparkContext, dbDir: String) => {
    val rddLo: RDD[(String, (Float, Int, Int))] = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(5), (columns(12).toFloat, columns(3).toInt, columns(4).toInt))
    })
    val rdd003: RDD[(String, Int)] = sc.textFile(dbDir + "/ddate*").map(line => {
      val columns = line.split("\\|")
      (columns(0), columns(4).toInt)
    })
    val rddLoDa: RDD[(String, ((Float, Int, Int), Int))] = rddLo.join(rdd003)
    val rddLoDaSuPreJoin: RDD[(Int, (Float, Int, Int))] = rddLoDa.map(x => (x._2._1._2, (x._2._1._1, x._2._1._3, x._2._2)))
    val rddSu: RDD[(Int, String)] = sc.textFile(dbDir + "/supplier*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(5))
    })
    val rddSuFilter: RDD[(Int, Int)] = rddSu.filter(x => (x._2 == "AMERICA")).map(x => (x._1, 0))
    val rddLoDaSu: RDD[(Int, ((Float, Int, Int), Int))] = rddLoDaSuPreJoin.join(rddSuFilter)
    val rddLoDaSuPaPreJoin: RDD[(Int, (Float, Int))] = rddLoDaSu.map(x => (x._2._1._2, (x._2._1._1, x._2._2)))
    val rddPart: RDD[(Int, String, String)] = sc.textFile(dbDir + "/part*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(3), columns(4))
    })
    val rddPartFilter = rddPart.filter(x => (x._2 == "MFGR#12"))
    val rddPartPreJoin: RDD[(Int, String)] = rddPartFilter.map(x => (x._1, x._3))
    val rddLoDaSuPaJoin: RDD[(Int, ((Float, Int), String))] = rddLoDaSuPaPreJoin.join(rddPartPreJoin)
    val rddGroupBy: RDD[((Int, String), Float)] = rddLoDaSuPaJoin.map({ case (partKey, ((revenue, year), brand1)) => ((year, brand1), (revenue))})
    val rddAggregate: RDD[((Int, String), Float)] = rddGroupBy.reduceByKey(_ + _)
    val rddOrderBy = rddAggregate.sortBy({ case ((year, brand1), sumRevenue) => (year, brand1)})
    (rddOrderBy, "hand_opt_ssb_2_1")
  }

  val ssb_2_1 = (sc: SparkContext, dbDir: String) => {
    val rdd001 = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1).toInt, columns(2).toInt, columns(3).toInt, columns(4).toInt, columns(5), columns(6), columns(7), columns(8).toInt, columns(9).toFloat, columns(10).toFloat, columns(11).toInt, columns(12).toFloat, columns(13).toFloat, columns(14).toInt, columns(15), columns(16))
    })
    val rdd002 = rdd001.map(x => (x._13, x._5, x._4, x._6))
    val rdd003 = sc.textFile(dbDir + "/ddate*").map(line => {
      val columns = line.split("\\|")
      (columns(0), columns(1), columns(2), columns(3), columns(4).toInt, columns(5).toInt, columns(6), columns(7).toInt, columns(8).toInt, columns(9).toInt, columns(10).toInt, columns(11).toInt, columns(12), columns(13), columns(14), columns(15), columns(16))
    })
    val rdd004 = rdd003.map(x => (x._5, x._1))
    val rdd005 = rdd002.map(x => (x._4, x))
    val rdd006 = rdd004.map(x => (x._2, x))
    val rdd007 = rdd005.join(rdd006).map(x => x._2)
    val rdd008 = rdd007.map(x => (x._2._1, x._1._1, x._1._2, x._1._3))
    val rdd009 = sc.textFile(dbDir + "/part*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7).toInt, columns(8))
    })
    val rdd010 = rdd009.filter(x => (x._4 == "MFGR#12"))
    val rdd011 = rdd010.map(x => (x._5, x._1))
    val rdd012 = rdd008.map(x => (x._4, x))
    val rdd013 = rdd011.map(x => (x._2, x))
    val rdd014 = rdd012.join(rdd013).map(x => x._2)
    val rdd015 = rdd014.map(x => (x._1._1, x._2._1, x._1._2, x._1._3))
    val rdd016 = sc.textFile(dbDir + "/supplier*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6))
    })
    val rdd017 = rdd016.filter(x => (x._6 == "AMERICA"))
    val rdd018 = rdd017.map(x => Tuple1(x._1))
    val rdd019 = rdd015.map(x => (x._4, x))
    val rdd020 = rdd018.map(x => (x._1, x))
    val rdd021 = rdd019.join(rdd020).map(x => x._2)
    val rdd022 = rdd021.map(x => (x._1._1, x._1._2, x._1._3))
    val rdd023 = rdd022.groupBy(x => (x._1, x._2))
    val rdd024 = rdd023.map(x => (x._1._1, x._1._2, x._2.map(x => x._3).sum))
    (rdd024, "ssb_2_1")
  }

  /**
   * select sum(lo_revenue),d_year,p_brand1
   * from lineorder,ddate,part,supplier
   * where lo_orderdate = d_datekey
   * and lo_partkey = p_partkey
   * and lo_suppkey = s_suppkey
   * and p_brand1 >= 'MFGR#2221'
   * and p_brand1 <= 'MFGR#2228'
   * and s_region = 'ASIA'
   * group by d_year,p_brand1
   * order by d_year,p_brand1
   */
  val hand_opt_ssb_2_2 = (sc: SparkContext, dbDir: String) => {
    val rddLo: RDD[(String, (Float, Int, Int))] = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(5), (columns(12).toFloat, columns(3).toInt, columns(4).toInt))
    })
    val rdd003: RDD[(String, Int)] = sc.textFile(dbDir + "/ddate*").map(line => {
      val columns = line.split("\\|")
      (columns(0), columns(4).toInt)
    })
    val rddLoDa: RDD[(String, ((Float, Int, Int), Int))] = rddLo.join(rdd003)
    val rddLoDaSuPreJoin: RDD[(Int, (Float, Int, Int))] = rddLoDa.map(x => (x._2._1._2, (x._2._1._1, x._2._1._3, x._2._2)))
    val rddSu: RDD[(Int, String)] = sc.textFile(dbDir + "/supplier*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(5))
    })
    val rddSuFilter: RDD[(Int, Int)] = rddSu.filter(x => (x._2 == "ASIA")).map(x => (x._1, 0))
    val rddLoDaSu: RDD[(Int, ((Float, Int, Int), Int))] = rddLoDaSuPreJoin.join(rddSuFilter)
    val rddLoDaSuPaPreJoin: RDD[(Int, (Float, Int))] = rddLoDaSu.map(x => (x._2._1._2, (x._2._1._1, x._2._2)))
    val rddPart: RDD[(Int, String, String)] = sc.textFile(dbDir + "/part*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(3), columns(4))
    })
    val rddPartFilter = rddPart.filter(x => ((x._2 >= "MFGR#2221") && (x._2 <= "MFGR#2228")))
    val rddPartPreJoin: RDD[(Int, String)] = rddPartFilter.map(x => (x._1, x._3))
    val rddLoDaSuPaJoin: RDD[(Int, ((Float, Int), String))] = rddLoDaSuPaPreJoin.join(rddPartPreJoin)
    val rddGroupBy: RDD[((Int, String), Float)] = rddLoDaSuPaJoin.map({ case (partKey, ((revenue, year), brand1)) => ((year, brand1), (revenue))})
    val rddAggregate: RDD[((Int, String), Float)] = rddGroupBy.reduceByKey(_ + _)
    val rddOrderBy = rddAggregate.sortBy({ case ((year, brand1), sumRevenue) => (year, brand1)})
    (rddOrderBy, "hand_opt_ssb_2_2")
  }

  val ssb_2_2 = (sc: SparkContext, dbDir: String) => {
    val rdd001 = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1).toInt, columns(2).toInt, columns(3).toInt, columns(4).toInt, columns(5), columns(6), columns(7), columns(8).toInt, columns(9).toFloat, columns(10).toFloat, columns(11).toInt, columns(12).toFloat, columns(13).toFloat, columns(14).toInt, columns(15), columns(16))
    })
    val rdd002 = rdd001.map(x => (x._13, x._5, x._4, x._6))
    val rdd003 = sc.textFile(dbDir + "/ddate*").map(line => {
      val columns = line.split("\\|")
      (columns(0), columns(1), columns(2), columns(3), columns(4).toInt, columns(5).toInt, columns(6), columns(7).toInt, columns(8).toInt, columns(9).toInt, columns(10).toInt, columns(11).toInt, columns(12), columns(13), columns(14), columns(15), columns(16))
    })
    val rdd004 = rdd003.map(x => (x._5, x._1))
    val rdd005 = rdd002.map(x => (x._4, x))
    val rdd006 = rdd004.map(x => (x._2, x))
    val rdd007 = rdd005.join(rdd006).map(x => x._2)
    val rdd008 = rdd007.map(x => (x._2._1, x._1._1, x._1._2, x._1._3))
    val rdd009 = sc.textFile(dbDir + "/part*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7).toInt, columns(8))
    })
    val rdd010 = rdd009.filter(x => ((x._5 >= "MFGR#2221") && (x._5 <= "MFGR#2228")))
    val rdd011 = rdd010.map(x => (x._5, x._1))
    val rdd012 = rdd008.map(x => (x._4, x))
    val rdd013 = rdd011.map(x => (x._2, x))
    val rdd014 = rdd012.join(rdd013).map(x => x._2)
    val rdd015 = rdd014.map(x => (x._1._1, x._2._1, x._1._2, x._1._3))
    val rdd016 = sc.textFile(dbDir + "/supplier*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6))
    })
    val rdd017 = rdd016.filter(x => (x._6 == "ASIA"))
    val rdd018 = rdd017.map(x => Tuple1(x._1))
    val rdd019 = rdd015.map(x => (x._4, x))
    val rdd020 = rdd018.map(x => (x._1, x))
    val rdd021 = rdd019.join(rdd020).map(x => x._2)
    val rdd022 = rdd021.map(x => (x._1._1, x._1._2, x._1._3))
    val rdd023 = rdd022.groupBy(x => (x._1, x._2))
    val rdd024 = rdd023.map(x => (x._1._1, x._1._2, x._2.map(x => x._3).sum))
    (rdd024, "ssb_2_2")
  }

  /**
   * select sum(lo_revenue),d_year,p_brand1
   * from lineorder,ddate,part,supplier
   * where lo_orderdate = d_datekey
   * and lo_partkey = p_partkey
   * and lo_suppkey = s_suppkey
   * and p_brand1 = 'MFGR#2239'
   * and s_region = 'EUROPE'
   * group by d_year,p_brand1
   * order by d_year,p_brand1
   */
  val hand_opt_ssb_2_3 = (sc: SparkContext, dbDir: String) => {
    val rddLo: RDD[(String, (Float, Int, Int))] = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(5), (columns(12).toFloat, columns(3).toInt, columns(4).toInt))
    })
    val rdd003: RDD[(String, Int)] = sc.textFile(dbDir + "/ddate*").map(line => {
      val columns = line.split("\\|")
      (columns(0), columns(4).toInt)
    })
    val rddLoDa: RDD[(String, ((Float, Int, Int), Int))] = rddLo.join(rdd003)
    val rddLoDaSuPreJoin: RDD[(Int, (Float, Int, Int))] = rddLoDa.map(x => (x._2._1._2, (x._2._1._1, x._2._1._3, x._2._2)))
    val rddSu: RDD[(Int, String)] = sc.textFile(dbDir + "/supplier*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(5))
    })
    val rddSuFilter: RDD[(Int, Int)] = rddSu.filter(x => (x._2 == "EUROPE")).map(x => (x._1, 0))
    val rddLoDaSu: RDD[(Int, ((Float, Int, Int), Int))] = rddLoDaSuPreJoin.join(rddSuFilter)
    val rddLoDaSuPaPreJoin: RDD[(Int, (Float, Int))] = rddLoDaSu.map(x => (x._2._1._2, (x._2._1._1, x._2._2)))
    val rddPart: RDD[(Int, String, String)] = sc.textFile(dbDir + "/part*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(3), columns(4))
    })
    val rddPartFilter = rddPart.filter(x => x._2 == "MFGR#2239")
    val rddPartPreJoin: RDD[(Int, String)] = rddPartFilter.map(x => (x._1, x._3))
    val rddLoDaSuPaJoin: RDD[(Int, ((Float, Int), String))] = rddLoDaSuPaPreJoin.join(rddPartPreJoin)
    val rddGroupBy: RDD[((Int, String), Float)] = rddLoDaSuPaJoin.map({ case (partKey, ((revenue, year), brand1)) => ((year, brand1), (revenue))})
    val rddAggregate: RDD[((Int, String), Float)] = rddGroupBy.reduceByKey(_ + _)
    val rddOrderBy = rddAggregate.sortBy({ case ((year, brand1), sumRevenue) => (year, brand1)})
    (rddOrderBy, "hand_opt_ssb_2_3")
  }

  val ssb_2_3 = (sc: SparkContext, dbDir: String) => {
    val rdd001 = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1).toInt, columns(2).toInt, columns(3).toInt, columns(4).toInt, columns(5), columns(6), columns(7), columns(8).toInt, columns(9).toFloat, columns(10).toFloat, columns(11).toInt, columns(12).toFloat, columns(13).toFloat, columns(14).toInt, columns(15), columns(16))
    })
    val rdd002 = rdd001.map(x => (x._13, x._5, x._4, x._6))
    val rdd003 = sc.textFile(dbDir + "/ddate*").map(line => {
      val columns = line.split("\\|")
      (columns(0), columns(1), columns(2), columns(3), columns(4).toInt, columns(5).toInt, columns(6), columns(7).toInt, columns(8).toInt, columns(9).toInt, columns(10).toInt, columns(11).toInt, columns(12), columns(13), columns(14), columns(15), columns(16))
    })
    val rdd004 = rdd003.map(x => (x._5, x._1))
    val rdd005 = rdd002.map(x => (x._4, x))
    val rdd006 = rdd004.map(x => (x._2, x))
    val rdd007 = rdd005.join(rdd006).map(x => x._2)
    val rdd008 = rdd007.map(x => (x._2._1, x._1._1, x._1._2, x._1._3))
    val rdd009 = sc.textFile(dbDir + "/part*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7).toInt, columns(8))
    })
    val rdd010 = rdd009.filter(x => (x._5 == "MFGR#2239"))
    val rdd011 = rdd010.map(x => (x._5, x._1))
    val rdd012 = rdd008.map(x => (x._4, x))
    val rdd013 = rdd011.map(x => (x._2, x))
    val rdd014 = rdd012.join(rdd013).map(x => x._2)
    val rdd015 = rdd014.map(x => (x._1._1, x._2._1, x._1._2, x._1._3))
    val rdd016 = sc.textFile(dbDir + "/supplier*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6))
    })
    val rdd017 = rdd016.filter(x => (x._6 == "EUROPE"))
    val rdd018 = rdd017.map(x => Tuple1(x._1))
    val rdd019 = rdd015.map(x => (x._4, x))
    val rdd020 = rdd018.map(x => (x._1, x))
    val rdd021 = rdd019.join(rdd020).map(x => x._2)
    val rdd022 = rdd021.map(x => (x._1._1, x._1._2, x._1._3))
    val rdd023 = rdd022.groupBy(x => (x._1, x._2))
    val rdd024 = rdd023.map(x => (x._1._1, x._1._2, x._2.map(x => x._3).sum))
    (rdd024, "ssb_2_3")
  }

  /**
   * select c_nation,s_nation,d_year,sum(lo_revenue) as revenue
   * from customer,lineorder,supplier,ddate
   * where lo_custkey = c_custkey
   * and lo_suppkey = s_suppkey
   * and lo_orderdate = d_datekey
   * and c_region = 'ASIA'
   * and s_region = 'ASIA'
   * and d_year >=1992 and d_year <= 1997
   * group by c_nation,s_nation,d_year
   * order by d_year asc,revenue desc
   */
  val hand_opt_ssb_3_1 = (sc: SparkContext, dbDir: String) => {
    val rddDate = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(4).toInt)
    })
    val rddDateFilter = rddDate.filter { case (dateKey, year) => year >= 1992 && year <= 1997}

    val rddLineOrder = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(5).toInt, (columns(4).toInt, columns(2).toInt, columns(12).toFloat))
    })
    val rddLoDate = rddLineOrder.join(rddDateFilter).map { case (key, ((customerKey, supplyKey, revenue), year)) => (supplyKey, (customerKey, year, revenue))}


    val rddSupplier = sc.textFile(dbDir + "/supplier*").map(line => {
      val columns = line.split("\\|")
      val supplierKey = columns(0).toInt
      val supplierNation = columns(4)
      val supplierRegion = columns(5)
      (supplierKey, (supplierNation, supplierRegion))
    })
    val rddSupplierFilter = rddSupplier.filter { case (supplierKey, (supplierNation, supplierRegion)) =>
      supplierRegion == "ASIA"
    }

    val rddLoDateSupplier: RDD[(Int, (Int, String, Float))] = rddLoDate.join(rddSupplierFilter).map {
      case (supplyKey, ((customerKey, year, revenue), (supplierNation, supplierRegion))) =>
        (customerKey, (year, supplierNation, revenue))
    }

    val rddCustomer = sc.textFile(dbDir + "/customer*").map(line => {
      val columns = line.split("\\|")
      val customerKey = columns(0).toInt
      val customerNation = columns(4)
      val customerRegion = columns(5)
      (customerKey, (customerNation, customerRegion))
    })
    val rddCustomerFilter = rddCustomer.filter {
      case (customerKey, (customerNation, customerRegion)) => customerRegion == "ASIA"
    }

    val rddLoDateSupplierCustomer = rddLoDateSupplier.join(rddCustomerFilter).map {
      case (customerKey, ((year, supplierNation, revenue), (customerNation, customerRegion))) =>
        ((customerNation, supplierNation, year), revenue)
    }

    val rddAggregate = rddLoDateSupplierCustomer.reduceByKey(_ + _)

    val rddSorted: RDD[((String, String, Int), Float)] = rddAggregate.sortBy { case ((customerNation, supplierNation, year), totalRevenue) =>
      ((year, totalRevenue), customerNation, supplierNation)
    }

    (rddSorted, "hand_opt_ssb_3_1")
  }

  val ssb_3_1 = (sc: SparkContext, dbDir: String) => {
    val rdd001 = sc.textFile(dbDir + "/customer*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7))
    })
    val rdd002 = rdd001.filter(x => (x._6 == "ASIA"))
    val rdd003 = rdd002.map(x => (x._5, x._1))
    val rdd004 = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1).toInt, columns(2).toInt, columns(3).toInt, columns(4).toInt, columns(5), columns(6), columns(7), columns(8).toInt, columns(9).toFloat, columns(10).toFloat, columns(11).toInt, columns(12).toFloat, columns(13).toFloat, columns(14).toInt, columns(15), columns(16))
    })
    val rdd005 = rdd004.map(x => (x._13, x._6, x._5, x._3))
    val rdd006 = rdd003.map(x => (x._2, x))
    val rdd007 = rdd005.map(x => (x._4, x))
    val rdd008 = rdd006.join(rdd007).map(x => x._2)
    val rdd009 = rdd008.map(x => (x._1._1, x._2._1, x._2._2, x._2._3))
    val rdd010 = sc.textFile(dbDir + "/supplier*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6))
    })
    val rdd011 = rdd010.filter(x => (x._6 == "ASIA"))
    val rdd012 = rdd011.map(x => (x._5, x._1))
    val rdd013 = rdd009.map(x => (x._4, x))
    val rdd014 = rdd012.map(x => (x._2, x))
    val rdd015 = rdd013.join(rdd014).map(x => x._2)
    val rdd016 = rdd015.map(x => (x._1._1, x._2._1, x._1._2, x._1._3))
    val rdd017 = sc.textFile(dbDir + "/ddate*").map(line => {
      val columns = line.split("\\|")
      (columns(0), columns(1), columns(2), columns(3), columns(4).toInt, columns(5).toInt, columns(6), columns(7).toInt, columns(8).toInt, columns(9).toInt, columns(10).toInt, columns(11).toInt, columns(12), columns(13), columns(14), columns(15), columns(16))
    })
    val rdd018 = rdd017.filter(x => ((x._5 >= 1992) && (x._5 <= 1997)))
    val rdd019 = rdd018.map(x => (x._5, x._1))
    val rdd020 = rdd016.map(x => (x._4, x))
    val rdd021 = rdd019.map(x => (x._2, x))
    val rdd022 = rdd020.join(rdd021).map(x => x._2)
    val rdd023 = rdd022.map(x => (x._1._1, x._1._2, x._2._1, x._1._3))
    val rdd024 = rdd023.groupBy(x => (x._1, x._2, x._3))
    val rdd025 = rdd024.map(x => (x._1._1, x._1._2, x._1._3, x._2.map(x => x._4).sum, x._2.map(x => x._4).sum))

    (rdd025, "ssb_3_1")
  }

  /**
   * select c_city,s_city,d_year,sum(lo_revenue) as revenue
   * from customer,lineorder,supplier,ddate
   * where lo_custkey = c_custkey
   * and lo_suppkey = s_suppkey
   * and lo_orderdate = d_datekey
   * and c_nation = 'UNITED STATES'
   * and s_nation = 'UNITED STATES'
   * and d_year >=1992 and d_year <= 1997
   * group by c_city,s_city,d_year
   * order by d_year asc,revenue desc
   */
  val hand_opt_ssb_3_2 = (sc: SparkContext, dbDir: String) => {
    val rddDate = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(4).toInt)
    })
    val rddDateFilter = rddDate.filter { case (dateKey, year) => year >= 1992 && year <= 1997}

    val rddLineOrder = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(5).toInt, (columns(4).toInt, columns(2).toInt, columns(12).toFloat))
    })
    val rddLoDate = rddLineOrder.join(rddDateFilter).map { case (key, ((customerKey, supplyKey, revenue), year)) => (supplyKey, (customerKey, year, revenue))}


    val rddSupplier = sc.textFile(dbDir + "/supplier*").map(line => {
      val columns = line.split("\\|")
      val supplierKey = columns(0).toInt
      val supplierCity = columns(3)
      val supplierNation = columns(5)
      (supplierKey, (supplierCity, supplierNation))
    })
    val rddSupplierFilter = rddSupplier.filter { case (supplierKey, (supplierCity, supplierNation)) =>
      supplierNation == "UNITED STATES"
    }

    val rddLoDateSupplier: RDD[(Int, (Int, String, Float))] = rddLoDate.join(rddSupplierFilter).map {
      case (supplyKey, ((customerKey, year, revenue), (supplierCity, supplierNation))) =>
        (customerKey, (year, supplierCity, revenue))
    }

    val rddCustomer = sc.textFile(dbDir + "/customer*").map(line => {
      val columns = line.split("\\|")
      val customerKey = columns(0).toInt
      val customerCity = columns(3)
      val customerNation = columns(4)
      (customerKey, (customerCity, customerNation))
    })
    val rddCustomerFilter = rddCustomer.filter {
      case (customerKey, (customerCity, customerNation)) => customerNation == "UNITED STATES"
    }

    val rddLoDateSupplierCustomer = rddLoDateSupplier.join(rddCustomerFilter).map {
      case (customerKey, ((year, supplierCity, revenue), (customerCity, customerNation))) =>
        ((customerCity, supplierCity, year), revenue)
    }

    val rddAggregate = rddLoDateSupplierCustomer.reduceByKey(_ + _)

    val rddSorted: RDD[((String, String, Int), Float)] = rddAggregate.sortBy { case ((customerCity, supplierCity, year), totalRevenue) =>
      ((year, totalRevenue), customerCity, supplierCity)
    }

    (rddSorted, "hand_opt_ssb_3_2")
  }

  val ssb_3_2 = (sc: SparkContext, dbDir: String) => {
    val rdd001 = sc.textFile(dbDir + "/customer*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7))
    })
    val rdd002 = rdd001.filter(x => (x._5 == "UNITED STATES"))
    val rdd003 = rdd002.map(x => (x._4, x._1))
    val rdd004 = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1).toInt, columns(2).toInt, columns(3).toInt, columns(4).toInt, columns(5), columns(6), columns(7), columns(8).toInt, columns(9).toFloat, columns(10).toFloat, columns(11).toInt, columns(12).toFloat, columns(13).toFloat, columns(14).toInt, columns(15), columns(16))
    })
    val rdd005 = rdd004.map(x => (x._13, x._6, x._5, x._3))
    val rdd006 = rdd003.map(x => (x._2, x))
    val rdd007 = rdd005.map(x => (x._4, x))
    val rdd008 = rdd006.join(rdd007).map(x => x._2)
    val rdd009 = rdd008.map(x => (x._1._1, x._2._1, x._2._2, x._2._3))
    val rdd010 = sc.textFile(dbDir + "/supplier*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6))
    })
    val rdd011 = rdd010.filter(x => (x._5 == "UNITED STATES"))
    val rdd012 = rdd011.map(x => (x._4, x._1))
    val rdd013 = rdd009.map(x => (x._4, x))
    val rdd014 = rdd012.map(x => (x._2, x))
    val rdd015 = rdd013.join(rdd014).map(x => x._2)
    val rdd016 = rdd015.map(x => (x._1._1, x._2._1, x._1._2, x._1._3))
    val rdd017 = sc.textFile(dbDir + "/ddate*").map(line => {
      val columns = line.split("\\|")
      (columns(0), columns(1), columns(2), columns(3), columns(4).toInt, columns(5).toInt, columns(6), columns(7).toInt, columns(8).toInt, columns(9).toInt, columns(10).toInt, columns(11).toInt, columns(12), columns(13), columns(14), columns(15), columns(16))
    })
    val rdd018 = rdd017.filter(x => ((x._5 >= 1992) && (x._5 <= 1997)))
    val rdd019 = rdd018.map(x => (x._5, x._1))
    val rdd020 = rdd016.map(x => (x._4, x))
    val rdd021 = rdd019.map(x => (x._2, x))
    val rdd022 = rdd020.join(rdd021).map(x => x._2)
    val rdd023 = rdd022.map(x => (x._1._1, x._1._2, x._2._1, x._1._3))
    val rdd024 = rdd023.groupBy(x => (x._1, x._2, x._3))
    val rdd025 = rdd024.map(x => (x._1._1, x._1._2, x._1._3, x._2.map(x => x._4).sum, x._2.map(x => x._4).sum))
    (rdd025, "ssb_3_2")
  }

  /**
   * select c_city,s_city,d_year,sum(lo_revenue) as revenue
   * from customer,lineorder,supplier,ddate
   * where lo_custkey = c_custkey
   * and lo_suppkey = s_suppkey
   * and lo_orderdate = d_datekey
   * and (c_city = 'UNITED KI1' or c_city = 'UNITED KI5')
   * and (s_city = 'UNITED KI1' or s_city = 'UNITED KI5')
   * and d_year >=1992 and d_year <= 1997
   * group by c_city,s_city,d_year
   * order by d_year asc,revenue desc
   */
  val hand_opt_ssb_3_3 = (sc: SparkContext, dbDir: String) => {
    val rddDate = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(4).toInt)
    })
    val rddDateFilter = rddDate.filter { case (dateKey, year) => year >= 1992 && year <= 1997}

    val rddLineOrder = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(5).toInt, (columns(4).toInt, columns(2).toInt, columns(12).toFloat))
    })
    val rddLoDate = rddLineOrder.join(rddDateFilter).map { case (key, ((customerKey, supplyKey, revenue), year)) => (supplyKey, (customerKey, year, revenue))}


    val rddSupplier = sc.textFile(dbDir + "/supplier*").map(line => {
      val columns = line.split("\\|")
      val supplierKey = columns(0).toInt
      val supplierCity = columns(3)
      (supplierKey, supplierCity)
    })
    val rddSupplierFilter = rddSupplier.filter { case (supplierKey, supplierCity) =>
      supplierCity == "UNITED KI1" || supplierCity == "UNITED KI15"
    }

    val rddLoDateSupplier: RDD[(Int, (Int, String, Float))] = rddLoDate.join(rddSupplierFilter).map {
      case (supplyKey, ((customerKey, year, revenue), supplierCity)) =>
        (customerKey, (year, supplierCity, revenue))
    }

    val rddCustomer = sc.textFile(dbDir + "/customer*").map(line => {
      val columns = line.split("\\|")
      val customerKey = columns(0).toInt
      val customerCity = columns(3)
      (customerKey, customerCity)
    })
    val rddCustomerFilter = rddCustomer.filter {
      case (customerKey, customerCity) =>
        customerCity == "UNITED KI1" || customerCity == "UNITED KI15"
    }

    val rddLoDateSupplierCustomer = rddLoDateSupplier.join(rddCustomer).map {
      case (customerKey, ((year, supplierCity, revenue), customerCity)) =>
        ((customerCity, supplierCity, year), revenue)
    }

    val rddAggregate = rddLoDateSupplierCustomer.reduceByKey(_ + _)

    val rddSorted: RDD[((String, String, Int), Float)] = rddAggregate.sortBy { case ((customerCity, supplierCity, year), totalRevenue) =>
      ((year, totalRevenue), customerCity, supplierCity)
    }
    (rddSorted, "hand_opt_ssb_3_3")
  }

  val ssb_3_3 = (sc: SparkContext, dbDir: String) => {
    val rdd001 = sc.textFile(dbDir + "/customer*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7))
    })
    val rdd002 = rdd001.filter(x => ((x._4 == "UNITED KI1") || (x._4 == "UNITED KI5")))
    val rdd003 = rdd002.map(x => (x._4, x._1))
    val rdd004 = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1).toInt, columns(2).toInt, columns(3).toInt, columns(4).toInt, columns(5), columns(6), columns(7), columns(8).toInt, columns(9).toFloat, columns(10).toFloat, columns(11).toInt, columns(12).toFloat, columns(13).toFloat, columns(14).toInt, columns(15), columns(16))
    })
    val rdd005 = rdd004.map(x => (x._13, x._6, x._5, x._3))
    val rdd006 = rdd003.map(x => (x._2, x))
    val rdd007 = rdd005.map(x => (x._4, x))
    val rdd008 = rdd006.join(rdd007).map(x => x._2)
    val rdd009 = rdd008.map(x => (x._1._1, x._2._1, x._2._2, x._2._3))
    val rdd010 = sc.textFile(dbDir + "/supplier*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6))
    })
    val rdd011 = rdd010.filter(x => ((x._4 == "UNITED KI1") || (x._4 == "UNITED KI5")))
    val rdd012 = rdd011.map(x => (x._4, x._1))
    val rdd013 = rdd009.map(x => (x._4, x))
    val rdd014 = rdd012.map(x => (x._2, x))
    val rdd015 = rdd013.join(rdd014).map(x => x._2)
    val rdd016 = rdd015.map(x => (x._1._1, x._2._1, x._1._2, x._1._3))
    val rdd017 = sc.textFile(dbDir + "/ddate*").map(line => {
      val columns = line.split("\\|")
      (columns(0), columns(1), columns(2), columns(3), columns(4).toInt, columns(5).toInt, columns(6), columns(7).toInt, columns(8).toInt, columns(9).toInt, columns(10).toInt, columns(11).toInt, columns(12), columns(13), columns(14), columns(15), columns(16))
    })
    val rdd018 = rdd017.filter(x => ((x._5 >= 1992) && (x._5 <= 1997)))
    val rdd019 = rdd018.map(x => (x._5, x._1))
    val rdd020 = rdd016.map(x => (x._4, x))
    val rdd021 = rdd019.map(x => (x._2, x))
    val rdd022 = rdd020.join(rdd021).map(x => x._2)
    val rdd023 = rdd022.map(x => (x._1._1, x._1._2, x._2._1, x._1._3))
    val rdd024 = rdd023.groupBy(x => (x._1, x._2, x._3))
    val rdd025 = rdd024.map(x => (x._1._1, x._1._2, x._1._3, x._2.map(x => x._4).sum, x._2.map(x => x._4).sum))
    (rdd025, "ssb_3_3")
  }

  /**
   * select d_year,c_nation,sum(lo_revenue-lo_supplycost) as profit
   * from lineorder,ddate,customer,supplier,part
   * where lo_custkey = c_custkey
   * and lo_suppkey = s_suppkey
   * and lo_partkey = p_partkey
   * and lo_orderdate = d_datekey
   * and c_region = 'AMERICA'
   * and s_region = 'AMERICA'
   * and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
   * group by d_year,c_nation
   * order by d_year,c_nation
   */
  val hand_opt_ssb_4_1 = (sc: SparkContext, dbDir: String) => {
    val rddDate = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(4).toInt)
    })

    val rddLineOrder = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      val orderDate = columns(5).toInt
      val customerKey = columns(2).toInt
      val supplierKey = columns(4).toInt
      val partKey = columns(3).toInt
      val revenue = columns(12).toFloat
      val supplyCost = columns(13).toFloat
      (orderDate, (supplierKey, customerKey, partKey, revenue - supplyCost))
    })
    val rddLoDate = rddLineOrder.join(rddDate).map {
      case (key, ((supplierKey, customerKey, partKey, profit), year)) =>
        (supplierKey, (customerKey, partKey, profit, year))
    }


    val rddSupplier = sc.textFile(dbDir + "/supplier*").map(line => {
      val columns = line.split("\\|")
      val supplierKey = columns(0).toInt
      val supplierRegion = columns(5)
      (supplierKey, supplierRegion)
    })
    val rddSupplierFilter = rddSupplier.filter { case (supplierKey, supplierRegion) =>
      supplierRegion == "AMERICA"
    }

    val rddLoDateSupplier = rddLoDate.join(rddSupplierFilter).map {
      case (supplyKey, ((customerKey, partKey, profit, year), supplierRegion)) =>
        (customerKey, (partKey, profit, year))
    }

    val rddCustomer = sc.textFile(dbDir + "/customer*").map(line => {
      val columns = line.split("\\|")
      val customerKey = columns(0).toInt
      val customerNation = columns(4)
      val customerRegion = columns(5)
      (customerKey, (customerNation, customerRegion))
    })
    val rddCustomerFilter = rddCustomer.filter {
      case (customerKey, (customerNation, customerRegion)) =>
        customerRegion == "AMERICA"
    }

    val rddLoDateSupplierCustomer: RDD[(Int, ((Int, String), Float))] = rddLoDateSupplier.join(rddCustomerFilter).map {
      case (customerKey, ((partKey, profit, year), (customerNation, customerRegion))) =>
        (partKey, ((year, customerNation), profit))
    }

    val rddPart = sc.textFile(dbDir + "/part*").map(line => {
      val columns = line.split("\\|")
      val partKey = columns(0).toInt
      val partMfgr = columns(2)
      (partKey, partMfgr)
    })

    val rddPartFilter: RDD[(Int, String)] = rddPart.filter {
      case (partKey, partMfgr) =>
        partMfgr == "MFGR#1" || partMfgr == "MFGR#2"
    }

    val rddLoDateSupplierCustomerPart = rddLoDateSupplierCustomer.join(rddPartFilter).map {
      case (partKey, (leftJoinSide, _)) =>
        leftJoinSide
    }

    val rddAggregate = rddLoDateSupplierCustomerPart.reduceByKey(_ + _)

    val rddSorted = rddAggregate.sortByKey()
    (rddSorted, "hand_opt_ssb_4_1")
  }

  val ssb_4_1 = (sc: SparkContext, dbDir: String) => {
    val rdd001 = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1).toInt, columns(2).toInt, columns(3).toInt, columns(4).toInt, columns(5), columns(6), columns(7), columns(8).toInt, columns(9).toFloat, columns(10).toFloat, columns(11).toInt, columns(12).toFloat, columns(13).toFloat, columns(14).toInt, columns(15), columns(16))
    })
    val rdd002 = rdd001.map(x => (x._13, x._14, x._4, x._5, x._3, x._6))
    val rdd003 = sc.textFile(dbDir + "/ddate*").map(line => {
      val columns = line.split("\\|")
      (columns(0), columns(1), columns(2), columns(3), columns(4).toInt, columns(5).toInt, columns(6), columns(7).toInt, columns(8).toInt, columns(9).toInt, columns(10).toInt, columns(11).toInt, columns(12), columns(13), columns(14), columns(15), columns(16))
    })
    val rdd004 = rdd003.map(x => (x._5, x._1))
    val rdd005 = rdd002.map(x => (x._6, x))
    val rdd006 = rdd004.map(x => (x._2, x))
    val rdd007 = rdd005.join(rdd006).map(x => x._2)
    val rdd008 = rdd007.map(x => (x._2._1, x._1._1, x._1._2, x._1._3, x._1._4, x._1._5))
    val rdd009 = sc.textFile(dbDir + "/customer*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7))
    })
    val rdd010 = rdd009.filter(x => (x._6 == "AMERICA"))
    val rdd011 = rdd010.map(x => (x._5, x._1))
    val rdd012 = rdd008.map(x => (x._6, x))
    val rdd013 = rdd011.map(x => (x._2, x))
    val rdd014 = rdd012.join(rdd013).map(x => x._2)
    val rdd015 = rdd014.map(x => (x._1._1, x._2._1, x._1._2, x._1._3, x._1._4, x._1._5))
    val rdd016 = sc.textFile(dbDir + "/supplier*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6))
    })
    val rdd017 = rdd016.filter(x => (x._6 == "AMERICA"))
    val rdd018 = rdd017.map(x => Tuple1(x._1))
    val rdd019 = rdd015.map(x => (x._6, x))
    val rdd020 = rdd018.map(x => (x._1, x))
    val rdd021 = rdd019.join(rdd020).map(x => x._2)
    val rdd022 = rdd021.map(x => (x._1._1, x._1._2, x._1._3, x._1._4, x._1._5))
    val rdd023 = sc.textFile(dbDir + "/part*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7).toInt, columns(8))
    })
    val rdd024 = rdd023.filter(x => ((x._3 == "MFGR#1") || (x._3 == "MFGR#2")))
    val rdd025 = rdd024.map(x => (x._3, x._1))
    val rdd026 = rdd022.map(x => (x._5, x))
    val rdd027 = rdd025.map(x => (x._2, x))
    val rdd028 = rdd026.join(rdd027).map(x => x._2)
    val rdd029 = rdd028.map(x => (x._1._1, x._1._2, x._1._3, x._1._4))
    val rdd030 = rdd029.groupBy(x => (x._1, x._2))
    val rdd031 = rdd030.map(x => (x._1._1, x._1._2, x._2.map(x => (x._3 - x._4)).sum))
    (rdd031, "ssb_4_1")
  }

  /**
   * select d_year,s_nation,p_category,sum(lo_revenue-lo_supplycost) as profit
   * from lineorder,ddate,customer,supplier,part
   * where lo_custkey = c_custkey
   * and lo_suppkey = s_suppkey
   * and lo_partkey = p_partkey
   * and lo_orderdate = d_datekey
   * and c_region = 'AMERICA'
   * and s_region = 'AMERICA'
   * and (d_year = 1997 or d_year = 1998)
   * and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
   * group by d_year,s_nation, p_category
   * order by d_year,s_nation, p_category
   */
  val hand_opt_ssb_4_2 = (sc: SparkContext, dbDir: String) => {
    val rddDate = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(4).toInt)
    })
    val rddDateFilter = rddDate.filter { case (dateKey, year) => year >= 1992 && year <= 1997}

    val rddLineOrder = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      val orderDate = columns(5).toInt
      val customerKey = columns(2).toInt
      val supplierKey = columns(4).toInt
      val partKey = columns(3).toInt
      val revenue = columns(12).toFloat
      val supplyCost = columns(13).toFloat
      (orderDate, (supplierKey, customerKey, partKey, revenue - supplyCost))
    })
    val rddLoDate = rddLineOrder.join(rddDateFilter).map {
      case (key, ((supplierKey, customerKey, partKey, profit), year)) =>
        (supplierKey, (customerKey, partKey, profit, year))
    }


    val rddSupplier = sc.textFile(dbDir + "/supplier*").map(line => {
      val columns = line.split("\\|")
      val supplierKey = columns(0).toInt
      val supplierNation = columns(4)
      val supplierRegion = columns(5)
      (supplierKey, (supplierNation, supplierRegion))
    })
    val rddSupplierFilter = rddSupplier.filter { case (supplierKey, (supplierNation, supplierRegion)) =>
      supplierRegion == "AMERICA"
    }

    val rddLoDateSupplier = rddLoDate.join(rddSupplierFilter).map {
      case (supplyKey, ((customerKey, partKey, profit, year), (supplierNation, supplierRegion))) =>
        (customerKey, (partKey, profit, year, supplierNation))
    }

    val rddCustomer = sc.textFile(dbDir + "/customer*").map(line => {
      val columns = line.split("\\|")
      val customerKey = columns(0).toInt
      val customerRegion = columns(5)
      (customerKey, customerRegion)
    })
    val rddCustomerFilter = rddCustomer.filter {
      case (customerKey, customerRegion) =>
        customerRegion == "AMERICA"
    }

    val rddLoDateSupplierCustomer = rddLoDateSupplier.join(rddCustomerFilter).map {
      case (customerKey, ((partKey, profit, year, supplierNation), customerNation)) =>
        (partKey, (year, supplierNation, profit))
    }

    val rddPart = sc.textFile(dbDir + "/part*").map(line => {
      val columns = line.split("\\|")
      val partKey = columns(0).toInt
      val partMfgr = columns(2)
      val partCategory = columns(3)
      (partKey, (partCategory, partMfgr))
    })

    val rddPartFilter = rddPart.filter {
      case (partKey, (partCategory, partMfgr)) =>
        partMfgr == "MFGR#1" || partMfgr == "MFGR#2"
    }

    val rddLoDateSupplierCustomerPart = rddLoDateSupplierCustomer.join(rddPartFilter).map {
      case (partKey, ((year, supplierNation, profit), (partCategory, partMfgr))) =>
        ((year, supplierNation, partCategory), profit)
    }

    val rddAggregate = rddLoDateSupplierCustomerPart.reduceByKey(_ + _)

    val rddSorted = rddAggregate.sortByKey()
    (rddSorted, "hand_opt_ssb_4_2")
  }

  val ssb_4_2 = (sc: SparkContext, dbDir: String) => {
    val rdd001 = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1).toInt, columns(2).toInt, columns(3).toInt, columns(4).toInt, columns(5), columns(6), columns(7), columns(8).toInt, columns(9).toFloat, columns(10).toFloat, columns(11).toInt, columns(12).toFloat, columns(13).toFloat, columns(14).toInt, columns(15), columns(16))
    })
    val rdd002 = rdd001.map(x => (x._13, x._14, x._4, x._5, x._3, x._6))
    val rdd003 = sc.textFile(dbDir + "/ddate*").map(line => {
      val columns = line.split("\\|")
      (columns(0), columns(1), columns(2), columns(3), columns(4).toInt, columns(5).toInt, columns(6), columns(7).toInt, columns(8).toInt, columns(9).toInt, columns(10).toInt, columns(11).toInt, columns(12), columns(13), columns(14), columns(15), columns(16))
    })
    val rdd004 = rdd003.filter(x => ((x._5 == 1997) || (x._5 == 1998)))
    val rdd005 = rdd004.map(x => (x._5, x._1))
    val rdd006 = rdd002.map(x => (x._6, x))
    val rdd007 = rdd005.map(x => (x._2, x))
    val rdd008 = rdd006.join(rdd007).map(x => x._2)
    val rdd009 = rdd008.map(x => (x._2._1, x._1._1, x._1._2, x._1._3, x._1._4, x._1._5))
    val rdd010 = sc.textFile(dbDir + "/customer*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7))
    })
    val rdd011 = rdd010.filter(x => (x._6 == "AMERICA"))
    val rdd012 = rdd011.map(x => Tuple1(x._1))
    val rdd013 = rdd009.map(x => (x._6, x))
    val rdd014 = rdd012.map(x => (x._1, x))
    val rdd015 = rdd013.join(rdd014).map(x => x._2)
    val rdd016 = rdd015.map(x => (x._1._1, x._1._2, x._1._3, x._1._4, x._1._5))
    val rdd017 = sc.textFile(dbDir + "/supplier*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6))
    })
    val rdd018 = rdd017.filter(x => (x._6 == "AMERICA"))
    val rdd019 = rdd018.map(x => (x._5, x._1))
    val rdd020 = rdd016.map(x => (x._5, x))
    val rdd021 = rdd019.map(x => (x._2, x))
    val rdd022 = rdd020.join(rdd021).map(x => x._2)
    val rdd023 = rdd022.map(x => (x._1._1, x._2._1, x._1._2, x._1._3, x._1._4))
    val rdd024 = sc.textFile(dbDir + "/part*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7).toInt, columns(8))
    })
    val rdd025 = rdd024.filter(x => ((x._3 == "MFGR#1") || (x._3 == "MFGR#2")))
    val rdd026 = rdd025.map(x => (x._4, x._3, x._1))
    val rdd027 = rdd023.map(x => (x._5, x))
    val rdd028 = rdd026.map(x => (x._3, x))
    val rdd029 = rdd027.join(rdd028).map(x => x._2)
    val rdd030 = rdd029.map(x => (x._1._1, x._1._2, x._2._1, x._1._3, x._1._4))
    val rdd031 = rdd030.groupBy(x => (x._1, x._2, x._3))
    val rdd032 = rdd031.map(x => (x._1._1, x._1._2, x._1._3, x._2.map(x => (x._4 - x._5)).sum))
    (rdd032, "ssb_4_2")
  }

  /**
   * select d_year,s_city,p_brand1,sum(lo_revenue-lo_supplycost) as profit
   * from lineorder,ddate,customer,supplier,part
   * where lo_custkey = c_custkey
   * and lo_suppkey = s_suppkey
   * and lo_partkey = p_partkey
   * and lo_orderdate = d_datekey
   * and s_nation = 'UNITED STATES'
   * and (d_year = 1997 or d_year = 1998)
   * and p_category = 'MFGR#14'
   * group by d_year,s_city,p_brand1
   * order by d_year,s_city,p_brand1
   */
  val hand_opt_ssb_4_3 = (sc: SparkContext, dbDir: String) => {
    val rddDate = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(4).toInt)
    })
    val rddDateFilter = rddDate.filter { case (dateKey, year) => year >= 1992 && year <= 1997}

    val rddLineOrder = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      val orderDate = columns(5).toInt
      val customerKey = columns(2).toInt
      val supplierKey = columns(4).toInt
      val partKey = columns(3).toInt
      val revenue = columns(12).toFloat
      val supplyCost = columns(13).toFloat
      (orderDate, (supplierKey, customerKey, partKey, revenue - supplyCost))
    })
    val rddLoDate = rddLineOrder.join(rddDateFilter).map {
      case (key, ((supplierKey, customerKey, partKey, profit), year)) =>
        (supplierKey, (customerKey, partKey, profit, year))
    }


    val rddSupplier = sc.textFile(dbDir + "/supplier*").map(line => {
      val columns = line.split("\\|")
      val supplierKey = columns(0).toInt
      val supplierCity = columns(3)
      val supplierNation = columns(4)
      (supplierKey, (supplierNation, supplierCity))
    })
    val rddSupplierFilter = rddSupplier.filter { case (supplierKey, (supplierNation, supplierCity)) =>
      supplierNation == "UNITED STATES"
    }

    val rddLoDateSupplier = rddLoDate.join(rddSupplierFilter).map {
      case (supplyKey, ((customerKey, partKey, profit, year), (supplierNation, supplierCity))) =>
        (customerKey, (partKey, profit, year, supplierCity))
    }

    val rddCustomer = sc.textFile(dbDir + "/customer*").map(line => {
      val columns = line.split("\\|")
      val customerKey = columns(0).toInt
      (customerKey, 0)
    })

    val rddLoDateSupplierCustomer = rddLoDateSupplier.join(rddCustomer).map {
      case (customerKey, ((partKey, profit, year, supplierCity), _)) =>
        (partKey, (year, supplierCity, profit))
    }

    val rddPart = sc.textFile(dbDir + "/part*").map(line => {
      val columns = line.split("\\|")
      val partKey = columns(0).toInt
      val partMfgr = columns(2)
      val partBrand1 = columns(4)
      (partKey, (partBrand1, partMfgr))
    })

    val rddPartFilter = rddPart.filter {
      case (partKey, (partBrand1, partMfgr)) =>
        partMfgr == "MFGR#1" || partMfgr == "MFGR#2"
    }

    val rddLoDateSupplierCustomerPart = rddLoDateSupplierCustomer.join(rddPartFilter).map {
      case (partKey, ((year, supplierNation, profit), (partBrand1, partMfgr))) =>
        ((year, supplierNation, partBrand1), profit)
    }

    val rddAggregate = rddLoDateSupplierCustomerPart.reduceByKey(_ + _)

    val rddSorted = rddAggregate.sortByKey()
    (rddSorted, "hand_opt_ssb_4_3")
  }

  val ssb_4_3 = (sc: SparkContext, dbDir: String) => {
    val rdd001 = sc.textFile(dbDir + "/lineorder*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1).toInt, columns(2).toInt, columns(3).toInt, columns(4).toInt, columns(5), columns(6), columns(7), columns(8).toInt, columns(9).toFloat, columns(10).toFloat, columns(11).toInt, columns(12).toFloat, columns(13).toFloat, columns(14).toInt, columns(15), columns(16))
    })
    val rdd002 = rdd001.map(x => (x._13, x._14, x._4, x._5, x._3, x._6))
    val rdd003 = sc.textFile(dbDir + "/ddate*").map(line => {
      val columns = line.split("\\|")
      (columns(0), columns(1), columns(2), columns(3), columns(4).toInt, columns(5).toInt, columns(6), columns(7).toInt, columns(8).toInt, columns(9).toInt, columns(10).toInt, columns(11).toInt, columns(12), columns(13), columns(14), columns(15), columns(16))
    })
    val rdd004 = rdd003.filter(x => ((x._5 == 1997) || (x._5 == 1998)))
    val rdd005 = rdd004.map(x => (x._5, x._1))
    val rdd006 = rdd002.map(x => (x._6, x))
    val rdd007 = rdd005.map(x => (x._2, x))
    val rdd008 = rdd006.join(rdd007).map(x => x._2)
    val rdd009 = rdd008.map(x => (x._2._1, x._1._1, x._1._2, x._1._3, x._1._4, x._1._5))
    val rdd010 = sc.textFile(dbDir + "/customer*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7))
    })
    val rdd011 = rdd010.map(x => Tuple1(x._1))
    val rdd012 = rdd009.map(x => (x._6, x))
    val rdd013 = rdd011.map(x => (x._1, x))
    val rdd014 = rdd012.join(rdd013).map(x => x._2)
    val rdd015 = rdd014.map(x => (x._1._1, x._1._2, x._1._3, x._1._4, x._1._5))
    val rdd016 = sc.textFile(dbDir + "/supplier*").map(line => {
      val columns = line.split("\\|")
      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6))
    })
    val rdd017 = rdd016.filter(x => (x._5 == "UNITED STATES"))
    val rdd018 = rdd017.map(x => (x._4, x._1))
    val rdd019 = rdd015.map(x => (x._5, x))
    val rdd020 = rdd018.map(x => (x._2, x))
    val rdd021 = rdd019.join(rdd020).map(x => x._2)
    val rdd022 = rdd021.map(x => (x._1._1, x._2._1, x._1._2, x._1._3, x._1._4))
    val rdd023 = sc.textFile(dbDir + "/part*").map(line => {
      val columns = line.split("\\|")

      (columns(0).toInt, columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7).toInt, columns(8))
    })
    val rdd024 = rdd023.filter(x => (x._4 == "MFGR#14"))
    val rdd025 = rdd024.map(x => (x._5, x._1))
    val rdd026 = rdd022.map(x => (x._5, x))
    val rdd027 = rdd025.map(x => (x._2, x))
    val rdd028 = rdd026.join(rdd027).map(x => x._2)
    val rdd029 = rdd028.map(x => (x._1._1, x._1._2, x._2._1, x._1._3, x._1._4))
    val rdd030 = rdd029.groupBy(x => (x._1, x._2, x._3))
    val rdd031 = rdd030.map(x => (x._1._1, x._1._2, x._1._3, x._2.map(x => (x._4 - x._5)).sum))
    (rdd031, "ssb_4_3")
  }
}
