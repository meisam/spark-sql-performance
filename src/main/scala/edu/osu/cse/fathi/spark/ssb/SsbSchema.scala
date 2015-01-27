package edu.osu.cse.fathi.spark.ssb


object SsbConversionFunctions {

  def toPart(s: Array[String]): Part = { 
    assert(s.length == 9, f"s.length = ${s.length}%d, s=${s}%s")
    new Part(s(0).toInt, s(1), s(2), s(3), s(4), s(5), s(6), s(7).toInt, s(8))
  }

  def toSuplier(s: Array[String]): Supplier = { 
    assert(s.length == 7, f"s.length = ${s.length}%d, s=${s}%s")
    new Supplier(s(0).toInt, s(1), s(2), s(3), s(4), s(5), s(6))
  }

  def toCustomer(s: Array[String]): Customer = { 
    assert(s.length == 8, f"s.length = ${s.length}%d, s=${s}%s")
    new Customer(s(0).toInt, s(1), s(2), s(3), s(4), s(5), s(6), s(7))
  }

  def toLineOrder(s: Array[String]): LineOrder = { 
    assert(s.length == 17, f"s.length = ${s.length}%d, s=${s}%s")
    new LineOrder(s(0).toInt, s(1).toInt, s(2).toInt, s(3).toInt, s(4).toInt, s(5).toInt, s(6), s(7).toInt,
      s(8).toInt, s(9).toInt, s(10).toInt, s(11).toInt, s(12).toInt, s(13).toInt, s(14).toInt, s(15).toInt, s(16))
  }

  def toDDate(s: Array[String]): DDate = { 
    assert(s.length == 17, f"s.length = ${s.length}%d, s=${s}%s")
    new DDate(s(0).toInt, s(1), s(2), s(3), s(4).toInt, s(5).toInt, s(6), s(7).toInt,
      s(8).toInt, s(9).toInt, s(10).toInt, s(11).toInt, s(12), s(13), s(14), s(15), s(16))
  }
}

case class Part(// CONVERTED TO PARQUET
                p_partkey: Int,
                p_name: String,
                p_mfgr: String,
                p_category: String,
                p_brand1: String,
                p_color: String,
                p_type: String,
                p_size: Int,
                p_container: String)

case class Supplier(// CONVERTED TO PARQUET
                    s_suppkey: Int,
                    s_name: String,
                    s_address: String,
                    s_city: String,
                    s_nation: String,
                    s_region: String,
                    s_phone: String)

case class Customer(// CONVERTED TO PARQUET
                    c_custkey: Int,
                    c_name: String,
                    c_address: String,
                    c_city: String,
                    c_nation: String,
                    c_region: String,
                    c_phone: String,
                    c_mktsegment: String)


case class LineOrder(// CONVERTED TO PARAQUET
                     lo_orderkey: Int,
                     lo_linenumber: Int,
                     lo_custkey: Int,
                     lo_partkey: Int,
                     lo_suppkey: Int,
                     lo_orderdate: Int,
                     lo_orderpriority: String,
                     lo_shippriority: Int, // actually string in the SSB schema
                     lo_quantity: Int,
                     lo_extendedprice: Int, //actually float in the SSB schema
                     lo_ordtotalprice: Int, //actually float in the SSB schema
                     lo_discount: Int, //actually float in the SSB schema
                     lo_revenue: Int, //actually float in the SSB schema
                     lo_supplycost: Int,
                     lo_tax: Int,
                     l_commitdate: Int,
                     l_shipmode: String)

case class DDate(// CONVERTED TO PARAQUET
                 d_datekey: Int,
                 d_date: String,
                 d_dayofweek: String,
                 d_month: String,
                 d_year: Int,
                 d_yearmonthnum: Int,
                 d_yearmonth: String,
                 d_daynuminweek: Int,
                 d_daynuminmonth: Int,
                 d_daynuminyear: Int,
                 d_monthnuminyear: Int,
                 d_weeknuminyear: Int,
                 d_sellingseason: String,
                 d_lastdayinweekfl: String,
                 d_lastdayinmonthfl: String,
                 d_holidayfl: String,
                 d_weekdayfl: String)