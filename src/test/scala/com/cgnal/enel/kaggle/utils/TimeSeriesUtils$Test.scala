package com.cgnal.enel.kaggle.utils

import com.cgnal.efm.predmain.uta.timeseries.TimeSeriesUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, FunSuite}

import TimeSeriesUtils$Test._
/**
  * Created by cavaste on 19/09/16.
  */
class TimeSeriesUtils$Test extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  override protected def beforeAll(): Unit = {
    conf = new SparkConf().setMaster("local[4]").setAppName("energyDisaggregation")
    sc = new SparkContext(conf)
    sqlContext = new HiveContext(sc)

    val data = Seq(
      Row(1343372406289529l,	-101d),
      Row(2343372406289526l,	100d),
      Row(3343372406289526l,	105d),  //
      Row(4343372406289526l,	-100d),
      Row(5343372406289526l,	-90d),
      Row(6343372406289526l,	-120d),//
      Row(7343372406289526l,	50d),//
      Row(8343372406289526l,	20d),
      Row(9343372406289526l,	-20d),
      Row(10433724062895269l,	-40d),
      Row(11433724062895269l,	-50d), //
      Row(12433724062895269l,	-30d),
      Row(13433724062895269l,	-30d),
      Row(14433724062895269l,	150d),//
      Row(15433724062895269l,	150d),
      Row(16433724062895269l,	100d),
      Row(17433724062895269l,	102d),
      Row(18433724062895269l,	102d),
      Row(19433724062895269l,	90d),
      Row(20433724062895269l,	-70d),//
      Row(21433724062895269l,	-70d),
      Row(22433724062895269l,	-70d),
      Row(23433724062895269l,	-60d),
      Row(24433724062895269l,	50d),//
      Row(25433724062895269l,	20d),
      Row(26433724062895269l,	-20d),
      Row(27433724062895269l,	-30d),//
      Row(28433724062895269l,	-30d) ,
      Row(29433724062895269l,	10d),//
      Row(30433724062895269l,	10d),
      Row(31433724062895269l,	-30d)//
    )

    val groundTruth = Seq(
      Row(1343372406289529l,	0),
      Row(2343372406289526l,	0),
      Row(3343372406289526l,	1),  //
      Row(4343372406289526l, 1),
      Row(5343372406289526l,	1),
      Row(6343372406289526l,	1),//
      Row(7343372406289526l,	1),//
      Row(8343372406289526l,	1),
      Row(9343372406289526l,	1),
      Row(10433724062895269l, 1),
      Row(11433724062895269l, 1), //
      Row(12433724062895269l, 1),
      Row(13433724062895269l, 1),
      Row(14433724062895269l, 1),//
      Row(15433724062895269l,	1),
      Row(16433724062895269l,	1),
      Row(17433724062895269l,	1),
      Row(18433724062895269l,	1),
      Row(19433724062895269l,	1),
      Row(20433724062895269l,	1),//
      Row(21433724062895269l,	0),
      Row(22433724062895269l,	0),
      Row(23433724062895269l,	0),
      Row(24433724062895269l,  1),//
      Row(25433724062895269l,	1),
      Row(26433724062895269l,	1),
      Row(27433724062895269l,	1),//
      Row(28433724062895269l,	0) ,
      Row(29433724062895269l,	0),
      Row(30433724062895269l,	0),
      Row(31433724062895269l,	0)
    )

    val schema: StructType =
      StructType(
        StructField("timeStamp", LongType, false) ::
          StructField("feature", DoubleType, false) :: Nil)

    val schemaGT: StructType =
      StructType(
        StructField("timeStamp", LongType, false) ::
          StructField("isOn", IntegerType, false) :: Nil)

    df = sqlContext.createDataFrame(sc.makeRDD(data), schema)
    dfGT = sqlContext.createDataFrame(sc.makeRDD(groundTruth), schemaGT)
  }

  override protected def afterAll(): Unit = {
    sc.stop()
  }



  test("Hamming Loss") {

    val res = TimeSeriesUtils.evaluateHammingLoss(
      df,
      dfGT, "isOn", "feature", "timeStamp", 1, 15, "prova")


    assert(res === 2d/31)
  }
}

object TimeSeriesUtils$Test {

  val filenameCSV = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/ExampleForCodeTest/testV.csv"
  var conf :SparkConf = _
  var sc :SparkContext = _
  var sqlContext : HiveContext= _
  var df: DataFrame = _
  var dfGT: DataFrame = _
  var arrayV :Array[(Array[String], Int)] = _
  var dfID:DataFrame = _

}