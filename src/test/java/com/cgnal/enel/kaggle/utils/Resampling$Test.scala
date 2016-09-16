package com.cgnal.enel.kaggle.utils

import com.cgnal.enel.kaggle.helpers.DatasetHelper
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, FunSuite}


import Resampling$Test._
/**
  * Created by cavaste on 16/09/16.
  */
class Resampling$Test extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  override protected def beforeAll(): Unit = {
    conf = new SparkConf().setMaster("local[4]").setAppName("energyDisaggregation")
    sc = new SparkContext(conf)
    sqlContext = new SQLContext(sc)
    df = DatasetHelper.fromCSVwithComplexToDF(sc, sqlContext,
      filenameCSV, DatasetHelper.VIschemaNoID)
    df.printSchema()
    arrayV = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameCSV)
    dfID = DatasetHelper.fromArrayIndexedToDFTimestampOrFeatures(sc, sqlContext,
      arrayV, DatasetHelper.Vschema, 1)

    dfID.printSchema()
  }

  val data = Seq(
    Row(1l, -101d),
    Row(2l, 100d),
    Row(3l, 105d), //
    Row(4l, -100d),
    Row(5l, -90d),
    Row(6l, -120d), //
    Row(7l, 50d), //
    Row(8l, 20d),
    Row(9l, -20d),
    Row(10l, -40d)
  )

  val schema: StructType =
    StructType(
      StructField("Timestamp", LongType, false) ::
        StructField("feature", DoubleType, false) :: Nil)
  dataDF = sqlContext.createDataFrame(sc.makeRDD(data), schema)
  dataDF.printSchema()


  test("check movingAverage"){
    val averagedDF: DataFrame =  Resampling.movingAverageReal(dataDF,
      "feature", 4,
      TimeStamp_ColName = "Timestamp")
    //println(averagedDF.take(1)(2).getDouble(0))
    assert(averagedDF.take(1)(0).get(2) == 1.0)
    // assert(averagedDF.take(3)(2).getInt(0) === 2)
  }
}


object Resampling$Test {
  val filenameCSV = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/ExampleForCodeTest/testV.csv"
  var conf :SparkConf = _
  var sc :SparkContext = _
  var sqlContext : SQLContext= _
  var hiveContext :HiveContext = _
  var df: DataFrame = _
  var arrayV :Array[(Array[String], Int)] = _
  var dfID:DataFrame = _
  var dataDF:DataFrame = _
}