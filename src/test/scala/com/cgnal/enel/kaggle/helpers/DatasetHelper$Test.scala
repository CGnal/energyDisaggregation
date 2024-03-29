package com.cgnal.enel.kaggle.helpers

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.StructType
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, FunSuite}

import DatasetHelper$Test._

/**
  * Created by cavaste on 11/08/16.
  */
class DatasetHelper$Test extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach{

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

  override protected def afterAll(): Unit = {
    sc.stop()
  }

  test("counting the rows") {
    assert(df.count() === 5.toLong)
  }

  test("check 1st row 1st element") {
    assert(df.select("fund").take(1)(0).getMap[String,Double](0).get("re").get === -75.49444580078125)
  }

  test("check 1st row 3rd element") {
    assert(df.select("2H").take(1)(0).getMap[String,Double](0).get("re").get === 0.0097901532426476479)
    assert(df.select("2H").take(1)(0).getMap[String,Double](0).get("im").get === +0.37801700830459595)
  }

  test("check 2nd row 1st element") {
    assert(df.select("fund").take(2)(1).getMap[String,Double](0).get("re").get === +84.738815307617188)
    assert(df.take(2)(1).getMap[String,Double](0).get("re").get === +84.738815307617188)
  }

  test("check 5th row 3rd element") {
    assert(df.select("2H").take(5)(4).getMap[String,Double](0).get("re").get === 0.36591875553131104)
    assert(df.take(5)(4).getMap[String,Double](2).get("im").get === +0.12984281778335571)
  }


  test("check elements with ID") {
    assert(dfID.select("Vfund").take(1)(0).getMap[String,Double](0).get("re").get === -75.49444580078125)
    assert(dfID.select("V2H").take(1)(0).getMap[String,Double](0).get("re").get === 0.0097901532426476479)
    assert(dfID.select("V2H").take(1)(0).getMap[String,Double](0).get("im").get === +0.37801700830459595)
    assert(dfID.select("Vfund").take(2)(1).getMap[String,Double](0).get("re").get === +84.738815307617188)
    assert(dfID.take(2)(1).getAs[Map[String,Double]](1).get("re").get === +84.738815307617188)
    assert(dfID.select("V2H").take(5)(4).getMap[String,Double](0).get("re").get === 0.36591875553131104)
    assert(dfID.take(5)(4).getMap[String,Double](3).get("im").get === +0.12984281778335571)
  }

  test("check ID label"){
    assert(dfID.take(1)(0).getInt(0) === 0)
    assert(dfID.take(2)(1).getInt(0) === 1)
    assert(dfID.take(3)(2).getInt(0) === 2)
    assert(dfID.take(4)(3).getInt(0) === 3)
    assert(dfID.take(5)(4).getInt(0) === 4)
  }


}


object DatasetHelper$Test {

  val filenameCSV = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/ExampleForCodeTest/testV.csv"
  var conf :SparkConf = _
  var sc :SparkContext = _
  var sqlContext : SQLContext= _
  var df: DataFrame = _
  var arrayV :Array[(Array[String], Int)] = _
  var dfID:DataFrame = _

}