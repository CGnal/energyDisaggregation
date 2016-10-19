package com.cgnal.enel.kaggle.utils

import com.cgnal.enel.kaggle.helpers.DatasetHelper
import org.apache.spark.sql.{DataFrame, UserDefinedFunction, SQLContext}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, FunSuite}

import ComplexMap$Test._

/**
  * Created by cavaste on 12/08/16.
  */
class ComplexMap$Test extends FunSuite with BeforeAndAfterAll{

  override protected def beforeAll(): Unit = {
    conf = new SparkConf().setMaster("local[4]").setAppName("energyDisaggregation")
    sc = new SparkContext(conf)
    sqlContext = new SQLContext(sc)
    complexMathTestSchema =
      StructType(StructField("ID1", MapType(StringType, DoubleType), false) ::
        StructField("ID2", MapType(StringType, DoubleType), false) :: Nil)
    df = DatasetHelper.fromCSVwithComplexToDF(sc, sqlContext,
      filenameComplexMathTest, complexMathTestSchema)
    df.printSchema()
  }

  override protected def afterAll(): Unit = {
    sc.stop()
  }

  test("testProd") {

    val df2 = df.withColumn("Prod", ComplexMap.complexProdUDF(df("ID1"), df("ID2")))
    assert(df2.select("Prod").take(1)(0).getMap[String,Double](0).get("re").get === -10)
    assert(df2.select("Prod").take(1)(0).getMap[String,Double](0).get("im").get === 11)
    assert(df2.select("Prod").take(2)(1).getMap[String,Double](0).get("re").get === 8)
    assert(df2.select("Prod").take(2)(1).getMap[String,Double](0).get("im").get === -2)

    df2.printSchema()

  }



  test("testConj") {
    val df2 = df.withColumn("Conj", ComplexMap.complexConjUDF(df.col("ID1")))
    assert(df2.select("Conj").take(1)(0).getMap[String,Double](0).get("re").get === 2)
    assert(df2.select("Conj").take(1)(0).getMap[String,Double](0).get("im").get === -3)
    assert(df2.select("Conj").take(2)(1).getMap[String,Double](0).get("re").get === 3)
    assert(df2.select("Conj").take(2)(1).getMap[String,Double](0).get("im").get === 5)
  }

  test("testProdComplexConj")  {
    val df2 = df.withColumn("Power", ComplexMap.complexProdUDF(df("ID1"), ComplexMap.complexConjUDF(df("ID2"))))
    assert(df2.select("Power").take(1)(0).getMap[String,Double](0).get("re").get === 14)
    assert(df2.select("Power").take(1)(0).getMap[String,Double](0).get("im").get === -5)
    assert(df2.select("Power").take(2)(1).getMap[String,Double](0).get("re").get === -2)
    assert(df2.select("Power").take(2)(1).getMap[String,Double](0).get("im").get === -8)

    df2.printSchema()
  }


  test("testAbs") {
    val df2 = df.withColumn("Abs", ComplexMap.complexAbsUDF(df.col("ID1")))
    assert(df2.select("Abs").take(1)(0).getDouble(0) === Math.sqrt(13d))
    assert(df2.select("Abs").take(2)(1).getDouble(0) === Math.sqrt(34d))

  }

  test("testSum") {

    val df2 = df.withColumn("Sum", ComplexMap.complexSumUDF(df("ID1"), df("ID2")))
    assert(df2.select("Sum").take(1)(0).getMap[String,Double](0).get("re").get === 3)
    assert(df2.select("Sum").take(1)(0).getMap[String,Double](0).get("im").get === 7)
    assert(df2.select("Sum").take(2)(1).getMap[String,Double](0).get("re").get === 4)
    assert(df2.select("Sum").take(2)(1).getMap[String,Double](0).get("im").get === -4)

  }

  test("testSubtraction") {

    val df2 = df.withColumn("Sub", ComplexMap.complexSubUDF(df("ID1"), df("ID2")))
    assert(df2.select("Sub").take(1)(0).getMap[String,Double](0).get("re").get === 1)
    assert(df2.select("Sub").take(1)(0).getMap[String,Double](0).get("im").get === -1)
    assert(df2.select("Sub").take(2)(1).getMap[String,Double](0).get("re").get === 2)
    assert(df2.select("Sub").take(2)(1).getMap[String,Double](0).get("im").get === -6)

  }


}

object ComplexMap$Test {

  var conf :SparkConf = _
  var sc :SparkContext = _
  var sqlContext: SQLContext  = _
  val filenameComplexMathTest = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/ExampleForCodeTest/testVMath.csv"
  var complexMathTestSchema: StructType = _
  var df:DataFrame = _
}










