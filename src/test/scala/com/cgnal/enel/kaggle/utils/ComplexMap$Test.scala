package com.cgnal.enel.kaggle.utils

import com.cgnal.enel.kaggle.helpers.DatasetHelper
import org.apache.spark.sql.{UserDefinedFunction, SQLContext}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSuite

/**
  * Created by cavaste on 12/08/16.
  */
class ComplexMap$Test extends FunSuite {

  val conf = new SparkConf().setMaster("local[4]").setAppName("energyDisaggregation")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val filenameCompleMathTest = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/ExampleForCodeTest/testVMath.csv"

  val complexMathTestSchema: StructType =
    StructType(StructField("ID1", MapType(StringType, DoubleType), false) ::
      StructField("ID2", MapType(StringType, DoubleType), false) :: Nil)


  val df = DatasetHelper.fromCSVwithComplexToDF(sc, sqlContext,
    filenameCompleMathTest, complexMathTestSchema)

  df.printSchema()

  test("testProd") {
    val complexProd = ((x: Map[String,Double], y: Map[String,Double]) => ComplexMap.prod(x,y))
    val complexProdUDF = udf(complexProd)
    val df2 = df.withColumn("Prod", complexProdUDF(df("ID1"), df("ID2")))
    assert(df2.select("Prod").take(1)(0).getMap[String,Double](0).get("re").get === -10)
    assert(df2.select("Prod").take(1)(0).getMap[String,Double](0).get("im").get === 11)
    assert(df2.select("Prod").take(2)(1).getMap[String,Double](0).get("re").get === 8)
    assert(df2.select("Prod").take(2)(1).getMap[String,Double](0).get("im").get === -2)

  }

  test("testConj") {
    val complexConj: (Map[String, Double]) => Map[String, Double] = ((x: Map[String,Double]) => ComplexMap.conj(x))
    val complexConjUDF: UserDefinedFunction = udf(complexConj)
    val df2 = df.withColumn("Conj", complexConjUDF(df.col("ID1")))
    assert(df2.select("Conj").take(1)(0).getMap[String,Double](0).get("re").get === 2)
    assert(df2.select("Conj").take(1)(0).getMap[String,Double](0).get("im").get === -3)
    assert(df2.select("Conj").take(2)(1).getMap[String,Double](0).get("re").get === 3)
    assert(df2.select("Conj").take(2)(1).getMap[String,Double](0).get("im").get === 5)
  }

  test("testAbs") {

  }

  test("testSum") {

  }

  test("testSubtraction") {

  }

}
