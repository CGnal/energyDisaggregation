package com.cgnal.enel.kaggle

import java.util

import com.cgnal.enel.kaggle.helpers.DatasetHelper
import com.cgnal.enel.kaggle.utils.ComplexMap
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import java.io.{FileReader, StringReader}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.functions.udf

/**
  * Created by cavaste on 08/08/16.
  */


object Main {

  def main(args: Array[String]) = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("energyDisaggregation")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    val filenameCSV_V = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/ExampleForCodeTest/testV.csv"
    val filenameCSV_I = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/ExampleForCodeTest/testI.csv"
    val filenameTimestamp = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/ExampleForCodeTest/timestamp.csv"


    val arrayV: Array[(Array[String], Int)] = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameCSV_V)
    val dfV: DataFrame = DatasetHelper.fromArrayIndexedToDF(sc, sqlContext,
      arrayV, DatasetHelper.Vschema, 1)

    val arrayI = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameCSV_I)
    val dfI: DataFrame = DatasetHelper.fromArrayIndexedToDF(sc, sqlContext,
      arrayI, DatasetHelper.Ischema, 1)

    val arrayTimestamp = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameTimestamp)
    val dfTS: DataFrame = DatasetHelper.fromArrayIndexedToDF(sc, sqlContext,
      arrayTimestamp, DatasetHelper.TSschema, 0)

    // dataframe with Voltage, Current and TimeTicks relative to a given Phase
    val dfVI: DataFrame = dfV.join(dfI, dfV("ID") === dfI("ID")).drop(dfI("ID")).join(dfTS, dfV("ID") === dfTS("ID")).drop(dfTS("ID"))

    // Adding Power = V * conj(I)
    dfVI.withColumn("PowerFund", ComplexMap.complexProdUDF(dfVI("Vfund"), ComplexMap.complexConjUDF(dfVI("Ifund"))))
    dfVI.withColumn("Power1H", ComplexMap.complexProdUDF(dfVI("V1H"), ComplexMap.complexConjUDF(dfVI("I1H"))))
    dfVI.withColumn("Power2H", ComplexMap.complexProdUDF(dfVI("V2H"), ComplexMap.complexConjUDF(dfVI("I2H"))))
    dfVI.withColumn("Power3H", ComplexMap.complexProdUDF(dfVI("V3H"), ComplexMap.complexConjUDF(dfVI("I3H"))))
    dfVI.withColumn("Power4H", ComplexMap.complexProdUDF(dfVI("V4H"), ComplexMap.complexConjUDF(dfVI("I4H"))))
    dfVI.withColumn("Power5H", ComplexMap.complexProdUDF(dfVI("V5H"), ComplexMap.complexConjUDF(dfVI("I5H"))))






    dfV.select(dfV("Vfund"), dfV("ID")).show()

/*
    dfV.withColumn("prodotto", dfV("fund")*dfV("2ndH"))

    val polcevera = dfV.take(1)(0).getInt(0)

    val frutta = dfV.take(1)(0)
*/



  }


}
