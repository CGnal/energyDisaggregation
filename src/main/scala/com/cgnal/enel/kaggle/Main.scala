package com.cgnal.enel.kaggle

import java.util

import com.cgnal.enel.kaggle.helpers.DatasetHelper
import com.cgnal.enel.kaggle.utils.{ComplexMap, ManageDataset}
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


    val arrayV = ManageDataset.fromCSVtoArrayAddingRowIndex(filenameCSV_V)
    val dfV: DataFrame = DatasetHelper.fromArrayIndexedToDF(sc, sqlContext,
      arrayV, DatasetHelper.Vschema, 1)

    val arrayI = ManageDataset.fromCSVtoArrayAddingRowIndex(filenameCSV_I)
    val dfI: DataFrame = DatasetHelper.fromArrayIndexedToDF(sc, sqlContext,
      arrayI, DatasetHelper.Ischema, 1)

    val arrayTimestamp = ManageDataset.fromCSVtoArrayAddingRowIndex(filenameTimestamp)
    val dfTS: DataFrame = DatasetHelper.fromArrayIndexedToDF(sc, sqlContext,
      arrayTimestamp, DatasetHelper.VItimeSchema, 0)

    val dfVI: DataFrame = dfV.join(dfI, dfV("ID") === dfI("ID")).drop(dfI("ID")).join(dfTS, dfV("ID") === dfTS("ID")).drop(dfTS("ID"))


    val complexConj = ((x: Map[String,Double]) => ComplexMap.conj(x))
    val complexConjUDF = udf(complexConj)
    dfVI.withColumn("ConjFund", complexConjUDF(dfVI("Vfund")))


    val complexProd = ((x: Map[String,Double], y: Map[String,Double]) => ComplexMap.prod(x,y))
    val complexProdUDF = udf(complexProd)
    dfVI.withColumn("PowerFund", complexProdUDF(dfVI("Vfund"), dfVI("Ifund")))





    dfV.select(dfV("Vfund"), dfV("ID")).show()

/*
    dfV.withColumn("prodotto", dfV("fund")*dfV("2ndH"))
*/
//    val frutta = udf((line: Int) => line * 2)
//    val myDf = dfV.withColumn("prodfrutta", frutta("Id"))

 /*   val polcevera = dfV.take(1)(0).getInt(0)

    val frutta = dfV.take(1)(0)
*/



  }


}
