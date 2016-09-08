package com.cgnal.enel.kaggle

import java.util

import com.cgnal.enel.kaggle.helpers.DatasetHelper
import com.cgnal.enel.kaggle.models.edgeDetection.edgeDetection
import com.cgnal.enel.kaggle.utils.{CustomMeanComplex, ComplexMap}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import java.io.{FileReader, StringReader}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by cavaste on 08/08/16.
  */


object Main {

  def main(args: Array[String]) = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("energyDisaggregation")
    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val sqlContext = new HiveContext(sc)

    // TEST
//    val filenameCSV_V = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/ExampleForCodeTest/testV.csv"
//    val filenameCSV_I = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/ExampleForCodeTest/testI.csv"
//    val filenameTimestamp = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/ExampleForCodeTest/timestamp.csv"


    val filenameCSV_V = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/LF1V.csv"
    val filenameCSV_I = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/LF1I.csv"
    val filenameTimestamp = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/TimeTicks1.csv"
    val filenameTaggingInfo = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/TaggingInfo.csv"



    val arrayV: Array[(Array[String], Int)] = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameCSV_V)
    val dfV: DataFrame = DatasetHelper.fromArrayIndexedToDFTimestampOrFeatures(sc, sqlContext,
      arrayV, DatasetHelper.Vschema, 1)

    val arrayI = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameCSV_I)
    val dfI: DataFrame = DatasetHelper.fromArrayIndexedToDFTimestampOrFeatures(sc, sqlContext,
      arrayI, DatasetHelper.Ischema, 1)

    val arrayTimestamp = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameTimestamp)
    val dfTS: DataFrame = DatasetHelper.fromArrayIndexedToDFTimestampOrFeatures(sc, sqlContext,
      arrayTimestamp, DatasetHelper.TSschema, 0)

    dfTS.show(5)

    // dataframe with Voltage, Current and TimeTicks relative to a given Phase
    val dfVI: DataFrame = dfV.join(dfI, dfV("IDtime") === dfI("IDtime")).drop(dfI("IDtime"))
      .join(dfTS, dfV("IDtime") === dfTS("IDtime")).drop(dfTS("IDtime"))

//    dfVI.printSchema()

    // Adding Power = V * conj(I)
    val dfVIfinal = dfVI.withColumn("PowerFund", ComplexMap.complexProdUDF(dfV("Vfund"), ComplexMap.complexConjUDF(dfI("Ifund"))))
    .withColumn("Power1H", ComplexMap.complexProdUDF(dfVI("V1H"), ComplexMap.complexConjUDF(dfVI("I1H"))))
    .withColumn("Power2H", ComplexMap.complexProdUDF(dfVI("V2H"), ComplexMap.complexConjUDF(dfVI("I2H"))))
    .withColumn("Power3H", ComplexMap.complexProdUDF(dfVI("V3H"), ComplexMap.complexConjUDF(dfVI("I3H"))))
    .withColumn("Power4H", ComplexMap.complexProdUDF(dfVI("V4H"), ComplexMap.complexConjUDF(dfVI("I4H"))))
    .withColumn("Power5H", ComplexMap.complexProdUDF(dfVI("V5H"), ComplexMap.complexConjUDF(dfVI("I5H"))))

//    dfVIfinal.show(5)
//    dfVIfinal.printSchema()

    val arrayTaggingInfo = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameTaggingInfo)
    val dfTaggingInfo: DataFrame = DatasetHelper.fromArrayIndexedToDFTaggingInfo(sc, sqlContext,
      arrayTaggingInfo, DatasetHelper.TagSchema)

//    dfTaggingInfo.printSchema()

    val dfEdgeWindows = edgeDetection.selectingEdgeWindowsSingleFeatureTimeInterval(dfVIfinal, dfTaggingInfo,
      "PowerFund", 1L, 1L, 12, sc, sqlContext)

    dfEdgeWindows.printSchema()

    val dfEdgeWindowsAppliance: DataFrame = dfEdgeWindows.join(dfTaggingInfo, dfEdgeWindows("IDedge") === dfTaggingInfo("IDedge"))
      .drop(dfTaggingInfo("IDedge"))




    // define UDAF
    val customMeanComplex = new CustomMeanComplex(12)

    // Calculate average value for each group
 /*   df.groupBy("key").agg(
      customMean(df.col("value")).as("custom_mean"),
      avg("value").as("avg")
    ).show()
*/

    dfEdgeWindowsAppliance.groupBy("applianceID").agg(
      customMeanComplex(dfEdgeWindowsAppliance.col("ON_TimeWindow_PowerFund")).as("custom_mean")
    ).show()


  }


}
