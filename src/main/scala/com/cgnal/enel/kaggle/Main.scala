package com.cgnal.enel.kaggle

import java.util

import com.cgnal.enel.kaggle.helpers.DatasetHelper
import com.cgnal.enel.kaggle.models.edgeDetection.edgeDetection
import com.cgnal.enel.kaggle.utils.{CustomMeanComplex, ComplexMap}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import java.io.{FileOutputStream, ObjectOutputStream, FileReader, StringReader}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import com.databricks.spark.avro._
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by cavaste on 08/08/16.
  */


object Main {

 def main(args: Array[String]): Unit = {
   //println("pippo")
   //mainFastCheck()
   mainToUse(args)
 }

def mainFastCheck(): Unit = {

  val conf  = new SparkConf().setMaster("local[4]").setAppName("energyDisaggregation")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val filenameCSV = "/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/ExampleForCodeTest/testV.csv"
  val dfTest = DatasetHelper.fromCSVwithComplexToDF(sc,sqlContext,filenameCSV, DatasetHelper.VIschemaNoID)
  dfTest.printSchema()
  dfTest.show()
  val TimeStampNumeric_ColumnName: String = "fund"
  val df_S = dfTest.select(TimeStampNumeric_ColumnName).cache()
  dfTest.select(TimeStampNumeric_ColumnName).cache()
  //df_S.orederBy(desc(TimeStampNumeric_ColumnName)).first()
  df_S.printSchema()
  df_S.show()
  println("pippo")
}

  //#########################################################


  def mainToUse(args: Array[String]) = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("energyDisaggregation")
    val sc = new SparkContext(conf)
    //    val sqlContext = new SQLContext(sc)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val sqlContext = new SQLContext(sc)

    computeDfEdgeWindows(sc, sqlContext)

//    computeApplianceSignature(sc, sqlContext,
//      "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/dfEdgeWindowsApplianceProva.csv",
//      "PowerFund")

  }

    def computeDfEdgeWindows(sc: SparkContext, sqlContext: SQLContext) = {

      // TEST
      val filenameCSV_V = "/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/ExampleForCodeTest/testV.csv"
      val filenameCSV_I = "/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/ExampleForCodeTest/testI.csv"
      val filenameTimestamp = "/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/ExampleForCodeTest/timestamp.csv"

      //val filenameCSV_V="/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/LF1V.csv"
      //val filenameCSV_I="/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/LF1I.csv"
      //val filenameTimestamp="/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/TimeTicks1.csv"
      // val filenameTaggingInfo="/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/TaggingInfo.csv"


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

      // Adding Power = V * conj(I)
      val dfVIfinal = dfVI.withColumn("PowerFund", ComplexMap.complexProdUDF(dfV("Vfund"), ComplexMap.complexConjUDF(dfI("Ifund"))))
        .withColumn("Power1H", ComplexMap.complexProdUDF(dfVI("V1H"), ComplexMap.complexConjUDF(dfVI("I1H"))))
        .withColumn("Power2H", ComplexMap.complexProdUDF(dfVI("V2H"), ComplexMap.complexConjUDF(dfVI("I2H"))))
        .withColumn("Power3H", ComplexMap.complexProdUDF(dfVI("V3H"), ComplexMap.complexConjUDF(dfVI("I3H"))))
        .withColumn("Power4H", ComplexMap.complexProdUDF(dfVI("V4H"), ComplexMap.complexConjUDF(dfVI("I4H"))))
        .withColumn("Power5H", ComplexMap.complexProdUDF(dfVI("V5H"), ComplexMap.complexConjUDF(dfVI("I5H"))))

      dfVIfinal.printSchema()
      dfVIfinal.cache()
      dfVIfinal.show()

      val arrayTaggingInfo = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameTaggingInfo)
      val dfTaggingInfo: DataFrame = DatasetHelper.fromArrayIndexedToDFTaggingInfo(sc, sqlContext,
        arrayTaggingInfo, DatasetHelper.TagSchema)

      val dfEdgeWindows = edgeDetection.selectingEdgeWindowsSingleFeatureTimeInterval(dfVIfinal, dfTaggingInfo,
        "PowerFund", 1L, 1L, 12, sc, sqlContext)

      val dfEdgeWindowsAppliance: DataFrame = dfEdgeWindows.join(dfTaggingInfo, dfEdgeWindows("IDedge") === dfTaggingInfo("IDedge"))
        .drop(dfTaggingInfo("IDedge"))

      dfEdgeWindowsAppliance.printSchema()

      dfEdgeWindowsAppliance.write
        .avro("/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/dfEdgeWindowsApplianceProva.csv")

    }


  /*def movingAverage(df: DataFrame,
                    columnName_harmonics: String = "PowerFund",
                    slidingWindow: Int = 6,
                    TimeStampNumeric_ColumnName: String =  "Timestamp"): DataFrame = {

    val df_S =
      df
        // .filter(df_sensorData("DateTime").endsWith("00:00:00")) //take only entire days
        .select(columnName_harmonics)
        .dropDuplicates(Seq(columnName_harmonics))
        //.orderBy(desc(TimeStampNumeric_ColumnName))
        .cache()

    val nRows = df_S.count().toInt
    val lastWindowIndex = (nRows - slidingWindow)
    //why orderby does not work ?
    //  val firstdfSrow = df_S.orderBy(desc(TimeStampNumeric_ColumnName)).first()
    //  val maxValue = firstdfSrow.get(firstdfSrow.fieldIndex(TimeStampNumeric_ColumnName)).toString.toLong

    val maxAllowedTimeStamp = df_S.take(nRows)(0)  // alloz
    val maxAllowedValue = df_S.take(lastWindowIndex)(0)


    val allowedTimeStamps: Array[Row] = df_S
      .filter(df_S(TimeStampNumeric_ColumnName) < maxAllowedValue) //filter data for which there is no enough historical information
      .collect()
    //.take(1)

    var i = 0
    val res = allowedTimeStamps
      .map(
        row => {
          //generate features
          println("---- " + i + "/" + allowedTimeStamps.length + " ----")
          i = i + 1
          // Pick up the n vlaues from the field columnName_harmonics and do the average
          //val timeStampStart = row.getLong(row.fieldIndex(TimeStampNumeric_ColumnName))
          //val timeStampEnd = timeStampStart + slidingWindow
          //val average = df_S.map()


        }
      )

  }
*/


  def computeApplianceSignature(sc: SparkContext, sqlContext: SQLContext,
                                dfEdgeWindowsFilename: String, selectedFeature: String) = {

    val filenameSampleSubmission = "/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/dataset/SampleSubmission.csv"

    val dfSampleSubmission = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(filenameSampleSubmission)


    val dfEdgeWindowsAppliance = sqlContext.read.avro(dfEdgeWindowsFilename)

    // define UDAF
    val customMeanComplex = new CustomMeanComplex("ON_TimeWindow_" + selectedFeature, 12)

    val dfAverageWindowEdge: DataFrame = dfEdgeWindowsAppliance.groupBy("ApplianceID").agg(
      customMeanComplex(dfEdgeWindowsAppliance.col("ON_TimeWindow_" + selectedFeature)).as("custom_mean")
    )



  }



}
