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
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{avg, max, min, sum}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by cavaste on 08/08/16.
  */


object Main {

 def main(args: Array[String]): Unit = {
   //println("pippo")
   mainFastCheck()
   //mainToUse(args)
 }

def mainFastCheck(): Unit = {

  val conf  = new SparkConf().setMaster("local[4]").setAppName("energyDisaggregation")
  val sc = new SparkContext(conf)
  //val sqlContext = new SQLContext(sc)
  val sqlContext = new HiveContext(sc)
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


  val data = Seq(
    Row(1l,	-101d),
    Row(2l,	100d),
    Row(3l,	105d),  //
    Row(4l,	-100d),
    Row(5l,	-90d),
    Row(6l,	-120d),//
    Row(7l,	50d),//
    Row(8l,	20d),
    Row(9l,	-20d),
    Row(10l,	-40d)
  )

  val schema: StructType =
    StructType(
      StructField("Timestamp", LongType, false) ::
        StructField("feature", DoubleType, false) :: Nil)
  val dataDF: DataFrame = sqlContext.createDataFrame(sc.makeRDD(data), schema)
  dataDF.printSchema()

  val averagedDF: DataFrame =  DatasetHelper.movingAverage(dataDF,harmonics_ColName = "feature",slidingWindow =4)
  //println(averagedDF.take(1)(2).getDouble(0))
  averagedDF.show()
  println("blablabl4: " + averagedDF.take(1)(0).get(2))//.getDouble(0)
}

  //#########################################################


  def mainToUse(args: Array[String]) = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("energyDisaggregation")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
//    val sqlContext = new SQLContext(sc)
    val sqlContext = new HiveContext(sc)
    computeDfEdgeWindows(sc, sqlContext)

//    computeApplianceSignature(sc, sqlContext,
//      "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/dfEdgeWindowsApplianceProva.csv",
//      "PowerFund")

  }

//    def computeDfEdgeWindows(sc: SparkContext, sqlContext: SQLContext) = {
      def computeDfEdgeWindows(sc: SparkContext, sqlContext: HiveContext) = {

      // TEST
      val filenameCSV_V = "/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/ExampleForCodeTest/testV.csv"
      val filenameCSV_I = "/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/ExampleForCodeTest/testI.csv"
      val filenameTimestamp = "/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/ExampleForCodeTest/timestamp.csv"

      //val filenameCSV_V="/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/LF1V.csv"
      //val filenameCSV_I="/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/LF1I.csv"
      //val filenameTimestamp="/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/TimeTicks1.csv"
      val filenameTaggingInfo="/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/TaggingInfo.csv"


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

    //  dfEdgeWindowsAppliance.write
      //  .avro("/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/dfEdgeWindowsApplianceProva.csv")



        val averagedDF =  DatasetHelper.movingAverage(dfVIfinal,harmonics_ColName = "Timestamp",slidingWindow = 3)
      //prova.select("Timestamp","Timestamp_localAvg").show()
        averagedDF.show()

    }

def downSampling(df: DataFrame, scalingFactor: Int = 6){}

/*
  def computeApplianceSignature(sc: SparkContext, sqlContext: HiveContext,
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

       customMeanComplex(dfEdgeWindowsAppliance.col("ON_TimeWindow_" + selectedFeature)).as("custom_mean")
    )



  }
*/



}
