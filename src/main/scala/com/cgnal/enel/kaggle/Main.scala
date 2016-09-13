package com.cgnal.enel.kaggle

import java.util

import com.cgnal.enel.kaggle.helpers.DatasetHelper
import com.cgnal.enel.kaggle.models.edgeDetection.edgeDetection
import com.cgnal.enel.kaggle.utils.{AverageOverComplex, ComplexMap}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import java.io.{FileOutputStream, ObjectOutputStream, FileReader, StringReader}
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import com.databricks.spark.avro._
import org.apache.spark.sql.hive.HiveContext

import scala.reflect.ClassTag

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
    val sqlContext = new SQLContext(sc)


//    computeStoreDfEdgeWindows(sc, sqlContext)

    computeEdgeSignatureAppliances[Map[String,Double]](sc, sqlContext,
      "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/dfEdgeWindowsApplianceProva.csv",
      "PowerFund", classOf[Map[String,Double]])
  }

    def computeStoreDfEdgeWindows(selectedFeature: String,
                                  timestampIntervalPreEdge: Long, timestampIntervalPostEdge: Long, edgeWindowSize: Int,
                                  sc: SparkContext, sqlContext: SQLContext) = {

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
      val dfVI: DataFrame = dfV.join(dfI, "IDtime")
        .join(dfTS, "IDtime")

      dfVI.printSchema()

      // Adding Power = V * conj(I)
      val dfVIfinal = dfVI.withColumn("PowerFund", ComplexMap.complexProdUDF(dfV("Vfund"), ComplexMap.complexConjUDF(dfI("Ifund"))))
        .withColumn("Power1H", ComplexMap.complexProdUDF(dfVI("V1H"), ComplexMap.complexConjUDF(dfVI("I1H"))))
        .withColumn("Power2H", ComplexMap.complexProdUDF(dfVI("V2H"), ComplexMap.complexConjUDF(dfVI("I2H"))))
        .withColumn("Power3H", ComplexMap.complexProdUDF(dfVI("V3H"), ComplexMap.complexConjUDF(dfVI("I3H"))))
        .withColumn("Power4H", ComplexMap.complexProdUDF(dfVI("V4H"), ComplexMap.complexConjUDF(dfVI("I4H"))))
        .withColumn("Power5H", ComplexMap.complexProdUDF(dfVI("V5H"), ComplexMap.complexConjUDF(dfVI("I5H"))))

      val arrayTaggingInfo = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameTaggingInfo)
      val dfTaggingInfo: DataFrame = DatasetHelper.fromArrayIndexedToDFTaggingInfo(sc, sqlContext,
        arrayTaggingInfo, DatasetHelper.TagSchema)

      val dfEdgeWindows = edgeDetection.selectingEdgeWindowsFromTagWithTimeIntervalSingleFeature[Map[String,Double]](dfVIfinal, dfTaggingInfo,
        selectedFeature, timestampIntervalPreEdge, timestampIntervalPostEdge, edgeWindowSize,
        sc, sqlContext)

      val dfEdgeWindowsTaggingInfo: DataFrame = dfEdgeWindows.join(dfTaggingInfo, "IDedge")

      dfEdgeWindowsTaggingInfo.printSchema()

      dfEdgeWindowsTaggingInfo.write
        .avro("/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/dfEdgeWindowsApplianceProva.csv")
    }


  def computeEdgeSignatureAppliances[SelFeatureType:ClassTag](sc: SparkContext, sqlContext: SQLContext,
                                                              dfEdgeWindowsFilename: String, edgeWindowSize: Int,
                                                              selectedFeature: String, selectedFeatureType: Class[SelFeatureType]) = {

    val filenameSampleSubmission = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/SampleSubmission.csv"

    val dfSampleSubmission = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(filenameSampleSubmission)

    val dfAppliancesToPredict = dfSampleSubmission.select("Appliance").distinct()

    val dfEdgeWindowsTaggingInfo = sqlContext.read.avro(dfEdgeWindowsFilename)

    val dfEdgeSignaturesAll =
    if (selectedFeatureType.isAssignableFrom(classOf[Map[String,Double]])) {
      // define UDAF
      val averageOverComplexON = new AverageOverComplex("ON_TimeWindow_" + selectedFeature, edgeWindowSize)
      val averageOverComplexOFF = new AverageOverComplex("OFF_TimeWindow_" + selectedFeature, edgeWindowSize)


      val dfEdgeSignatures: DataFrame = dfEdgeWindowsTaggingInfo.groupBy("ApplianceID").agg(
        averageOverComplexON(dfEdgeWindowsTaggingInfo.col("ON_TimeWindow_" + selectedFeature)).as("ON_TimeSignature_" + selectedFeature),
        averageOverComplexOFF(dfEdgeWindowsTaggingInfo.col("OFF_TimeWindow_" + selectedFeature)).as("OFF_TimeSignature_" + selectedFeature))

      dfEdgeSignatures.printSchema()
      dfEdgeSignatures
    }
    else {

      val dfEdgeSignatures: DataFrame = dfEdgeWindowsTaggingInfo.groupBy("ApplianceID").agg(
        avg("ON_TimeWindow_" + selectedFeature).as("ON_TimeSignature_" + selectedFeature),
        avg("OFF_TimeWindow_" + selectedFeature).as("OFF_TimeSignature_" + selectedFeature))

      dfEdgeSignatures.printSchema()
      dfEdgeSignatures
    }

    val dfEdgeSignatures = dfEdgeSignaturesAll.join(dfAppliancesToPredict, dfEdgeSignaturesAll("ApplianceID") === dfAppliancesToPredict("Appliance"))
      .drop(dfAppliancesToPredict("Appliance"))

    dfEdgeSignatures

  }


}
