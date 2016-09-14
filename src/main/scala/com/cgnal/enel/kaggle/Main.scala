package com.cgnal.enel.kaggle

import java.util

import com.cgnal.enel.kaggle.helpers.DatasetHelper
import com.cgnal.enel.kaggle.models.EdgeDetection.EdgeDetection
import com.cgnal.enel.kaggle.utils.{AverageOverComplex, ComplexMap}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import java.io.{FileOutputStream, ObjectOutputStream, FileReader, StringReader}
import org.apache.spark.sql.functions._
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

    val selectedFeature = "RealPowerFund"

    val timestampIntervalPreEdge = 2L
    val timestampIntervalPostEdge = 2L
    val timestepsNumberPreEdge = 12
    val timestepsNumberPostEdge = 12
    val edgeWindowSize = 24

    val filenameDfEdgeWindowsFeature = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/dfEdgeWindowsApplianceProva2.csv"

    // INGESTION AND EDGE PREPROCESSING
    val (dfFeatures, dfEdgeWindowsTaggingInfo) = ingestionAndEdgePreprocessing(selectedFeature,
      timestampIntervalPreEdge, timestampIntervalPostEdge, edgeWindowSize,
      filenameDfEdgeWindowsFeature,
      sc, sqlContext)



    // SINGLE FEATURE SELECTED FEATURE TYPE: DOUBLE --------------------------------------------------------------------

    // COMPUTING EDGE SIGNATURE
    val filenameSampleSubmission = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/SampleSubmission.csv"
    val (dfEdgeSignatures, dfAppliancesToPredict) = EdgeDetection.computeEdgeSignatureAppliances[Double](filenameDfEdgeWindowsFeature,
      edgeWindowSize, selectedFeature, classOf[Double],
      filenameSampleSubmission,
      sc, sqlContext)

    // COMPUTING SIMILARITY with respect to a single appliance
    val applianceID = 30

    val OnSignature: Array[Double] = dfEdgeSignatures.filter(dfEdgeSignatures("ApplianceID") === (applianceID))
      .head.getAs[Array[Double]]("ON_TimeSignature_" + selectedFeature)
    val OffSignature: Array[Double] = dfEdgeSignatures.filter(dfEdgeSignatures("ApplianceID") === (applianceID))
      .head.getAs[Array[Double]]("OFF_TimeSignature_" + selectedFeature)

    val dfRealFeatureEdgeScoreAppliance = EdgeDetection.computeSimilarityEdgeSignaturesRealFeatureGivenAppliance(dfFeatures, selectedFeature,
      OnSignature, OffSignature,
      timestepsNumberPreEdge, timestepsNumberPostEdge,
      sc, sqlContext)

    dfRealFeatureEdgeScoreAppliance.printSchema()


  }










    def ingestionAndEdgePreprocessing(selectedFeature: String,
                                      timestampIntervalPreEdge: Long, timestampIntervalPostEdge: Long, edgeWindowSize: Int,
                                      filenameDfEdgeWindowsFeature: String,
                                      sc: SparkContext, sqlContext: SQLContext): (DataFrame, DataFrame) = {
      // INGESTION DATASET TO DF
      // TEST
      //    val filenameCSV_V = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/ExampleForCodeTest/testV.csv"
      //    val filenameCSV_I = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/ExampleForCodeTest/testI.csv"
      //    val filenameTimestamp = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/ExampleForCodeTest/timestamp.csv"

      val filenameCSV_V = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/LF1V.csv"
      val filenameCSV_I = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/LF1I.csv"
      val filenameTimestamp = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/TimeTicks1.csv"
      val filenameTaggingInfo = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/TaggingInfo.csv"


      val dfVI = DatasetHelper.importingDatasetToDfHouseDay(filenameCSV_V, filenameCSV_I,
        filenameTimestamp, filenameTaggingInfo,
        sc, sqlContext)

      val dfFeatures = DatasetHelper.addPowerToDfFeatures(dfVI)

      dfFeatures.printSchema()

      // EDGE DETECTION ALGORITHM
      // Selecting edge windows for a given Feature
      val dfEdgeWindowsTaggingInfo = EdgeDetection.computeStoreDfEdgeWindowsSingleFeature(dfFeatures,
        filenameTaggingInfo, filenameDfEdgeWindowsFeature,
        selectedFeature, timestampIntervalPreEdge, timestampIntervalPostEdge, edgeWindowSize,
        sc, sqlContext)

      (dfFeatures,dfEdgeWindowsTaggingInfo)

    }







}
