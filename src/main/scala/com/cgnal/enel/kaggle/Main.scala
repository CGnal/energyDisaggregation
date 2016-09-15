package com.cgnal.enel.kaggle

import java.util

import com.cgnal.efm.predmain.uta.timeseries.TimeSeriesUtils
import com.cgnal.enel.kaggle.helpers.DatasetHelper
import com.cgnal.enel.kaggle.models.EdgeDetection.EdgeDetection
import com.cgnal.enel.kaggle.utils.{Resampling, ProvaUDAF, AverageOverComplex, ComplexMap}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import java.io.{FileOutputStream, ObjectOutputStream, FileReader, StringReader}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import com.databricks.spark.avro._
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

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

    val selectedFeature = "RealPowerFund"
    val partitionNumber = 4

    val averageSmoothingWindowSize = 12 // number of timestamps, unit: [167ms]
    val downsamplingBinSize = 6 // number of timestamps, unit: [167ms]

    val downsamplingBinPredictionSize = 60

    val timestampIntervalPreEdge = 4L
    val timestampIntervalPostEdge = 8L
    val timestepsNumberPreEdge = 4
    val timestepsNumberPostEdge = 7
    val edgeWindowSize = timestepsNumberPreEdge + timestepsNumberPostEdge + 1

    type SelFeatureType = Double

    val filenameDfEdgeWindowsFeature = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/dfEdgeWindowsApplianceProva3.csv"


    // INGESTION (from csv to DataFrame)
    val filenameCSV_V = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/LF1V.csv"
    val filenameCSV_I = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/LF1I.csv"
    val filenameTimestamp = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/TimeTicks1.csv"
    val filenameTaggingInfo = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/TaggingInfo.csv"

    val dfVI = DatasetHelper.importingDatasetToDfHouseDay(filenameCSV_V, filenameCSV_I,
      filenameTimestamp, filenameTaggingInfo,
      sc, sqlContext)

    val dfFeatures = DatasetHelper.addPowerToDfFeatures(dfVI)
    dfFeatures.printSchema()


    // 1 RESAMPLING
    val dfFeatureSmoothed = Resampling.movingAverageReal(dfFeatures, selectedFeature, averageSmoothingWindowSize)
    val dfFeatureResampled = Resampling.downsampling(dfFeatureSmoothed, downsamplingBinSize)


    // 2 EDGE DETECTION ALGORITHM
    // Selecting edge windows for a given Feature
    val (dfEdgeWindowsTaggingInfo, dfTaggingInfo) = EdgeDetection.computeStoreDfEdgeWindowsSingleFeature[SelFeatureType](dfFeatureResampled,
      filenameTaggingInfo, filenameDfEdgeWindowsFeature,
      selectedFeature, timestampIntervalPreEdge, timestampIntervalPostEdge, edgeWindowSize,
      sc, sqlContext)


    // SINGLE FEATURE SELECTED FEATURE TYPE: DOUBLE --------------------------------------------------------------------

    // 3 COMPUTING EDGE SIGNATURE
    val filenameSampleSubmission = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/SampleSubmission.csv"
    val (dfEdgeSignatures, dfAppliancesToPredict) = EdgeDetection.computeEdgeSignatureAppliances[SelFeatureType](filenameDfEdgeWindowsFeature,
      edgeWindowSize, selectedFeature, classOf[SelFeatureType],
      filenameSampleSubmission,
      sc, sqlContext)

    // 4 COMPUTING SIMILARITY with respect to a single appliance
    val applianceID = 30

    val OnSignature: Array[SelFeatureType] = dfEdgeSignatures.filter(dfEdgeSignatures("ApplianceID") === (applianceID))
      .head.getAs[mutable.WrappedArray[SelFeatureType]]("ON_TimeSignature_" + selectedFeature).toArray[SelFeatureType]

    val OffSignature: Array[SelFeatureType] = dfEdgeSignatures.filter(dfEdgeSignatures("ApplianceID") === (applianceID))
      .head.getAs[mutable.WrappedArray[SelFeatureType]]("OFF_TimeSignature_" + selectedFeature).toArray[SelFeatureType]

    val dfRealFeatureEdgeScoreAppliance = EdgeDetection.computeSimilarityEdgeSignaturesRealFeatureGivenAppliance(dfFeatureResampled,
      selectedFeature,
      OnSignature, OffSignature,
      timestepsNumberPreEdge, timestepsNumberPostEdge, partitionNumber,
      sc, sqlContext)

    dfRealFeatureEdgeScoreAppliance.printSchema()


    // 5 RESAMPLING SIMILARITY SCORES
    val dfRealFeatureEdgeScoreApplianceDS = Resampling.edgeScoreDownsampling(dfRealFeatureEdgeScoreAppliance,
      selectedFeature, downsamplingBinPredictionSize)


    // 6
    val dfGroundTruth: DataFrame = TimeSeriesUtils.addOnOffRangesToDF(dfRealFeatureEdgeScoreApplianceDS, "TimestampPrediction",
      dfTaggingInfo, applianceID, "applianceID", "ON_Time", "OFF_Time", "GroundTruth")


    val HLoverThreshold = TimeSeriesUtils.hammingLossCurve(dfRealFeatureEdgeScoreApplianceDS,
      dfGroundTruth, "GroundTruth", "DeltaScorePrediction_" + selectedFeature,
      "TimestampPrediction", 1)

    print("ciaoone intu cuulo")


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
      Row(10l,	-40d),
      Row(11l,	-50d), //
      Row(12l,	30d),
      Row(13l,	-30d),
      Row(14l,	150d),//
      Row(15l,	150d),
      Row(16l,	100d),
      Row(17l,	102d),
      Row(18l,	102d),
      Row(19l,	90d),
      Row(20l,	-70d),//
      Row(21l,	-70d),
      Row(22l,	-70d),
      Row(23l,	-60d),
      Row(24l,	50d),//
      Row(25l,	20d),
      Row(26l,	-20d),
      Row(27l,	-30d),//
      Row(28l,	-30d) ,
      Row(29l,	10d),//
      Row(30l,	10d),
      Row(31l,	-30d)//
    )

    val schema: StructType =
      StructType(
        StructField("Timestamp", LongType, false) ::
          StructField("feature", DoubleType, false) :: Nil)
    val dataDF: DataFrame = sqlContext.createDataFrame(sc.makeRDD(data), schema)
    dataDF.printSchema()

    val averagedDF: DataFrame =  Resampling.movingAverageReal(dataDF,selectedFeature="feature",slidingWindowSize=3,TimeStamp_ColName = "Timestamp")
    //println(averagedDF.take(1)(2).getDouble(0))
    averagedDF.show()
    println("blablabl4: " + averagedDF.take(1)(0).get(2))//.getDouble(0)

  }


}
