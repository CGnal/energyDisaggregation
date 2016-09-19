package com.cgnal.enel.kaggle

import java.nio.file.{Files, Paths}
import java.util

import com.cgnal.efm.predmain.uta.timeseries.TimeSeriesUtils
import com.cgnal.enel.kaggle.helpers.DatasetHelper
import com.cgnal.enel.kaggle.models.EdgeDetection.EdgeDetection
import com.cgnal.enel.kaggle.utils.{AverageOverComplex, ComplexMap, ProvaUDAF, Resampling}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.{FileOutputStream, FileReader, ObjectOutputStream, StringReader}

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext}
import com.databricks.spark.avro._
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLag
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}

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
    val sqlContext: HiveContext = new HiveContext(sc)
    val filenameCSV = "/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/ExampleForCodeTest/testV.csv"

    val data = Seq(
      Row(1l,	-101d),
      Row(2l,	100d),
      Row(3l,	105d),
      Row(4l,	-100d),
      Row(5l,	-90d),
      Row(6l,	-120d), Row(7l,	50d), Row(8l,	20d),
      Row(9l,	-20d), Row(10l,	-40d), Row(11l,	-50d), Row(12l,	30d),
      Row(13l,	-30d), Row(14l,	150d), Row(15l,	150d), Row(16l,	100d), Row(17l,	102d), Row(18l,	102d), Row(19l,	90d), Row(20l,	-70d),
      Row(21l,	-70d), Row(22l,	-70d),
      Row(23l,	-60d), Row(24l,	50d), Row(25l,	20d), Row(26l,	-20d), Row(27l,	-30d), Row(28l,	-30d) , Row(29l,	10d), Row(30l,	10d), Row(31l,	-30d)//
    )

    val schema: StructType =
      StructType(
        StructField("Timestamp", LongType, false) ::
          StructField("feature", DoubleType, false) :: Nil)
    val dataDF: DataFrame = sqlContext.createDataFrame(sc.makeRDD(data), schema)
    dataDF.printSchema()

    val averagedDF: DataFrame =  Resampling.movingAverageReal(dataDF,selectedFeature = "feature",slidingWindowSize =3,TimeStamp_ColName = "Timestamp")

    //println(averagedDF.take(1)(2).getDouble(0))
    averagedDF.show()
    //println("blablabl4: " + averagedDF.take(1)(0).get(0))//.getDouble(0)

    val selectedFeature = "feature"
    val timestampCol = "Timestamp"   //"Timestamp"

    val prova = Resampling.firstDifference(averagedDF, selectedFeature, timestampCol)
    prova.show()

  }

  //#########################################################


  def mainToUse(args: Array[String]) = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("energyDisaggregation")
    val sc = new SparkContext(conf)
    //    val sqlContext = new SQLContext(sc)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val sqlContext = new HiveContext(sc)

    val selectedFeature = "RealPowerFund"
    val partitionNumber = 4

    val averageSmoothingWindowSize = 12 // number of timestamps, unit: [167ms]
    val downsamplingBinSize = 12 // take one point every downsamplingBinSiz timestamps, unit: [167ms]

    val downsamplingBinPredictionSize = 30 // take one point every downsamplingBinPredictionSize points, unit: [downsamplingBinSize*167ms]

    val timestampIntervalPreEdge = 4L // time interval in unit: [downsamplingBinSize*167ms]
    val timestampIntervalPostEdge = 8L // time interval in unit: [downsamplingBinSize*167ms]
    val timestepsNumberPreEdge = 4 // number of points in the interval
    val timestepsNumberPostEdge = 7 // number of points in the interval
    val edgeWindowSize = timestepsNumberPreEdge + timestepsNumberPostEdge + 1

    type SelFeatureType = Double

    val filenameDfEdgeWindowsFeature = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/dfEdgeWindowsApplianceProva.csv"


    println("1. INGESTION (from csv to DataFrame)")
    val filenameCSV_V = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/LF1V.csv"
    val filenameCSV_I = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/LF1I.csv"
    val filenameTimestamp = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/TimeTicks1.csv"
    val filenameTaggingInfo = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/TaggingInfo.csv"

    val dfVI = DatasetHelper.importingDatasetToDfHouseDay(filenameCSV_V, filenameCSV_I,
      filenameTimestamp, filenameTaggingInfo,
      sc, sqlContext).cache()

    val dfFeatures = DatasetHelper.addPowerToDfFeatures(dfVI)
    dfFeatures.printSchema()
    dfVI.unpersist()


    println("2. RESAMPLING")
    val dfFeatureSmoothed = Resampling.movingAverageReal(dfFeatures, selectedFeature, averageSmoothingWindowSize).cache()
    val dfFeatureResampled = Resampling.downsampling(dfFeatureSmoothed, downsamplingBinSize).cache()
    dfFeatureSmoothed.unpersist()
    // TODO add first difference


    println("3. EDGE DETECTION ALGORITHM")
    val arrayTaggingInfo = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameTaggingInfo)
    val dfTaggingInfo: DataFrame = DatasetHelper.fromArrayIndexedToDFTaggingInfo(sc, sqlContext,
      arrayTaggingInfo, DatasetHelper.TagSchema).cache()

    println("3a Selecting edge windows for a given feature")
/*    EdgeDetection.computeStoreDfEdgeWindowsSingleFeature[SelFeatureType](dfFeatureResampled,
      dfTaggingInfo,
      filenameTaggingInfo, filenameDfEdgeWindowsFeature,
      selectedFeature, timestampIntervalPreEdge, timestampIntervalPostEdge, edgeWindowSize,
      sc, sqlContext)
*/

    // SINGLE FEATURE SELECTED FEATURE TYPE: DOUBLE --------------------------------------------------------------------
    println("3b. COMPUTING EDGE SIGNATURE of a single Feature")
    val filenameSampleSubmission = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/SampleSubmission.csv"
    val (dfEdgeSignatures, dfAppliancesToPredict) = EdgeDetection.computeEdgeSignatureAppliances[SelFeatureType](filenameDfEdgeWindowsFeature,
      edgeWindowSize, selectedFeature, classOf[SelFeatureType],
      filenameSampleSubmission,
      sc, sqlContext)

    dfEdgeSignatures.cache()
    dfAppliancesToPredict.cache()


    println("4. COMPUTING EDGE SIMILARITY for a single appliance")
    // TODO loop su appliances
    val applianceID = 30

    val OnSignature: Array[SelFeatureType] = dfEdgeSignatures.filter(dfEdgeSignatures("ApplianceID") === (applianceID))
      .head.getAs[mutable.WrappedArray[SelFeatureType]]("ON_TimeSignature_" + selectedFeature).toArray[SelFeatureType]

    val OffSignature: Array[SelFeatureType] = dfEdgeSignatures.filter(dfEdgeSignatures("ApplianceID") === (applianceID))
      .head.getAs[mutable.WrappedArray[SelFeatureType]]("OFF_TimeSignature_" + selectedFeature).toArray[SelFeatureType]

    val dfRealFeatureEdgeScoreAppliance = EdgeDetection.computeSimilarityEdgeSignaturesRealFeatureGivenAppliance(dfFeatureResampled,
      selectedFeature,
      OnSignature, OffSignature,
      timestepsNumberPreEdge, timestepsNumberPostEdge, partitionNumber,
      sc, sqlContext).cache()


    println("5 RESAMPLING SIMILARITY SCORES")
    val dfRealFeatureEdgeScoreApplianceDS = Resampling.edgeScoreDownsampling(dfRealFeatureEdgeScoreAppliance,
      selectedFeature, downsamplingBinPredictionSize).cache()

    dfRealFeatureEdgeScoreAppliance.unpersist()


    println("6. THRESHOLD + FINDPEAKS ESTRAZIONE ON_Time OFF_Time PREDICTED")
    //TODO: VALIDATION SET
    val nrOfThresholds = 10
    val nrOfAppliances = 1

    println("6a add ground truth prediction")
    val dfGroundTruth: DataFrame = TimeSeriesUtils.addOnOffRangesToDF(dfRealFeatureEdgeScoreApplianceDS, "TimestampPrediction",
      dfTaggingInfo, applianceID, "applianceID", "ON_Time", "OFF_Time", "GroundTruth").cache()

    val HLoverThreshold = TimeSeriesUtils.hammingLossCurve(dfRealFeatureEdgeScoreApplianceDS,
      dfGroundTruth, "GroundTruth", "DeltaScorePrediction_" + selectedFeature,
      "TimestampPrediction", nrOfAppliances, nrOfThresholds)

    HLoverThreshold.foreach(x => println(x._1, x._2))
  }


}
