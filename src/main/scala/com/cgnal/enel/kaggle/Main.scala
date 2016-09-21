package com.cgnal.enel.kaggle

import java.nio.file.{Paths, Files}
import java.util

import com.cgnal.efm.predmain.uta.timeseries.TimeSeriesUtils
import com.cgnal.enel.kaggle.helpers.DatasetHelper
import com.cgnal.enel.kaggle.models.EdgeDetection.EdgeDetection
import com.cgnal.enel.kaggle.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import java.io.{FileOutputStream, ObjectOutputStream, FileReader, StringReader}
import org.apache.spark.sql.expressions.{Window, WindowSpec}

import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import com.databricks.spark.avro._
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.io.Path
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
    val downsamplingBinSize = 6 // take one point every downsamplingBinSiz timestamps, unit: [167ms]

    val downsamplingBinPredictionSize = 60 // take one point every downsamplingBinPredictionSize points, unit: [downsamplingBinSize*167ms]

    val timestampIntervalPreEdge = 5L // time interval amplitude in sec. Note that the sampling bin is [downsamplingBinSize*167ms]
    val timestampIntervalPostEdge = 9L // time interval amplitude in sec. Note that the sampling bin is [downsamplingBinSize*167ms]

    val timestepsNumberPreEdge = 5 // number of points in the interval
    val timestepsNumberPostEdge = 8 // number of points in the interval
    val edgeWindowSize = timestepsNumberPreEdge + timestepsNumberPostEdge + 1

    // check consistency of timestamp intervals with timesteps number
    if ((timestampIntervalPreEdge/(downsamplingBinSize * 0.167)).round != timestepsNumberPreEdge ||
      (timestampIntervalPostEdge/(downsamplingBinSize * 0.167)).round - 1 != timestepsNumberPostEdge)
      sys.error("check the width of the pre and post edge intervals")


    type SelFeatureType = Double

    val filenameDfEdgeWindowsFeature = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/dfEdgeWindowsApplianceProva.csv"


    println("1. INGESTION (from csv to DataFrame)")
    var dateTime = DateTime.now()
    // TODO inserire ciclo su HOUSE e su GIORNI
    val filenameCSV_V = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/LF1V.csv"
    val filenameCSV_I = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/LF1I.csv"
    val filenameTimestamp = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/TimeTicks1.csv"
    val filenameTaggingInfo = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/TaggingInfo.csv"
    val filenameDfFeatures = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/dfFeatures.csv"

    val ingestionLabel = 0

    val dfFeatures = if (ingestionLabel == 1) {
      val dfVI = DatasetHelper.importingDatasetToDfHouseDay(filenameCSV_V, filenameCSV_I,
        filenameTimestamp,
        sc, sqlContext)
      val dfFeatures = DatasetHelper.addPowerToDfFeatures(dfVI)
      dfFeatures.printSchema()

      val path: Path = Path (filenameDfFeatures)
      if (path.exists) {
        Try(path.deleteRecursively())
      }
      dfFeatures.write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .save(filenameDfFeatures)
      dfFeatures
    }
    else {
      sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true") // Use first line of all files as header
        .option("inferSchema", "true")
        .load(filenameDfFeatures)
    }



    println("Time for INGESTION: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")



    println("2. RESAMPLING")
    dateTime = DateTime.now()
    val dfFeatureSmoothed = Resampling.movingAverageReal(dfFeatures, selectedFeature, averageSmoothingWindowSize)
    val dfFeatureResampled = Resampling.downsampling(dfFeatureSmoothed, downsamplingBinSize)
    val dfFeatureResampledDiff = Resampling.firstDifference(dfFeatureResampled, selectedFeature, "IDtime")

    val dfFeatureEdgeDetection = dfFeatureResampledDiff.cache()
    println("Time for RESAMPLING: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")





    println("3. EDGE DETECTION ALGORITHM")
    dateTime = DateTime.now()
    val arrayTaggingInfo: Array[(Array[String], Int)] = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameTaggingInfo)
    val dfTaggingInfo: DataFrame = DatasetHelper.fromArrayIndexedToDFTaggingInfo(sc, sqlContext,
      arrayTaggingInfo, DatasetHelper.TagSchema)

    println("3a Cross-validation")
    val (dfTaggingInfoTrain, dfTaggingInfoTest) = CrossValidation.validationSplit(dfTaggingInfo, sqlContext)

    println("3b Selecting edge windows for a given feature")
    EdgeDetection.computeStoreDfEdgeWindowsSingleFeature[SelFeatureType](dfFeatureEdgeDetection,
      dfTaggingInfoTrain,
      filenameTaggingInfo, filenameDfEdgeWindowsFeature,
      selectedFeature, timestampIntervalPreEdge, timestampIntervalPostEdge, edgeWindowSize,
      sc, sqlContext)

    // SINGLE FEATURE SELECTED FEATURE TYPE: DOUBLE --------------------------------------------------------------------
    println("3c. COMPUTING EDGE SIGNATURE of a single Feature")
    val filenameSampleSubmission = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/SampleSubmission.csv"
    val (dfEdgeSignatures, dfAppliancesToPredict) = EdgeDetection.computeEdgeSignatureAppliancesWithVar[SelFeatureType](filenameDfEdgeWindowsFeature,
      edgeWindowSize, selectedFeature, classOf[SelFeatureType],
      filenameSampleSubmission,
      sc, sqlContext)

    dfEdgeSignatures.cache()
    //  dfAppliancesToPredict.cache()

    println("Time for EDGE DETECTION: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")





    println("4. COMPUTING EDGE SIMILARITY for a single appliance")
    dateTime = DateTime.now()
    // TODO loop su appliances
    val applianceID = 30

    val appliances: Array[Int] = dfEdgeSignatures.select("ApplianceID").collect()
      .map(row => row.getAs[Int]("ApplianceID"))


    //    appliances.map { (applianceID: Int) =>

    val OnSignature: Array[SelFeatureType] = dfEdgeSignatures.filter(dfEdgeSignatures("ApplianceID") === (applianceID))
      .head.getAs[mutable.WrappedArray[SelFeatureType]]("ON_TimeSignature_" + selectedFeature).toArray[SelFeatureType]

    val OffSignature: Array[SelFeatureType] = dfEdgeSignatures.filter(dfEdgeSignatures("ApplianceID") === (applianceID))
      .head.getAs[mutable.WrappedArray[SelFeatureType]]("OFF_TimeSignature_" + selectedFeature).toArray[SelFeatureType]

    val dfRealFeatureEdgeScoreAppliance = EdgeDetection.computeSimilarityEdgeSignaturesRealFeatureGivenAppliance(dfFeatureEdgeDetection,
      selectedFeature,
      OnSignature, OffSignature,
      timestepsNumberPreEdge, timestepsNumberPostEdge, partitionNumber,
      sc, sqlContext).cache()

    println("Time for COMPUTING EDGE SIMILARITY: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")




    println("5 RESAMPLING SIMILARITY SCORES")
    dateTime = DateTime.now()
    val dfRealFeatureEdgeScoreApplianceDS = Resampling.edgeScoreDownsampling(dfRealFeatureEdgeScoreAppliance,
      selectedFeature, downsamplingBinPredictionSize).cache()

    dfRealFeatureEdgeScoreAppliance.unpersist()

    println("Time for RESAMPLING SIMILARITY SCORES: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")




    println("6. THRESHOLD + FINDPEAKS ESTRAZIONE ON_Time OFF_Time PREDICTED")
    dateTime = DateTime.now()

    val nrOfThresholds = 10

    println("6a add ground truth prediction")
    val onOffWindowsGroundTruth: Array[(Long, Long)] = dfTaggingInfo
      .filter(dfTaggingInfo("applianceID") === applianceID)
      .select("ON_Time", "OFF_Time").map(r => (r.getLong(0), r.getLong(1))).collect()

    val dfGroundTruth: DataFrame = TimeSeriesUtils.addOnOffStatusToDF(dfRealFeatureEdgeScoreApplianceDS, onOffWindowsGroundTruth,
      "TimestampPrediction", "GroundTruth").cache()

    println("6b selecting threshold to test")
    val thresholdToTestSorted: Array[SelFeatureType] = TimeSeriesUtils.extractingThreshold(dfRealFeatureEdgeScoreApplianceDS,
      "DeltaScorePrediction_" + selectedFeature,
      nrOfThresholds)

    //    val thresholdToTestSorted = Array(0d)

    println("6c computing hamming loss for each threshold")
    val hammingLosses: Array[Double] = thresholdToTestSorted.map(threshold =>
      TimeSeriesUtils.evaluateHammingLoss(
        dfRealFeatureEdgeScoreApplianceDS,
        dfGroundTruth, "GroundTruth", "DeltaScorePrediction_" + selectedFeature,
        "TimestampPrediction", applianceID, threshold)
    )

    val HLoverThreshold: Array[(SelFeatureType, SelFeatureType)] = thresholdToTestSorted.zip(hammingLosses)

    println("Time for THRESHOLD + FINDPEAKS ESTRAZIONE ON_Time OFF_Time PREDICTED: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")


    HLoverThreshold.foreach(x => println(x._1, x._2))

  }


}





