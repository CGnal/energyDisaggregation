package com.cgnal.enel.kaggle

import java.nio.file.{Paths, Files}
import java.util

import com.cgnal.efm.predmain.uta.timeseries.TimeSeriesUtils
import com.cgnal.enel.kaggle.helpers.DatasetHelper
import com.cgnal.enel.kaggle.models.EdgeDetection.EdgeDetection
import com.cgnal.enel.kaggle.utils._//{Resampling, myUDAFs,ProvaUDAF, AverageOverComplex, ComplexMap}
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

//    mainTrue(args)

    mainFastCheck()
  }

  def mainTrue(args: Array[String]) = {

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
        filenameTimestamp, filenameTaggingInfo,
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
    val arrayTaggingInfo = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameTaggingInfo)
    val dfTaggingInfo: DataFrame = DatasetHelper.fromArrayIndexedToDFTaggingInfo(sc, sqlContext,
      arrayTaggingInfo, DatasetHelper.TagSchema)

    println("3a Cross-validation")

    

    println("3b Selecting edge windows for a given feature")
    //TODO: VALIDATION SET
    EdgeDetection.computeStoreDfEdgeWindowsSingleFeature[SelFeatureType](dfFeatureEdgeDetection,
      dfTaggingInfo,
      filenameTaggingInfo, filenameDfEdgeWindowsFeature,
      selectedFeature, timestampIntervalPreEdge, timestampIntervalPostEdge, edgeWindowSize,
      sc, sqlContext)


    // SINGLE FEATURE SELECTED FEATURE TYPE: DOUBLE --------------------------------------------------------------------
    println("3c. COMPUTING EDGE SIGNATURE of a single Feature")
    val filenameSampleSubmission = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/SampleSubmission.csv"
    val (dfEdgeSignatures, dfAppliancesToPredict) = EdgeDetection.computeEdgeSignatureAppliances[SelFeatureType](filenameDfEdgeWindowsFeature,
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
    val nrOfAppliances = 1

    println("6a add ground truth prediction")
    val dfGroundTruth: DataFrame = TimeSeriesUtils.addOnOffRangesToDF(dfRealFeatureEdgeScoreApplianceDS, "TimestampPrediction",
      dfTaggingInfo, applianceID, "applianceID", "ON_Time", "OFF_Time", "GroundTruth").cache()

    println("6b selecting threshold to test")
    val thresholdToTestSorted = TimeSeriesUtils.extractingThreshold(dfRealFeatureEdgeScoreApplianceDS,
      "DeltaScorePrediction_" + selectedFeature,
      nrOfThresholds)

//    val thresholdToTestSorted = Array(0d)

    println("6c computing hamming loss for each threshold")
    val hammingLosses: Array[Double] = thresholdToTestSorted.map(threshold =>
      TimeSeriesUtils.evaluateHammingLoss(
        dfRealFeatureEdgeScoreApplianceDS,
        dfGroundTruth, "GroundTruth", "DeltaScorePrediction_" + selectedFeature,
        "TimestampPrediction", nrOfAppliances, threshold)
    )

    val HLoverThreshold = thresholdToTestSorted.zip(hammingLosses)

    println("Time for THRESHOLD + FINDPEAKS ESTRAZIONE ON_Time OFF_Time PREDICTED: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")


    HLoverThreshold.foreach(x => println(x._1, x._2))
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

    // SINGLE FEATURE SELECTED FEATURE TYPE: DOUBLE --------------------------------------------------------------------
    type SelFeatureType = Double
    val filenameDfEdgeWindowsFeature = "/Users/aagostinelli/Desktop/EnergyDisaggregation/shortSparkTest/dfEdgeWindowsApplianceProva.csv"
    val filenameSampleSubmission = "/Users/aagostinelli/Desktop/EnergyDisaggregation/SampleSubmission.csv"

    val timestepsNumberPreEdge = 4 // number of points in the interval
    val timestepsNumberPostEdge = 7 // number of points in the interval
    val edgeWindowSize = timestepsNumberPreEdge + timestepsNumberPostEdge + 1
    val selectedFeature = "RealPowerFund"

    println("3b. COMPUTING EDGE SIGNATURE of a single Feature")
    val (dfEdgeSignatures, dfAppliancesToPredict) =
      computeEdgeSignatureAppliancesWithStD[SelFeatureType](
        filenameDfEdgeWindowsFeature,
        edgeWindowSize, selectedFeature, classOf[SelFeatureType],
        filenameSampleSubmission, sc, sqlContext)

    dfEdgeSignatures.cache()
    dfAppliancesToPredict.cache()
    dfEdgeSignatures.show()
    dfAppliancesToPredict.show()


  }
  ///#########################################################
  ///#########################################################
  ///#########################################################
  ///#########################################################
  // type SelFeatureType = Double
  def computeEdgeSignatureAppliancesWithStD[SelFeatureType:ClassTag](dfEdgeWindowsFilename: String, edgeWindowSize: Int,
                                                                     selectedFeature: String, selectedFeatureType: Class[SelFeatureType],
                                                                     filenameSampleSubmission: String,
                                                                     sc: SparkContext, sqlContext: SQLContext) = {


    if (!(selectedFeatureType.isAssignableFrom(classOf[Map[String,Double]]) || selectedFeatureType.isAssignableFrom(classOf[Double])))
      sys.error("Selected Feature Type must be Double or Map[String,Double]")

    val dfSampleSubmission = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(filenameSampleSubmission)

    val dfAppliancesToPredict = dfSampleSubmission.select("Appliance").distinct()

    val dfEdgeWindowsTaggingInfo = sqlContext.read.avro(dfEdgeWindowsFilename).cache()

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

        val averageOverRealON = new AverageOverReal("ON_TimeWindow_" + selectedFeature, edgeWindowSize)
        val averageOverRealOFF = new AverageOverReal("OFF_TimeWindow_" + selectedFeature, edgeWindowSize)

        val dfEdgeSignatures: DataFrame = dfEdgeWindowsTaggingInfo.groupBy("ApplianceID").agg(
          averageOverRealON(dfEdgeWindowsTaggingInfo.col("ON_TimeWindow_" + selectedFeature)).as("ON_TimeSignature_" + selectedFeature),
          averageOverRealOFF(dfEdgeWindowsTaggingInfo.col("OFF_TimeWindow_" + selectedFeature)).as("OFF_TimeSignature_" + selectedFeature))

        dfEdgeSignatures.printSchema()
        dfEdgeSignatures
      }

    dfEdgeWindowsTaggingInfo.unpersist()

    val dfEdgeSignatures = dfEdgeSignaturesAll.join(dfAppliancesToPredict, dfEdgeSignaturesAll("ApplianceID") === dfAppliancesToPredict("Appliance"))
      .drop(dfAppliancesToPredict("Appliance"))

    (dfEdgeSignatures, dfAppliancesToPredict)
  }
  ///#########################################################
  ///#########################################################
  ///#########################################################
  //#########################################################


}
