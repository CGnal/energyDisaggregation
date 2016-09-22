package com.cgnal.enel.kaggle

import java.nio.file.{Paths, Files}
import java.util
import com.cgnal.enel.kaggle.helpers.DatasetHelper
import com.cgnal.enel.kaggle.models.EdgeDetection.EdgeDetection
import com.cgnal.enel.kaggle.models.edgeDetection.SimilarityScore
import com.cgnal.enel.kaggle.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.first

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

    val filenameDfEdgeWindowsFeature = ReferencePath.filenameDfEdgeWindowsFeature


    println("1. INGESTION (from csv to DataFrame)")
    var dateTime = DateTime.now()
    // TODO inserire ciclo su HOUSE e su GIORNI
    val filenameSampleSubmission = ReferencePath.filenameSampleSubmission
    val filenameCSV_V = ReferencePath.filenameCSV_V
    val filenameCSV_I = ReferencePath.filenameCSV_I
    val filenameTimestamp = ReferencePath.filenameTimestamp
    val filenameTaggingInfo = ReferencePath.filenameTaggingInfo
    val filenameDfFeatures = ReferencePath.filenameDfFeatures
    ///////////
    ///##### writing output
    // Trigger OnOff tables per appliances, per threshold
    /////
    val outputDirName = ReferencePath.outputDirName
    val edgeVarOutputFileName = ReferencePath.edgeVarOutputFileName

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







    val dfSampleSubmission = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(filenameSampleSubmission)

    val dfAppliancesToPredict = dfSampleSubmission.select("Appliance").distinct()








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
    val dfEdgeSignatures = EdgeDetection.computeEdgeSignatureAppliancesWithVar[SelFeatureType](filenameDfEdgeWindowsFeature,
      edgeWindowSize, selectedFeature, classOf[SelFeatureType],
      filenameSampleSubmission,
      sc, sqlContext)

    dfEdgeSignatures.cache()


    val path: Path = Path (edgeVarOutputFileName)
    if (path.exists) {
      Try(path.deleteRecursively())
    }
    dfEdgeSignatures.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(edgeVarOutputFileName)


    println("Time for EDGE DETECTION: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")



    println("4. COMPUTING EDGE SIMILARITY for a single appliance")
    dateTime = DateTime.now()


    val appliancesTrain: Array[Int] = dfTaggingInfoTrain.groupBy("ApplianceID").agg(first("ApplianceID").as("ApplianceIDtrain"))
      .select("ApplianceIDtrain").map(row => row.getAs[Int](0)).collect()


    //Array esterno appliance, Array interno threshold
    val resultsOverAppliances: Array[(Int, String, Array[(SelFeatureType, SelFeatureType)])] =
      appliancesTrain.map { (applianceID: Int) =>


        val OnSignature: Array[SelFeatureType] = dfEdgeSignatures.filter(dfEdgeSignatures("ApplianceID") === (applianceID))
          .head.getAs[mutable.WrappedArray[SelFeatureType]]("ON_TimeSignature_" + selectedFeature).toArray[SelFeatureType]

        val OffSignature: Array[SelFeatureType] = dfEdgeSignatures.filter(dfEdgeSignatures("ApplianceID") === (applianceID))
          .head.getAs[mutable.WrappedArray[SelFeatureType]]("OFF_TimeSignature_" + selectedFeature).toArray[SelFeatureType]

        val dfRealFeatureEdgeScoreAppliance = EdgeDetection.computeSimilarityEdgeSignaturesRealFeatureGivenAppliance(dfFeatureEdgeDetection,
          selectedFeature,
          OnSignature, OffSignature,
          timestepsNumberPreEdge, timestepsNumberPostEdge, partitionNumber,
          sc, sqlContext).cache()

        var writingOutput = outputDirName + "ScoreNoDS" + "_AppID" + applianceID.toString + ".csv"
        val path2: Path = Path(writingOutput)
        if (path2.exists) {
          Try(path2.deleteRecursively())
        }
        dfRealFeatureEdgeScoreAppliance.write
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .save(writingOutput)

        println("Time for COMPUTING EDGE SIMILARITY: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")


        println("5 RESAMPLING SIMILARITY SCORES")
        dateTime = DateTime.now()
        val dfRealFeatureEdgeScoreApplianceDS = Resampling.edgeScoreDownsampling(dfRealFeatureEdgeScoreAppliance,
          selectedFeature, downsamplingBinPredictionSize).cache()

        dfRealFeatureEdgeScoreAppliance.unpersist()

        println("Time for RESAMPLING SIMILARITY SCORES: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")

        writingOutput = outputDirName + "ScoreDS" + "_AppID" + applianceID.toString + ".csv"
        val path: Path = Path(writingOutput)
        if (path.exists) {
          Try(path.deleteRecursively())
        }
        dfRealFeatureEdgeScoreApplianceDS.write
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .save(writingOutput)



        println("6. THRESHOLD + FINDPEAKS ESTRAZIONE ON_Time OFF_Time PREDICTED")
        dateTime = DateTime.now()

        val nrOfThresholds = 2

        println("6a add ground truth prediction")
        val onOffWindowsGroundTruth: Array[(Long, Long)] = dfTaggingInfo
          .filter(dfTaggingInfo("applianceID") === applianceID)
          .select("ON_Time", "OFF_Time").map(r => (r.getLong(0), r.getLong(1))).collect()

        val dfGroundTruth: DataFrame = HammingLoss.addOnOffStatusToDF(dfRealFeatureEdgeScoreApplianceDS, onOffWindowsGroundTruth,
          "TimestampPrediction", "GroundTruth").cache()

        println("6b selecting threshold to test")
        val thresholdToTestSorted: Array[SelFeatureType] = SimilarityScore.extractingThreshold(dfRealFeatureEdgeScoreApplianceDS,
          "DeltaScorePrediction_" + selectedFeature,
          nrOfThresholds)


        println("6c computing hamming loss for each threshold")
        val hammingLosses: Array[Double] = thresholdToTestSorted.map(threshold =>
          HammingLoss.evaluateHammingLoss(
            dfRealFeatureEdgeScoreApplianceDS,
            dfGroundTruth, "GroundTruth", "DeltaScorePrediction_" + selectedFeature,
            "TimestampPrediction", applianceID, threshold, outputDirName)
        )

        val HLoverThreshold: Array[(SelFeatureType, SelFeatureType)] = thresholdToTestSorted.zip(hammingLosses)
        println("Time for THRESHOLD + FINDPEAKS ESTRAZIONE ON_Time OFF_Time PREDICTED: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")



        HLoverThreshold.foreach(x => println(x._1, x._2))

        val applianceName = dfTaggingInfo.filter(dfTaggingInfo("applianceID") === applianceID).head.getAs[String]("ApplianceName")

        (applianceID, applianceName, HLoverThreshold)
      }
    //save temp object
    val temp = resultsOverAppliances.toList
    val store = new ObjectOutputStream(new FileOutputStream(outputDirName + "resultsOverAppliances.dat"))
    store.writeObject(temp)
    store.close()


    val bestResultOverAppliances =
      HammingLoss.extractingHLoverThresholdAndAppliances(outputDirName + "resultsOverAppliances.dat")

  }

}





