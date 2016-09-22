package com.cgnal.enel.kaggle

import java.nio.charset.StandardCharsets
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


    println("1. INGESTION (from csv to DataFrame)")
    var dateTime = DateTime.now()
    // TODO inserire ciclo su HOUSE e su GIORNI
    val filenameSampleSubmission = ReferencePath.filenameSampleSubmission


    val dayFolderArrayTraining = Array("07_26_1343286001")
    val dayFolderTest = "07_27_1343372401"
    val house = "H4"


    val dfFeatureTrain = CrossValidation.creatingDfFeatureFixedHouseOverDays(dayFolderArrayTraining, house,
      sc, sqlContext, 0)

    val dfFeatureTest = CrossValidation.creatingDfFeatureFixedHouseAndDay(dayFolderTest, house,
      sc, sqlContext, 0)


    println("Time for INGESTION: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")



    println("2. RESAMPLING")
    dateTime = DateTime.now()
    // TRAINING SET
    val dfFeatureSmoothedTrain = Resampling.movingAverageReal(dfFeatureTrain, selectedFeature, averageSmoothingWindowSize)
    val dfFeatureResampledTrain = Resampling.downsampling(dfFeatureSmoothedTrain, downsamplingBinSize)
    val dfFeatureResampledDiffTrain = Resampling.firstDifference(dfFeatureResampledTrain, selectedFeature, "IDtime")

    val dfFeatureEdgeDetectionTrain = dfFeatureResampledDiffTrain.cache()

    // TEST SET
    val dfFeatureSmoothedTest = Resampling.movingAverageReal(dfFeatureTest, selectedFeature, averageSmoothingWindowSize)
    val dfFeatureResampledTest = Resampling.downsampling(dfFeatureSmoothedTest, downsamplingBinSize)
    val dfFeatureResampledDiffTest = Resampling.firstDifference(dfFeatureResampledTest, selectedFeature, "IDtime")

    val dfFeatureEdgeDetectionTest = dfFeatureResampledDiffTest.cache()
    println("Time for RESAMPLING: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")



    println("3. EDGE DETECTION ALGORITHM")
    dateTime = DateTime.now()
    // TAGGING INFO TRAIN
    val dfTaggingInfoTrain = CrossValidation.creatingDfTaggingInfoFixedHouseOverDays(dayFolderArrayTraining, house,
      sc, sqlContext)
    // TAGGING INFO TEST
    val dfTaggingInfoTest = CrossValidation.creatingDfTaggingInfoFixedHouseAndDay(dayFolderTest, house,
      sc, sqlContext)


    println("3b Selecting edge windows for a given feature")

    val dirNameTrain = ReferencePath.datasetDirPath + house + "/Train" + dayFolderArrayTraining(0) +
      "_" + dayFolderArrayTraining.last

    val dfEdgeWindowsFilename = dirNameTrain + "/dfFeatureEdgeWindows.csv"

    EdgeDetection.computeStoreDfEdgeWindowsSingleFeature[SelFeatureType](dfFeatureEdgeDetectionTrain,
      dfTaggingInfoTrain,
      dfEdgeWindowsFilename,
      selectedFeature, timestampIntervalPreEdge, timestampIntervalPostEdge, edgeWindowSize,
      sc, sqlContext)

    // SINGLE FEATURE SELECTED FEATURE TYPE: DOUBLE --------------------------------------------------------------------
    println("3c. COMPUTING EDGE SIGNATURE of a single Feature")
    val dfEdgeWindowsTaggingInfo = sqlContext.read.avro(dfEdgeWindowsFilename).cache()

    val dfEdgeSignatures = EdgeDetection.computeEdgeSignatureAppliancesWithVar[SelFeatureType](dfEdgeWindowsTaggingInfo,
      edgeWindowSize, selectedFeature, classOf[SelFeatureType],
      filenameSampleSubmission,
      sc, sqlContext)

    dfEdgeSignatures.cache()
    println("Time for EDGE DETECTION: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")

    // saving dfEdgeSignature
    val edgeVarOutputFileName = dirNameTrain + "/onoff_EdgeSignatureWithVar.csv"
    CSVutils.storingSparkCsv(dfEdgeSignatures, edgeVarOutputFileName)








    println("4. COMPUTING EDGE SIMILARITY for a single appliance")
    dateTime = DateTime.now()

    // selecting appliances founded in the Training set
    val dfSampleSubmission = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(filenameSampleSubmission)

    val dfAppliancesSampleSubmission: DataFrame = dfSampleSubmission.select("Appliance").distinct()

    val appliancesTrain: Array[Int] = dfEdgeSignatures.join(dfAppliancesSampleSubmission,
      dfEdgeSignatures("ApplianceID") === dfAppliancesSampleSubmission).select("ApplianceID")
      .map(row => row.getAs[Int](0)).collect()


    //Array esterno appliance, Array interno threshold
    val resultsOverAppliances: Array[(Int, String, Array[(SelFeatureType, SelFeatureType)])] =
      appliancesTrain.map { (applianceID: Int) =>


        val OnSignature: Array[SelFeatureType] = dfEdgeSignatures.filter(dfEdgeSignatures("ApplianceID") === (applianceID))
          .head.getAs[mutable.WrappedArray[SelFeatureType]]("ON_TimeSignature_" + selectedFeature).toArray[SelFeatureType]

        val OffSignature: Array[SelFeatureType] = dfEdgeSignatures.filter(dfEdgeSignatures("ApplianceID") === (applianceID))
          .head.getAs[mutable.WrappedArray[SelFeatureType]]("OFF_TimeSignature_" + selectedFeature).toArray[SelFeatureType]

        // EVALUATION HL OVER TRAINING SET
        val dfRealFeatureEdgeScoreApplianceTrain = EdgeDetection.computeSimilarityEdgeSignaturesRealFeatureGivenAppliance(dfFeatureEdgeDetectionTrain,
          selectedFeature,
          OnSignature, OffSignature,
          timestepsNumberPreEdge, timestepsNumberPostEdge, partitionNumber,
          sc, sqlContext).cache()

        // saving dfRealFeatureEdgeScoreApplianceTrain
        val filenameDfRealFeatureEdgeScoreAppliance = dirNameTrain + "/ScoreNoDS_AppID" + applianceID.toString + ".csv"
        CSVutils.storingSparkCsv(dfRealFeatureEdgeScoreApplianceTrain, filenameDfRealFeatureEdgeScoreAppliance)


        println("Time for COMPUTING EDGE SIMILARITY: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")
        println("5 RESAMPLING SIMILARITY SCORES")
        dateTime = DateTime.now()
        val dfRealFeatureEdgeScoreApplianceDStrain = Resampling.edgeScoreDownsampling(dfRealFeatureEdgeScoreApplianceTrain,
          selectedFeature, downsamplingBinPredictionSize).cache()

        dfRealFeatureEdgeScoreApplianceTrain.unpersist()

        println("Time for RESAMPLING SIMILARITY SCORES: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")

        // saving dfRealFeatureEdgeScoreApplianceTrain DOWNSAMPLED
        val filenameDfRealFeatureEdgeScoreApplianceDS = dirNameTrain + "/ScoreDS_AppID" + applianceID.toString + ".csv"
        CSVutils.storingSparkCsv(dfRealFeatureEdgeScoreApplianceDStrain, filenameDfRealFeatureEdgeScoreApplianceDS)



        println("6. THRESHOLD + FINDPEAKS ESTRAZIONE ON_Time OFF_Time PREDICTED")
        dateTime = DateTime.now()

        val nrOfThresholds = 2

        println("6a add ground truth prediction")
        val onOffWindowsGroundTruth: Array[(Long, Long)] = dfTaggingInfoTrain
          .filter(dfTaggingInfoTrain("applianceID") === applianceID)
          .select("ON_Time", "OFF_Time").map(r => (r.getLong(0), r.getLong(1))).collect()

        val outputFilename = dirNameTrain + "/OnOffArrayGroundTruth_AppID" + applianceID.toString + ".txt"

        val stringOnOff: String = onOffWindowsGroundTruth.mkString("\n")
        Files.write(Paths.get(outputFilename), stringOnOff.getBytes(StandardCharsets.UTF_8))



        val dfGroundTruth: DataFrame = HammingLoss.addOnOffStatusToDF(dfRealFeatureEdgeScoreApplianceDStrain, onOffWindowsGroundTruth,
          "TimestampPrediction", "GroundTruth").cache()

        println("6b selecting threshold to test")
        val thresholdToTestSorted: Array[SelFeatureType] = SimilarityScore.extractingThreshold(dfRealFeatureEdgeScoreApplianceDStrain,
          "DeltaScorePrediction_" + selectedFeature,
          nrOfThresholds)


        println("6c computing hamming loss for each threshold")
        val hammingLosses: Array[Double] = thresholdToTestSorted.map(threshold =>
          HammingLoss.evaluateHammingLoss(
            dfRealFeatureEdgeScoreApplianceDStrain,
            dfGroundTruth, "GroundTruth", "DeltaScorePrediction_" + selectedFeature,
            "TimestampPrediction", applianceID, threshold, dirNameTrain)
        )

        val HLoverThreshold: Array[(SelFeatureType, SelFeatureType)] = thresholdToTestSorted.zip(hammingLosses)
        println("Time for THRESHOLD + FINDPEAKS ESTRAZIONE ON_Time OFF_Time PREDICTED: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")



        HLoverThreshold.foreach(x => println(x._1, x._2))

        val applianceName = dfTaggingInfoTrain.filter(dfTaggingInfoTrain("applianceID") === applianceID).head.getAs[String]("ApplianceName")

        (applianceID, applianceName, HLoverThreshold)
      }
    //save temp object
    val temp = resultsOverAppliances.toList
    val store = new ObjectOutputStream(new FileOutputStream(dirNameTrain + "/resultsOverAppliances.dat"))
    store.writeObject(temp)
    store.close()


    val bestResultOverAppliances =
      HammingLoss.extractingHLoverThresholdAndAppliances(dirNameTrain + "/resultsOverAppliances.dat")

  }

}





