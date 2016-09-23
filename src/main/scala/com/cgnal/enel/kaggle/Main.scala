package com.cgnal.enel.kaggle

import java.nio.charset.StandardCharsets
import java.nio.file.{Paths, Files}
import breeze.numerics.round
import com.cgnal.enel.kaggle.models.edgeDetection.{SimilarityScore, EdgeDetection}
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

    // PARAMETERS ------------------------------------------------------------------------------------------------------
    // DATASET TRAINING AND TEST SET
/*    val dayFolderArrayTraining = Array("10_22_1350889201", "10_23_1350975601", "10_24_1351062001")
    val dayFolderTest = "10_25_1351148401"
    val house = "H1"
*/
    val dayFolderArrayTraining = Array("07_27_1343372401")
    val dayFolderTest = "07_26_1343286001"
    val house = "H4"

    val partitionNumber = 4

    // MODEL PARAMETERS
    val selectedFeature = "RealPowerFund"
    type SelFeatureType = Double

    val averageSmoothingWindowSize = 6 // number of timestamps, unit: [167ms]
    val downsamplingBinSize = 6 // take one point every downsamplingBinSiz timestamps, unit: [167ms]


    val timestampIntervalPreEdge = 5L // time interval amplitude in sec. Note that the sampling bin is [downsamplingBinSize*167ms]
    val timestampIntervalPostEdge = 5L // time interval amplitude in sec. Note that the sampling bin is [downsamplingBinSize*167ms]

    val nrThresholdsPerAppliance = 20

    val readingFromFileLabelDfIngestion = 1
    
    val readingFromFileLabelDfEdgeSignature = 0

    //------------------------------------------------------------------------------------------------------------------

    val timestepsNumberPreEdge= (timestampIntervalPreEdge.toInt/(downsamplingBinSize * 0.167)).round.toInt // number of points in the interval
    val timestepsNumberPostEdge = (timestampIntervalPostEdge/(downsamplingBinSize * 0.167)).round.toInt // number of points in the interval
    val edgeWindowSize = timestepsNumberPreEdge + timestepsNumberPostEdge + 1
    val downsamplingBinPredictionSize: Int = 60/(downsamplingBinSize * 0.167).round.toInt
    //------------------------------------------------------------------------------------------------------------------



    // 1 INGESTION -----------------------------------------------------------------------------------------------------
    println("1. INGESTION (from csv to DataFrame)")
    var dateTime = DateTime.now()
    // TODO inserire ciclo su HOUSE
    // Training set
    val dfFeatureTrain = CrossValidation.creatingDfFeatureFixedHouseOverDays(dayFolderArrayTraining, house,
      sc, sqlContext, readingFromFileLabelDfIngestion)
    // Test set
    val dfFeatureTest = CrossValidation.creatingDfFeatureFixedHouseAndDay(dayFolderTest, house,
      sc, sqlContext, readingFromFileLabelDfIngestion)
    println("Time for INGESTION: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")
    //------------------------------------------------------------------------------------------------------------------


    // 2 RESAMPLING -----------------------------------------------------------------------------------------------------
    println("2. RESAMPLING")
    dateTime = DateTime.now()
    // TRAINING SET
    val dfFeatureSmoothedTrain = Resampling.movingAverageReal(dfFeatureTrain, selectedFeature, averageSmoothingWindowSize)
    val dfFeatureResampledTrain = Resampling.downsampling(dfFeatureSmoothedTrain, downsamplingBinSize)
    // first diff
    val dfFeatureResampledDiffTrain = Resampling.firstDifference(dfFeatureResampledTrain, selectedFeature, "IDtime")
    val dfFeatureEdgeDetectionTrain = dfFeatureResampledDiffTrain.cache()

    // TEST SET
    val dfFeatureSmoothedTest = Resampling.movingAverageReal(dfFeatureTest, selectedFeature, averageSmoothingWindowSize)
    val dfFeatureResampledTest = Resampling.downsampling(dfFeatureSmoothedTest, downsamplingBinSize)
    // first diff
    val dfFeatureResampledDiffTest = Resampling.firstDifference(dfFeatureResampledTest, selectedFeature, "IDtime")
    val dfFeatureEdgeDetectionTest = dfFeatureResampledDiffTest.cache()
    println("Time for RESAMPLING: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")
    //------------------------------------------------------------------------------------------------------------------


    val dirNameTrain = ReferencePath.datasetDirPath + house + "/Train" + dayFolderArrayTraining(0) +
      "_" + dayFolderArrayTraining.last + "_avg" + averageSmoothingWindowSize.toString +
    "_dw" + downsamplingBinSize.toString + "_preInt" + timestampIntervalPreEdge.toString +
    "_postInt" + timestampIntervalPostEdge.toString

    // 3 EDGE DETECTION ALGORITHM -----------------------------------------------------------------------------------------------------
    println("3. EDGE DETECTION ALGORITHM")
    dateTime = DateTime.now()
    // TAGGING INFO TRAIN
    val dfTaggingInfoTrain = CrossValidation.creatingDfTaggingInfoFixedHouseOverDays(dayFolderArrayTraining, house,
      sc, sqlContext)
    // TAGGING INFO TEST
    val dfTaggingInfoTest = CrossValidation.creatingDfTaggingInfoFixedHouseAndDay(dayFolderTest, house,
      sc, sqlContext)

    // Prepare dataset to compute edge signature (for all the appliances)
    println("3b Selecting edge windows for a given feature")


    val dfEdgeSignaturesFileName = dirNameTrain + "/onoff_EdgeSignatureWithVar.csv"

    val dfEdgeSignatures =
      if (readingFromFileLabelDfEdgeSignature == 0) {
        EdgeDetection.buildStoreDfEdgeSignature(classOf[SelFeatureType],dfFeatureEdgeDetectionTrain,
          dfTaggingInfoTrain, selectedFeature, timestampIntervalPreEdge, timestampIntervalPostEdge, edgeWindowSize,
          dfEdgeSignaturesFileName,
          sc, sqlContext)
      }
      else {
        sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true") // Use first line of all files as header
          .option("inferSchema", "true") // Automatically infer data types
          .load(dfEdgeSignaturesFileName)
      }
    //------------------------------------------------------------------------------------------------------------------


    // SELECTING THE APPLIANCES TO MAKE PREDICTION FOR -----------------------------------------------------------------
    val filenameSampleSubmission = ReferencePath.filenameSampleSubmission
    val dfSampleSubmission = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(filenameSampleSubmission)

    val dfAppliancesSampleSubmission: DataFrame = dfSampleSubmission.select("Appliance").distinct()

    val appliancesTrain: Array[Int] = dfEdgeSignatures.join(dfAppliancesSampleSubmission,
      dfEdgeSignatures("ApplianceID") === dfAppliancesSampleSubmission("Appliance")).select("ApplianceID")
      .map(row => row.getAs[Int](0)).collect()
    //------------------------------------------------------------------------------------------------------------------


    // BUILD PREDICTION AND COMPUTE HAMMING LOSS OVER ALL THE APPLIANCES -----------------------------------------------
    // TRAINING SET
    val bestResultOverAppliancesTrain = EdgeDetection.buildPredictionRealFeatureLoopOverAppliances(dfFeatureEdgeDetectionTrain,
      dfEdgeSignatures, dfTaggingInfoTrain,
      appliancesTrain,
      selectedFeature,
      timestepsNumberPreEdge, timestepsNumberPostEdge,
      downsamplingBinPredictionSize,
      nrThresholdsPerAppliance,
      partitionNumber,
      dirNameTrain,
      sc: SparkContext, sqlContext: SQLContext)

    bestResultOverAppliancesTrain.foreach(x => println(x))

    val temp = bestResultOverAppliancesTrain.toList
    val storeTrain = new ObjectOutputStream(new FileOutputStream(dirNameTrain + "/bestResultsOverAppliances.dat"))
    storeTrain.writeObject(temp)
    storeTrain.close()


    // TEST SET
    val dirNameTest = ReferencePath.datasetDirPath + house + "/Test" + dayFolderTest

    val bestResultOverAppliancesTest = EdgeDetection.buildPredictionRealFeatureLoopOverAppliances(dfFeatureEdgeDetectionTest,
      dfEdgeSignatures, dfTaggingInfoTest,
      appliancesTrain,
      selectedFeature,
      timestepsNumberPreEdge, timestepsNumberPostEdge,
      downsamplingBinPredictionSize,
      nrThresholdsPerAppliance,
      partitionNumber,
      dirNameTest,
      sc: SparkContext, sqlContext: SQLContext)

    bestResultOverAppliancesTest.foreach(x => println(x))

    val temp2 = bestResultOverAppliancesTest.toList
    val storeTest = new ObjectOutputStream(new FileOutputStream(dirNameTest + "/bestResultsOverAppliances.dat"))
    storeTest.writeObject(temp2)
    storeTest.close()

    //------------------------------------------------------------------------------------------------------------------

  }
}





