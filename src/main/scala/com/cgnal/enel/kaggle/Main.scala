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

import java.io._
import org.apache.spark.sql.expressions.{Window, WindowSpec}

import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{GroupedData, DataFrame, SQLContext, Row}
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

    // Initialization of the spark context
    val conf = new SparkConf().setMaster("local[4]").setAppName("energyDisaggregation")
    val sc = new SparkContext(conf)
    //    val sqlContext = new SQLContext(sc)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val sqlContext = new HiveContext(sc)

    // PARAMETERS ------------------------------------------------------------------------------------------------------
    // DATASET TRAINING AND TEST SET
    // The code is built to process data from a single home by splitting on training and test set based on the day
/*    val dayFolderArrayTraining = Array("10_22_1350889201", "10_23_1350975601") //folders corresponding to the days used as
    // training set (it can be an array with more days)
    val dayFolderTest = "10_25_1351148401" // folder corresponding to the day used as test set (it must contain one day)
    val house = "H1"
*/
    val dayFolderArrayTraining = Array("07_27_1343372401")
    val dayFolderTest = "07_26_1343286001"
    val house = "H4"

    // -----------------------------------------------------------------------------------------------------------------
    val partitionNumber = 4 // set equal to the number of local nodes

    // MODEL PARAMETERS
    val selectedFeature = "RealPowerFund" // Feature used to compute edgeSignature and similarity (it must be one of the features
    // founded in dfFeature)
    type SelFeatureType = Double

    val averageSmoothingWindowSize = 6 // number of timestamps, unit: [167ms]
    val downsamplingBinSize = 6 // take one point every downsamplingBinSiz timestamps, unit: [167ms]


    val timestampIntervalPreEdge = 5L // time interval amplitude in sec. Note that the sampling bin is [downsamplingBinSize*167ms]
    val timestampIntervalPostEdge = 5L // time interval amplitude in sec. Note that the sampling bin is [downsamplingBinSize*167ms]

    val nrThresholdsPerAppliance = 4

    val readingFromFileLabelDfIngestion = 1  // flag to read dfFeature (dataframe with the features) from filesystem (if previously computed)
    // instead of building it from csv
    val readingFromFileLabelDfEdgeSignature = 1  // flag to read dfEdgeSignature (dataframe with the ON/OFF signatures)
    // from filesystem (if previously computed) instead of computing it

    val scoresONcolName = "recipMsdON_TimePrediction_" + selectedFeature
    val scoresOFFcolName = "recipNegMsdOFF_TimePrediction_" + selectedFeature


    val extraLabelOutputDirName = "RecipMSDscore"

    //------------------------------------------------------------------------------------------------------------------
    val timestepsNumberPreEdge= (timestampIntervalPreEdge.toInt/(downsamplingBinSize * 0.167)).round.toInt // number of points in the interval
    val timestepsNumberPostEdge = (timestampIntervalPostEdge/(downsamplingBinSize * 0.167)).round.toInt - 1 // number of points in the interval
    val edgeWindowSize = timestepsNumberPreEdge + timestepsNumberPostEdge + 1
    val downsamplingBinPredictionSec = 60
    val downsamplingBinPredictionSize: Int = downsamplingBinPredictionSec/(downsamplingBinSize * 0.167).round.toInt
    //------------------------------------------------------------------------------------------------------------------

    if (nrThresholdsPerAppliance <= 1) sys.error("When using evenly spaced thresholds nrThresholdsPerAppliance must be >= 2 ")

    // OUTPUT DIR NAME -------------------------------------------------------------------------------------------------
    val dirNameTrain = ReferencePath.datasetDirPath + house + "/Train"+ extraLabelOutputDirName + dayFolderArrayTraining(0) +
      "_" + dayFolderArrayTraining.last + "_avg" + averageSmoothingWindowSize.toString +
      "_dw" + downsamplingBinSize.toString + "_preInt" + timestampIntervalPreEdge.toString +
      "_postInt" + timestampIntervalPostEdge.toString

    val dirNameTest = ReferencePath.datasetDirPath + house + "/Test" + extraLabelOutputDirName + dayFolderTest + "_avg" + averageSmoothingWindowSize.toString +
      "_dw" + downsamplingBinSize.toString + "_preInt" + timestampIntervalPreEdge.toString +
      "_postInt" + timestampIntervalPostEdge.toString

    val outputTextFilenameTraining = dirNameTrain + "/outputFileTraining.txt"
    val outputTextFilenameTest = dirNameTest + "/outputFileTest.txt"
    //------------------------------------------------------------------------------------------------------------------


    // SAVING PRINTED OUTPUT TO A FILE
    // inizializzazione
    val theDirTrain = new File(dirNameTrain)
    if (!theDirTrain.exists()) theDirTrain.mkdirs()
    val fileOutputTrain = new File(outputTextFilenameTraining)
    val bwTrain: BufferedWriter = new BufferedWriter(new FileWriter(fileOutputTrain))


    // 1 INGESTION -----------------------------------------------------------------------------------------------------
    println("1. INGESTION (from csv to DataFrame)")
    var dateTime = DateTime.now()
    // TODO inserire ciclo su HOUSE
    // Training set
    // create the dataframe with the features from csv (or read it from filesystem depending on the flag readingFromFileLabelDfIngestion)
    val dfFeatureTrain = CrossValidation.creatingDfFeatureFixedHouseOverDays(dayFolderArrayTraining, house,
      dirNameTrain, sc, sqlContext, readingFromFileLabelDfIngestion)
    // -----------------------------------------------------------------------------------------------------------------


    // TAGGING INFO TRAIN
    // Create the dataframe with the Tagging Info from csv
    val dfTaggingInfoTrainTemp = CrossValidation.creatingDfTaggingInfoFixedHouseOverDays(dayFolderArrayTraining, house,
      sc, sqlContext)
    val dfTaggingInfoTrainTemp2 = dfTaggingInfoTrainTemp.filter(dfTaggingInfoTrainTemp("ApplianceID")<=38)
    val dfTaggingInfoTrain = dfTaggingInfoTrainTemp2.filter(dfTaggingInfoTrainTemp2("ON_Time") !== dfTaggingInfoTrainTemp2("OFF_Time"))
      .cache()


    // TAGGING INFO TEST
    val dfTaggingInfoTestTemp = CrossValidation.creatingDfTaggingInfoFixedHouseAndDay(dayFolderTest, house,
      sc, sqlContext)
    val dfTaggingInfoTestTemp2 = dfTaggingInfoTestTemp.filter(dfTaggingInfoTestTemp("ApplianceID")<=38)
    val dfTaggingInfoTest = dfTaggingInfoTestTemp2.filter(dfTaggingInfoTestTemp2("ON_Time") !== dfTaggingInfoTestTemp2("OFF_Time"))
      .cache()

    //------------------------------------------------------------------------------------------------------------------

    // NUMEROSITY
    // CHECK of the number of appliances and edges founded in the TRAINING SET
    val nrEdgesPerApplianceTrain: Map[Int, Int] = dfTaggingInfoTrain.select("ApplianceID").map(row => row.getAs[Int](0)).collect()
      .groupBy(identity).map({case(x,y) => (x,y.size)})
    val appliancesTrain = nrEdgesPerApplianceTrain.keySet.toArray
    println("Number of appliances in the training set: " + appliancesTrain.length.toString)
    nrEdgesPerApplianceTrain.foreach({case(x,y) => println("ApplianceID: " + x.toString + " number of ON/OFF events in the training set: " + y.toString)})

    // CHECK of the number of appliances and edges founded in the TEST SET
    val nrEdgesPerApplianceTest: Map[Int, Int] = dfTaggingInfoTest.filter(dfTaggingInfoTest("ApplianceID").isin(appliancesTrain:_*))
      .select("ApplianceID").map(row => row.getAs[Int](0)).collect()
      .groupBy(identity).map({case(x,y) => (x,y.size)})
    val appliancesTest = nrEdgesPerApplianceTest.keySet.toArray
    println("\n\nNumber of appliances in the test (and in the training) set: " + appliancesTest.length.toString)
    nrEdgesPerApplianceTest.foreach({case(x,y) => println("ApplianceID: " + x.toString + " number of ON/OFF events in the test set: " + y.toString)})



    // 2 RESAMPLING ----------------------------------------------------------------------------------------------------
    println("2. RESAMPLING")
    dateTime = DateTime.now()
    // TRAINING SET
    val dfFeatureSmoothedTrain = Resampling.movingAverageReal(dfFeatureTrain, selectedFeature, averageSmoothingWindowSize)
    val dfFeatureResampledTrain = Resampling.downsampling(dfFeatureSmoothedTrain, downsamplingBinSize)
    // first diff
    val dfFeatureResampledDiffTrain = Resampling.firstDifference(dfFeatureResampledTrain, selectedFeature, "IDtime")
    val dfFeatureEdgeDetectionTrain = dfFeatureResampledDiffTrain.cache()

    // Serialization of the dfFeature actually used
    CSVutils.storingSparkCsv(dfFeatureEdgeDetectionTrain, dirNameTrain + "/dfFeaturePreProcessed.csv")
    // -----------------------------------------------------------------------------------------------------------------


    // 3 EDGE DETECTION ALGORITHM --------------------------------------------------------------------------------------
    println("3. EDGE DETECTION ALGORITHM")
    dateTime = DateTime.now()


    val dfEdgeSignaturesFileName = dirNameTrain + "/onoff_EdgeSignature"
    // Compute edge signature relative to a single (real) feature (for all the appliances)
    // (or read it from filesystem depending on the flag readingFromFileLabelDfEdgeSignature)
    val dfEdgeSignatures =
      if (readingFromFileLabelDfEdgeSignature == 0) {
        EdgeDetection.buildStoreDfEdgeSignature(classOf[SelFeatureType],dfFeatureEdgeDetectionTrain,
          dfTaggingInfoTrain, selectedFeature, timestampIntervalPreEdge, timestampIntervalPostEdge, edgeWindowSize,
          dfEdgeSignaturesFileName,
          sc, sqlContext)
      }
      else {
        sqlContext.read.avro(dfEdgeSignaturesFileName + ".avro").cache()
      }
    //------------------------------------------------------------------------------------------------------------------



    //------------------------------------------------------------------------------------------------------------------
    // BUILD PREDICTION AND COMPUTE HAMMING LOSS OVER ALL THE APPLIANCES -----------------------------------------------
    // TRAINING SET
    // build prediction for each appliance in the appliancesTrain array by using nrThresholdsPerAppliance threshold
    // evenly spaced between 0 and the max value founded
    bwTrain.write("TRAINING SET loop over appliances (and thresholds):")
    val bestResultOverAppliancesTrain = EdgeDetection.buildPredictionRealFeatureLoopOverAppliances(dfFeatureEdgeDetectionTrain,
      dfEdgeSignatures, dfTaggingInfoTrain,
      appliancesTrain,
      selectedFeature,
      timestepsNumberPreEdge, timestepsNumberPostEdge,
      downsamplingBinPredictionSize,
      nrThresholdsPerAppliance,
      partitionNumber,
      scoresONcolName, scoresOFFcolName,
      dirNameTrain, bwTrain, downsamplingBinPredictionSec,
      sc: SparkContext, sqlContext: SQLContext)

    dfTaggingInfoTrain.unpersist()

    // printing the best result for each appliance
    bestResultOverAppliancesTrain.foreach(x => println(x))
    bwTrain.write("\n\nBEST RESULTS OF TRAINING SET (over appliances):")
    bestResultOverAppliancesTrain.foreach(x => bwTrain.write(x.toString))

    // storing the results with the best threshold and relative HL for each appliance
//    val temp = bestResultOverAppliancesTrain.toList
    val storeTrain = new ObjectOutputStream(new FileOutputStream(dirNameTrain + "/bestResultsOverAppliances.dat"))
    storeTrain.writeObject(bestResultOverAppliancesTrain)
    storeTrain.close()


    // TEST SET -------------------------------------------------------------------------------------------------------
    val theDirTest = new File(dirNameTest)
    if (!theDirTest.exists()) theDirTest.mkdirs()
    val bwTest: BufferedWriter = new BufferedWriter(new FileWriter(outputTextFilenameTest))
    if (appliancesTest.length != 0) {
      // from now on the code performs the same operations already done
      // create the dataframe with the features from csv (or read it from filesystem depending on the flag readingFromFileLabelDfIngestion)
      val dfFeatureTest = CrossValidation.creatingDfFeatureFixedHouseAndDay(dayFolderTest, house, dirNameTest,
        sc, sqlContext, readingFromFileLabelDfIngestion)
      println("Time for INGESTION: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")


      // 2 RESAMPLING ----------------------------------------------------------------------------------------------------
      val dfFeatureSmoothedTest = Resampling.movingAverageReal(dfFeatureTest, selectedFeature, averageSmoothingWindowSize)
      val dfFeatureResampledTest = Resampling.downsampling(dfFeatureSmoothedTest, downsamplingBinSize)
      // first diff
      val dfFeatureResampledDiffTest = Resampling.firstDifference(dfFeatureResampledTest, selectedFeature, "IDtime")
      val dfFeatureEdgeDetectionTest = dfFeatureResampledDiffTest.cache()
      println("Time for RESAMPLING: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")

      // build prediction for each appliance in the appliancesTest array by using nrThresholdsPerAppliance threshold
      // evenly spaced between 0 and the max value founded
      val outputTextFilenameTest = dirNameTrain + "/outputFileTest.txt"

      val fileOutputTest = new File(outputTextFilenameTest)

      bwTest.write("TEST SET loop over appliances (and thresholds):")
      val bestResultOverAppliancesTest = EdgeDetection.buildPredictionRealFeatureLoopOverAppliances(dfFeatureEdgeDetectionTest,
        dfEdgeSignatures,
        dfTaggingInfoTest,
        appliancesTest,
        selectedFeature,
        timestepsNumberPreEdge, timestepsNumberPostEdge,
        downsamplingBinPredictionSize,
        nrThresholdsPerAppliance,
        partitionNumber,
        scoresONcolName, scoresOFFcolName,
        dirNameTest, bwTest, downsamplingBinPredictionSec,
        sc: SparkContext, sqlContext: SQLContext)

      dfTaggingInfoTest.unpersist()

      // printing the best result for each appliance
      bestResultOverAppliancesTest.foreach(x => println(x))

      bwTest.write("RESULTS OF TEST SET, bestResultOverAppliances:")
      bestResultOverAppliancesTest.foreach(x => bwTest.write(x.toString + "\n"))

      // storing the results with the best threshold and relative HL for each appliance
      //    val temp2: List[(Int, String, SelFeatureType, SelFeatureType)] = bestResultOverAppliancesTest.toList
      val storeTest = new ObjectOutputStream(new FileOutputStream(dirNameTest + "/bestResultsOverAppliances.dat"))
      storeTest.writeObject(bestResultOverAppliancesTest)
      storeTest.close()
      //------------------------------------------------------------------------------------------------------------------
    }
    else
      {println("TEST CANNOT BE PERFORM BECAUSE THERE ARE NO AVAILABLE APPLIANCES")
        bwTest.write("TEST CANNOT BE PERFORM BECAUSE THERE ARE NO AVAILABLE APPLIANCES")}

    //chiusura
    bwTest.close()
    bwTrain.close()
  }
}





