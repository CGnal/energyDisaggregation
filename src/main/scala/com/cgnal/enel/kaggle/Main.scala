package com.cgnal.enel.kaggle

import java.nio.charset.StandardCharsets
import java.nio.file.{Paths, Files}
import breeze.numerics.round
import com.cgnal.enel.kaggle.models.edgeDetection.{SimilarityScore, EdgeDetection}
import com.cgnal.enel.kaggle.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

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
    val selectedFeaturePreProcessed = "RealPowerFund_FirstDiff"
    // founded in dfFeature)
    type SelFeatureType = Double

    // -----------------------------------------------------------------------------------------------------------------
    // MOST IMPORTANT PARAMETERS
    // -----------------------------------------------------------------------------------------------------------------
    val averageSmoothingWindowSize = 6 // number of timestamps, unit: [167ms]
    val downsamplingBinSize = 1 // take one point every downsamplingBinSiz timestamps, unit: [167ms]

    val timestampIntervalPreEdge = 5L // time interval amplitude in sec. Note that the sampling bin is [downsamplingBinSize*167ms]
    // it is the width considered BEFORE each ON/OFF event to compute signature
    val timestampIntervalPostEdge = 5L // time interval amplitude in sec. Note that the sampling bin is [downsamplingBinSize*167ms]
    // it is the width considered AFTER each ON/OFF event to compute signature

    val downsamplingBinPredictionSec = 60 // take one point every downsamplingBinPredictionSec sec

    val zeroAndGroundTruthThresholdLabel = 0 // if this is 1 the "nrThresholdsPerAppliance" parameter is not used
    val nrThresholdsPerAppliance = 19 // note that the actual nr of thresholds will be (nrThresholdsPerAppliance + 1)
    // -----------------------------------------------------------------------------------------------------------------
    // -----------------------------------------------------------------------------------------------------------------

    val readingFromFileLabelDfIngestion = 1  // flag to read dfFeature (dataframe with the features) from filesystem (if previously computed)
    val readingFromFileLabelDfPreProcessed = 1
    // instead of building it from csv
    val readingFromFileLabelDfEdgeSignature = 1  // flag to read dfEdgeSignature (dataframe with the ON/OFF signatures)
    // from filesystem (if previously computed) instead of computing it

    val scoresONcolName = "recipMsdON_TimePrediction_" + selectedFeaturePreProcessed
    val scoresOFFcolName = "recipNegMsdOFF_TimePrediction_" + selectedFeaturePreProcessed


    val extraLabelOutputDirName = "RecipMSD"

    //------------------------------------------------------------------------------------------------------------------
    val timestepsNumberPreEdge= (timestampIntervalPreEdge.toInt/(downsamplingBinSize * 0.167)).round.toInt // number of points in the interval
    val timestepsNumberPostEdge = (timestampIntervalPostEdge/(downsamplingBinSize * 0.167)).round.toInt - 1 // number of points in the interval
    val edgeWindowSize = timestepsNumberPreEdge + timestepsNumberPostEdge + 1
    val downsamplingBinPredictionSize: Int = (downsamplingBinPredictionSec/(downsamplingBinSize * 0.167)).round.toInt
    //------------------------------------------------------------------------------------------------------------------

    if (nrThresholdsPerAppliance <= 1) sys.error("When using evenly spaced thresholds nrThresholdsPerAppliance must be >= 2 ")

    // OUTPUT DIR NAME -------------------------------------------------------------------------------------------------
    val dirNameFeatureTrain = ReferencePath.datasetDirPath + house + "/dfFeatureTrain" + dayFolderArrayTraining(0) +
      "_" + dayFolderArrayTraining.last + "_avg" + averageSmoothingWindowSize.toString +
      "_dw" + downsamplingBinSize.toString + "_preInt" + timestampIntervalPreEdge.toString +
      "_postInt" + timestampIntervalPostEdge.toString

    val dirNameFeatureTest = ReferencePath.datasetDirPath + house + "/dfFeatureTest" + dayFolderTest +
      "_avg" + averageSmoothingWindowSize.toString +
      "_dw" + downsamplingBinSize.toString + "_preInt" + timestampIntervalPreEdge.toString +
      "_postInt" + timestampIntervalPostEdge.toString

    val dirNameResultsTrain =
      if (zeroAndGroundTruthThresholdLabel == 1) {
        ReferencePath.datasetDirPath + house + "/Results" + selectedFeaturePreProcessed + "/Train" + extraLabelOutputDirName + "_" + dayFolderArrayTraining(0) +
          "_" + dayFolderArrayTraining.last + "_avg" + averageSmoothingWindowSize.toString +
          "_dw" + downsamplingBinSize.toString + "_preInt" + timestampIntervalPreEdge.toString +
          "_postInt" + timestampIntervalPostEdge.toString + "_dwPrediction" + downsamplingBinPredictionSec.toString + "_0andGTthr"
      }
      else {
        ReferencePath.datasetDirPath + house + "/Results" + selectedFeaturePreProcessed + "/Train" + extraLabelOutputDirName + "_" + dayFolderArrayTraining(0) +
          "_" + dayFolderArrayTraining.last + "_avg" + averageSmoothingWindowSize.toString +
          "_dw" + downsamplingBinSize.toString + "_preInt" + timestampIntervalPreEdge.toString +
          "_postInt" + timestampIntervalPostEdge.toString + "_dwPrediction" + downsamplingBinPredictionSec.toString + "_nrThr" + (nrThresholdsPerAppliance+1).toString
      }

    val dirNameResultsTest =
      if (zeroAndGroundTruthThresholdLabel == 1) {ReferencePath.datasetDirPath + house + "/Results" + selectedFeaturePreProcessed + "/Test" + extraLabelOutputDirName + "_" + dayFolderTest + "_avg" + averageSmoothingWindowSize.toString +
        "_dw" + downsamplingBinSize.toString + "_preInt" + timestampIntervalPreEdge.toString +
        "_postInt" + timestampIntervalPostEdge.toString + "_dwPrediction" + downsamplingBinPredictionSec.toString + "_0andGTthr"
      }
      else{ReferencePath.datasetDirPath + house + "/Results" + selectedFeaturePreProcessed + "/Test" + extraLabelOutputDirName + "_" + dayFolderTest + "_avg" + averageSmoothingWindowSize.toString +
        "_dw" + downsamplingBinSize.toString + "_preInt" + timestampIntervalPreEdge.toString +
        "_postInt" + timestampIntervalPostEdge.toString + "_dwPrediction" + downsamplingBinPredictionSec.toString + "_nrThr" + (nrThresholdsPerAppliance+1).toString
      }

    val outputTextFilenameTraining = dirNameResultsTrain + "/outputFileTraining.txt"
    val outputTextFilenameTest = dirNameResultsTest + "/outputFileTest.txt"
    //------------------------------------------------------------------------------------------------------------------

    // SAVING PRINTED OUTPUT TO A FILE
    // inizializzazione
    val theDirTrain = new File(dirNameResultsTrain)
    if (!theDirTrain.exists()) theDirTrain.mkdirs()
    val fileOutputTrain = new File(outputTextFilenameTraining)
    val bwTrain: BufferedWriter = new BufferedWriter(new FileWriter(fileOutputTrain))


    println("1. INGESTION (from csv to DataFrame)")
    var dateTime = DateTime.now()
    val dfFeatureEdgeDetectionTrain =
      if (readingFromFileLabelDfPreProcessed == 0) {
        // 1 INGESTION -----------------------------------------------------------------------------------------------------
        // TODO inserire ciclo su HOUSE
        // Training set
        // create the dataframe with the features from csv (or read it from filesystem depending on the flag readingFromFileLabelDfIngestion)
        val dfFeatureTrain = CrossValidation.creatingDfFeatureFixedHouseOverDays(dayFolderArrayTraining, house,
          dirNameFeatureTrain, sc, sqlContext, readingFromFileLabelDfIngestion)
        // -----------------------------------------------------------------------------------------------------------------

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
        CSVutils.storingSparkCsv(dfFeatureEdgeDetectionTrain, dirNameFeatureTrain + "/dfFeaturePreProcessed.csv")
        dfFeatureEdgeDetectionTrain
      }

      else {
        sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true") // Use first line of all files as header
          .option("inferSchema", "true")
          .load(dirNameFeatureTrain + "/dfFeaturePreProcessed.csv")
      }

    // TAGGING INFO TRAIN
    // Create the dataframe with the Tagging Info from csv
    val dfTaggingInfoTrainTemp = CrossValidation.creatingDfTaggingInfoFixedHouseOverDays(dayFolderArrayTraining, house,
      sc, sqlContext)
    val dfTaggingInfoTrainTemp2 = dfTaggingInfoTrainTemp.filter(dfTaggingInfoTrainTemp("ApplianceID") <= 38)
    val dfTaggingInfoTrain = dfTaggingInfoTrainTemp2.filter(dfTaggingInfoTrainTemp2("ON_Time") !== dfTaggingInfoTrainTemp2("OFF_Time"))
      .cache()


    // TAGGING INFO TEST
    val dfTaggingInfoTestTemp = CrossValidation.creatingDfTaggingInfoFixedHouseAndDay(dayFolderTest, house,
      sc, sqlContext)
    val dfTaggingInfoTestTemp2 = dfTaggingInfoTestTemp.filter(dfTaggingInfoTestTemp("ApplianceID") <= 38)
    val dfTaggingInfoTest = dfTaggingInfoTestTemp2.filter(dfTaggingInfoTestTemp2("ON_Time") !== dfTaggingInfoTestTemp2("OFF_Time"))
      .cache()

    //------------------------------------------------------------------------------------------------------------------

    // NUMEROSITY
    // CHECK of the number of appliances and edges founded in the TRAINING SET
    val nrEdgesPerApplianceTrain: Map[Int, Int] = dfTaggingInfoTrain.select("ApplianceID").map(row => row.getAs[Int](0)).collect()
      .groupBy(identity).map({ case (x, y) => (x, y.size) })
    val appliancesTrain = nrEdgesPerApplianceTrain.keySet.toArray.sorted
    println("Number of appliances in the training set: " + appliancesTrain.length.toString)
    nrEdgesPerApplianceTrain.foreach({ case (x, y) => println("ApplianceID: " + x.toString + " number of ON/OFF events in the training set: " + y.toString) })

    // CHECK of the number of appliances and edges founded in the TEST SET
    val nrEdgesPerApplianceTest: Map[Int, Int] = dfTaggingInfoTest.filter(dfTaggingInfoTest("ApplianceID").isin(appliancesTrain: _*))
      .select("ApplianceID").map(row => row.getAs[Int](0)).collect()
      .groupBy(identity).map({ case (x, y) => (x, y.size) })
    val appliancesTest = nrEdgesPerApplianceTest.keySet.toArray.sorted
    println("\n\nNumber of appliances in the test (and in the training) set: " + appliancesTest.length.toString)
    nrEdgesPerApplianceTest.foreach({ case (x, y) => println("ApplianceID: " + x.toString + " number of ON/OFF events in the test set: " + y.toString) })

    // -----------------------------------------------------------------------------------------------------------------


    // 3 EDGE DETECTION ALGORITHM --------------------------------------------------------------------------------------
    println("3. EDGE DETECTION ALGORITHM")
    dateTime = DateTime.now()


    val dfEdgeSignaturesFileName = dirNameFeatureTrain + "/onoff_EdgeSignature"
    // Compute edge signature relative to a single (real) feature (for all the appliances)
    // (or read it from filesystem depending on the flag readingFromFileLabelDfEdgeSignature)
    val dfEdgeSignatures =
      if (readingFromFileLabelDfEdgeSignature == 0) {
        EdgeDetection.buildStoreDfEdgeSignature(classOf[SelFeatureType],dfFeatureEdgeDetectionTrain,
          dfTaggingInfoTrain, selectedFeaturePreProcessed, timestampIntervalPreEdge, timestampIntervalPostEdge, edgeWindowSize,
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
      selectedFeaturePreProcessed,
      timestepsNumberPreEdge, timestepsNumberPostEdge,
      downsamplingBinPredictionSize,
      nrThresholdsPerAppliance,
      partitionNumber,
      scoresONcolName, scoresOFFcolName,
      dirNameResultsTrain, bwTrain, downsamplingBinPredictionSec,
      sc, sqlContext,
      zeroAndGroundTruthThresholdLabel)

    dfTaggingInfoTrain.unpersist()

    // printing the best result for each appliance
    //(applianceID, applianceName, thresholdON, thesholdOFF, sensitivity, precision, hammingLoss, hammingLoss0Model, hammingLoss/hammingLoss0Model)
    bestResultOverAppliancesTrain.foreach(x => println(x))
    bwTrain.write("\n\nBEST RESULTS OF TRAINING SET (over appliances):")
    bestResultOverAppliancesTrain.foreach(x => bwTrain.write(x.toString + "\n"))

    // COMPUTING GLOBAL PERFORMANCES OVER APPLIANCES AND PRINT THEM
    val HLtotalTrain = bestResultOverAppliancesTrain.map(tuple => tuple._7).reduce(_+_)/bestResultOverAppliancesTrain.length
    val HLalways0TotalTrain = bestResultOverAppliancesTrain.map(tuple => tuple._8).reduce(_+_)/bestResultOverAppliancesTrain.length

    val sensitivityTotalTrain = bestResultOverAppliancesTrain.map(tuple => tuple._5).reduce(_+_)/bestResultOverAppliancesTrain.length
    val precisionTotalTrain = bestResultOverAppliancesTrain.map(tuple => tuple._6).reduce(_+_)/bestResultOverAppliancesTrain.length

    val pippoTrain = HLtotalTrain/HLalways0TotalTrain
    println(f"\n\nTotal Sensitivity over appliances: $sensitivityTotalTrain%1.3f, Precision: $precisionTotalTrain%1.3f " +
      f"HL : $HLtotalTrain%1.5f, HL always0Model: $HLalways0TotalTrain%1.5f, HL/HL0: $pippoTrain%3.2f\n")

    bwTrain.write(f"\n\nTotal Sensitivity over appliances: $sensitivityTotalTrain%1.3f, Precision: $precisionTotalTrain%1.3f " +
      f"HL : $HLtotalTrain%1.5f, HL always0Model: $HLalways0TotalTrain%1.5f, HL/HL0: $pippoTrain%3.2f\n")


    // storing the results with the best threshold and relative HL for each appliance
    val storeTrain = new ObjectOutputStream(new FileOutputStream(dirNameResultsTrain + "/bestResultsOverAppliances.dat"))
    storeTrain.writeObject(bestResultOverAppliancesTrain)
    storeTrain.close()


    // STORING A CSV WITH RESULTS
    val storeTrainCSV = new PrintWriter(new File(dirNameResultsTrain + "/bestResultsOverAppliances.csv"))
    storeTrainCSV.println("applianceID, applianceName, thresholdON, thesholdOFF, sensitivity, precision, hammingLoss, hammingLoss0Model, hammingLoss/hammingLoss0Model")
    bestResultOverAppliancesTrain.foreach(tuple => storeTrainCSV.println(tuple._1 + "," +
      tuple._2 + "," +
      tuple._3 + "," +
      tuple._4 + "," +
      tuple._5 + "," +
      tuple._6 + "," +
      tuple._7 + "," +
      tuple._8 + "," +
      tuple._9))
    storeTrainCSV.close()




    // TEST SET -------------------------------------------------------------------------------------------------------
    val theDirTest = new File(dirNameResultsTest)
    if (!theDirTest.exists()) theDirTest.mkdirs()

    val bwTest: BufferedWriter = new BufferedWriter(new FileWriter(outputTextFilenameTest))
    if (appliancesTest.length != 0) {
      // from now on the code performs the same operations already done
      // create the dataframe with the features from csv (or read it from filesystem depending on the flag readingFromFileLabelDfIngestion)
      val dfFeatureTest = CrossValidation.creatingDfFeatureFixedHouseAndDay(dayFolderTest, house, dirNameFeatureTest,
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

      bwTest.write("TEST SET loop over appliances (and thresholds):")
      val bestResultOverAppliancesTest = EdgeDetection.buildPredictionRealFeatureLoopOverAppliances(dfFeatureEdgeDetectionTest,
        dfEdgeSignatures,
        dfTaggingInfoTest,
        appliancesTest,
        selectedFeaturePreProcessed,
        timestepsNumberPreEdge, timestepsNumberPostEdge,
        downsamplingBinPredictionSize,
        nrThresholdsPerAppliance,
        partitionNumber,
        scoresONcolName, scoresOFFcolName,
        dirNameResultsTest, bwTest, downsamplingBinPredictionSec,
        sc, sqlContext,
        zeroAndGroundTruthThresholdLabel)

      dfTaggingInfoTest.unpersist()

      // printing the best result for each appliance
      bestResultOverAppliancesTest.foreach(x => println(x))

      bwTest.write("RESULTS OF TEST SET, bestResultOverAppliances:")
      bestResultOverAppliancesTest.foreach(x => bwTest.write(x.toString + "\n"))

      // COMPUTING GLOBAL PERFORMANCES OVER APPLIANCES AND PRINT THEM
      val HLtotalTest = bestResultOverAppliancesTest.map(tuple => tuple._7).reduce(_+_)/bestResultOverAppliancesTest.length
      val HLalways0TotalTest = bestResultOverAppliancesTest.map(tuple => tuple._8).reduce(_+_)/bestResultOverAppliancesTest.length

      val sensitivityTotalTest = bestResultOverAppliancesTest.map(tuple => tuple._5).reduce(_+_)/bestResultOverAppliancesTest.length
      val precisionTotalTest = bestResultOverAppliancesTest.map(tuple => tuple._6).reduce(_+_)/bestResultOverAppliancesTest.length

      val pippoTest = HLtotalTest/HLalways0TotalTest
      println(f"\n\nTotal Sensitivity over appliances: $sensitivityTotalTest%1.3f, Precision: $precisionTotalTest%1.3f " +
        f"HL : $HLtotalTest%1.5f, HL always0Model: $HLalways0TotalTest%1.5f, HL/HL0: $pippoTest%3.2f\n")

      bwTest.write(f"\n\nTotal Sensitivity over appliances: $sensitivityTotalTest%1.3f, Precision: $precisionTotalTest%1.3f " +
        f"HL : $HLtotalTest%1.5f, HL always0Model: $HLalways0TotalTest%1.5f, HL/HL0: $pippoTest%3.2f\n")


      // storing the results with the best threshold and relative HL for each appliance
      val storeTest = new ObjectOutputStream(new FileOutputStream(dirNameResultsTest + "/bestResultsOverAppliances.dat"))
      storeTest.writeObject(bestResultOverAppliancesTest)
      storeTest.close()

      // STORING A CSV WITH RESULTS
      val storeTestCSV = new PrintWriter(new File(dirNameResultsTest + "/bestResultsOverAppliances.csv"))
      storeTestCSV.println("applianceID, applianceName, thresholdON, thesholdOFF, sensitivity, precision, hammingLoss, hammingLoss0Model, hammingLoss/hammingLoss0Model")
      bestResultOverAppliancesTest.foreach(tuple => storeTestCSV.println(tuple._1 + "," +
        tuple._2 + "," +
        tuple._3 + "," +
        tuple._4 + "," +
        tuple._5 + "," +
        tuple._6 + "," +
        tuple._7 + "," +
        tuple._8 + "," +
        tuple._9))
      storeTestCSV.close()
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





