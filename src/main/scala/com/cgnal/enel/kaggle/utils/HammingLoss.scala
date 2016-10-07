package com.cgnal.enel.kaggle.utils

import java.io.{BufferedWriter, FileInputStream, ObjectInputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Paths, Files}

import com.cgnal.enel.kaggle.models.edgeDetection.SimilarityScore
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
  * Created by cavaste on 22/09/16.
  */
object HammingLoss {


  /***
    *
    * @param df
    * @param onOffWindows
    * @param timeStampColName
    * @param newColName
    * @param downsamplingBinPredictionSec
    * @param timestampFactor
    * @return
    */
  def addOnOffStatusToDF(
                          df: DataFrame,
                          onOffWindows: Array[(Long, Long)],
                          timeStampColName: String,
                          newColName: String,
                          downsamplingBinPredictionSec: Int,
                          timestampFactor: Double = 1E7): DataFrame = {

    val predictionRanges: Array[Long] =
      onOffWindows.flatMap(tuple => {
        df.filter((df(timeStampColName) >= tuple._1 && df(timeStampColName) <= tuple._2) ||
          abs(df(timeStampColName) - tuple._1) < downsamplingBinPredictionSec*timestampFactor.toInt &&
            abs(df(timeStampColName) - tuple._2) < downsamplingBinPredictionSec*timestampFactor.toInt)
            .select(timeStampColName)
          .collect()
          .map(rowTime => {
            rowTime.getAs[Long](timeStampColName)
          })
      })

    val predictionRangesBC =
      df.sqlContext.sparkContext
        .broadcast[Array[Long]](predictionRanges)

    //build prediction
    df.withColumn(newColName,
      df(timeStampColName)
        .isin(predictionRangesBC.value:_*).cast(IntegerType))
  }



  def evaluateHammingLossSensPrecAndThreshold(dfEdgeScoresGroundTruth: DataFrame,
                                              groundTruthColName: String,
                                              scoresONcolName: String,
                                              scoresOFFcolName: String,
                                              timeStampColName: String,
                                              applianceID: Int,
                                              thresholdON: Double,
                                              thresholdOFF: Double,
                                              downsamplingBinPredictionSec: Int,
                                              onOffOutputDirName: String) = {

    println("evaluate HAMMING LOSS for appliance: " + applianceID.toString + " with thresholdON: " + thresholdON.toString +
      " thresholdOFF: " + thresholdOFF.toString)

    val onOffWindows: Array[(Long, Long)] =
      SimilarityScore.findOnOffIntervals(
        dfEdgeScoresGroundTruth, thresholdON, thresholdOFF,
        scoresONcolName, scoresOFFcolName, timeStampColName)

    val outputFilename = onOffOutputDirName+ "/OnOffArray_AppID" +
      applianceID.toString + "_thresholdON" + (thresholdON*1E7).toInt.toString + ".txt"

    val stringOnOff: String = onOffWindows.mkString("\n")
    Files.write(Paths.get(outputFilename), stringOnOff.getBytes(StandardCharsets.UTF_8))


    val dfPredictionsGroundTruth = addOnOffStatusToDF(dfEdgeScoresGroundTruth,onOffWindows,
      timeStampColName, "prediction", downsamplingBinPredictionSec).cache()

    val numberSteps = dfPredictionsGroundTruth.count()

    val predictionXORgroundTruth =
      dfPredictionsGroundTruth.withColumn("XOR", (dfPredictionsGroundTruth(groundTruthColName) !== dfPredictionsGroundTruth("prediction")).cast(IntegerType))

    val hammingLoss: DataFrame =
      predictionXORgroundTruth
        .agg(sum("XOR"))

    val HL: Double = hammingLoss.head().getLong(0).toDouble / numberSteps

    // computing Precision and Sensitivity
    val Positive = dfPredictionsGroundTruth.agg(sum(groundTruthColName)).head.getAs[Long](0)
    val dfTP = dfPredictionsGroundTruth.filter(dfPredictionsGroundTruth("prediction") === 1 && dfPredictionsGroundTruth(groundTruthColName) === 1)
    val TP = dfTP.agg(sum("prediction")).head.getAs[Long](0)

    val detectedPositive = dfPredictionsGroundTruth.agg(sum("prediction")).head.getAs[Long](0)

    val pluto = if (TP == 0) (0d,0d)
    else (TP.toDouble/Positive, TP.toDouble/detectedPositive)

    val sensitivity = pluto._1
    val precision = pluto._2

    println(f"current hamming loss: $HL%1.5f, sensitivity: $sensitivity%1.6f, precision: $precision%1.6f;" +
      f" [TP: $TP, DETECTED POSITIVE: $detectedPositive, POSITIVE: $Positive, ALL STEPS: $numberSteps]")

    //extracting threshold
    val minThresholdONtp = dfTP.agg(min(scoresONcolName)).head.getAs[Double](0)
    val maxThresholdOFFtp = dfTP.agg(max(scoresOFFcolName)).head.getAs[Double](0)

    if (maxThresholdOFFtp > 0 || minThresholdONtp < 0) sys.error("thresholdOFF must be negative and thresholdOFF positive")

    (HL, sensitivity, precision, (minThresholdONtp, maxThresholdOFFtp))

  }


  def evaluateHammingLossSensPrec(dfEdgeScoresGroundTruth: DataFrame,
                                  groundTruthColName: String,
                                  scoresONcolName: String,
                                  scoresOFFcolName: String,
                                  timeStampColName: String,
                                  applianceID: Int,
                                  thresholdON: Double,
                                  thresholdOFF: Double,
                                  downsamplingBinPredictionSec: Int,
                                  onOffOutputDirName: String) = {

    println("evaluate HAMMING LOSS for appliance: " + applianceID.toString + " with thresholdON: " + thresholdON.toString +
      " thresholdOFF: " + thresholdOFF.toString)

    val onOffWindows: Array[(Long, Long)] =
      SimilarityScore.findOnOffIntervals(
        dfEdgeScoresGroundTruth, thresholdON, thresholdOFF,
        scoresONcolName, scoresOFFcolName, timeStampColName)

    val outputFilename = onOffOutputDirName+ "/OnOffArray_AppID" +
      applianceID.toString + "_thresholdON" + (thresholdON*1E7).toInt.toString + ".txt"

    val stringOnOff: String = onOffWindows.mkString("\n")
    Files.write(Paths.get(outputFilename), stringOnOff.getBytes(StandardCharsets.UTF_8))


    val dfPredictionsGroundTruth = addOnOffStatusToDF(dfEdgeScoresGroundTruth,onOffWindows,
      timeStampColName, "prediction", downsamplingBinPredictionSec)

    val numberSteps = dfPredictionsGroundTruth.count()

    val predictionXORgroundTruth =
      dfPredictionsGroundTruth.withColumn("XOR", (dfPredictionsGroundTruth(groundTruthColName) !== dfPredictionsGroundTruth("prediction")).cast(IntegerType))

    val hammingLoss: DataFrame =
      predictionXORgroundTruth
        .agg(sum("XOR"))

    val HL: Double = hammingLoss.head().getLong(0).toDouble / numberSteps

    // computing Precision and Sensitivity
    val Positive = dfPredictionsGroundTruth.agg(sum(groundTruthColName)).head.getAs[Long](0)
    val dfTP = dfPredictionsGroundTruth.filter(dfPredictionsGroundTruth("prediction") === 1 && dfPredictionsGroundTruth(groundTruthColName) === 1)
    val TP = dfTP.agg(sum("prediction")).head.getAs[Long](0)

    val detectedPositive = dfPredictionsGroundTruth.agg(sum("prediction")).head.getAs[Long](0)

    val pluto = if (TP == 0) (0d,0d)
    else (TP.toDouble/Positive, TP.toDouble/detectedPositive)

    val sensitivity = pluto._1
    val precision = pluto._2

    println(f"current hamming loss: $HL%1.5f, sensitivity: $sensitivity%1.6f, precision: $precision%1.6f;" +
      f" [TP: $TP, DETECTED POSITIVE: $detectedPositive, POSITIVE: $Positive, ALL STEPS: $numberSteps]")


    (HL, sensitivity, precision)

  }



  /***
    *
    * @param resultsOverAppliances Array(applianceID, applianceName, ((thresholdON, thesholdOFF), (hammingLoss, sensitivity, precision)), hammingLoss0Model)
    * @return
    */

  def extractingPerfOverThresholdAndAppliances(resultsOverAppliances: Array[(Int, String, Array[((Double, Double), (Double, Double, Double))], Double)])
  = {

    val resultsOverAppliancesNot0Model: Array[(Int, String, Array[((Double, Double), (Double, Double, Double))], Double)] = resultsOverAppliances.map(tuple =>
      (tuple._1, tuple._2, tuple._3.filter(tuple2 => tuple2._2._1 != tuple._4), tuple._4))

    val bestResultOverAppliances = resultsOverAppliancesNot0Model.map(tuple => {

      val bestThresholdHL: ((Double, Double), (Double, Double, Double)) = tuple._3.minBy(_._2._1)
      val bestThresholdON = bestThresholdHL._1._1
      val bestThresholdOFF = bestThresholdHL._1._2
      val bestHL = bestThresholdHL._2._1
      val bestSensitivity = bestThresholdHL._2._2
      val bestPrecision = bestThresholdHL._2._3

      (tuple._1, tuple._2, bestThresholdON, bestThresholdOFF, bestSensitivity, bestPrecision, bestHL, tuple._4, bestHL/tuple._4)
    })

    //(applianceID, applianceName, thresholdON, thesholdOFF, sensitivity, precision, hammingLoss, hammingLoss0Model, hammingLoss/hammingLoss0Model)
    bestResultOverAppliances

  }

}
