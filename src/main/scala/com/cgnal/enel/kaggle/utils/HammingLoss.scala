package com.cgnal.enel.kaggle.utils

import java.io.{FileInputStream, ObjectInputStream}
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



  def evaluateHammingLossSensPrec(dfEdgeScores: DataFrame,
                                  dfGroundTruth: DataFrame,
                                  groundTruthColName: String,
                                  scoresONcolName: String,
                                  scoresOFFcolName: String,
                                  timeStampColName: String,
                                  applianceID: Int,
                                  thresholdON: Double,
                                  absolutethresholdOFF: Double,
                                  downsamplingBinPredictionSec: Int,
                                  onOffOutputDirName: String) = {

    println("evaluate HAMMING LOSS for appliance: " + applianceID.toString + " with thresholdON: " + thresholdON.toString +
      " thresholdOFF: " + absolutethresholdOFF.toString)

    val onOffWindows: Array[(Long, Long)] =
      SimilarityScore.findOnOffIntervals(
        dfEdgeScores, thresholdON, absolutethresholdOFF,
        scoresONcolName, scoresOFFcolName, timeStampColName)

    val outputFilename = onOffOutputDirName+ "/OnOffArray_AppID" +
      applianceID.toString + "_thresholdON" + (thresholdON*1E7).toInt.toString + ".txt"

    val stringOnOff: String = onOffWindows.mkString("\n")
    Files.write(Paths.get(outputFilename), stringOnOff.getBytes(StandardCharsets.UTF_8))


    val predictionsDf = addOnOffStatusToDF(dfEdgeScores,onOffWindows,
      timeStampColName, "prediction", downsamplingBinPredictionSec)

    val df = dfGroundTruth.join(predictionsDf, Seq(timeStampColName), "left_outer")

    val predictionXORgroundTruth =
      df.withColumn("XOR", (df(groundTruthColName) !== df("prediction")).cast(IntegerType))

    val hammingLoss: DataFrame =
      predictionXORgroundTruth
        .agg(sum("XOR"))

    val HL: Double = hammingLoss.head().getLong(0) / dfGroundTruth.count().toDouble

    // computing Precision and Sensitivity
    val Positive: Int = dfGroundTruth.agg(sum(groundTruthColName)).head.getAs[Int](0)
    val dfTP = df.filter(df("prediction") === 1 && df(groundTruthColName) === 1)
    val TP = dfTP.agg(sum("prediction")).head.getAs[Int](0)

    val dfFN = df.filter(df("prediction") === 0 && df(groundTruthColName) === 1)
    val FN = dfFN.agg(sum("prediction")).head.getAs[Int](0)

    val dfFP = df.filter(df("prediction") === 1 && df(groundTruthColName) === 0)
    val FP = dfFP.agg(sum("prediction")).head.getAs[Int](0)

    val sensitivity = TP.toDouble/(TP+FN)
    val precision = TP.toDouble/(TP+FP)

    println("current hamming loss: %.2f, sensitivity: %.2f, precision: %.2f", HL.toString, sensitivity, precision)

    (HL, sensitivity, precision)


  }



  /***
    *
    * @param resultsOverAppliances Array(applianceID, applianceName, ((thresholdON, thesholdOFF), (hammingLoss, sensitivity, precision)), hammingLoss0Model)
    * @return
    */

  def extractingHLoverThresholdAndAppliances(resultsOverAppliances: Array[(Int, String, Array[((Double, Double), (Double, Double, Double))], Double)])
  = {

    val resultsOverAppliancesNot0Model = resultsOverAppliances.map(tuple =>
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

    val HLtotal = bestResultOverAppliances.map(tuple => tuple._5).reduce(_+_)/bestResultOverAppliances.length
    val HLalways0Total = bestResultOverAppliances.map(tuple => tuple._6).reduce(_+_)/bestResultOverAppliances.length

    val sensitivityTotal = bestResultOverAppliances.map(tuple => tuple._7).reduce(_+_)/bestResultOverAppliances.length
    val precisionTotal = bestResultOverAppliances.map(tuple => tuple._8).reduce(_+_)/bestResultOverAppliances.length

    println("\n\nTotal Sensitivity over appliances: %.3f, Precision: %.3f, HL : %.5f, HL always0Model: %.5f, HL/HL0: %.2f",
      sensitivityTotal, precisionTotal, HLtotal, HLalways0Total, HLtotal/HLalways0Total)

    //(applianceID, applianceName, thresholdON, thesholdOFF, sensitivity, precision, hammingLoss, hammingLoss0Model, hammingLoss/hammingLoss0Model)
    bestResultOverAppliances

  }

}
