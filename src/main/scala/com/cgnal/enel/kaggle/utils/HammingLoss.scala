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



  def evaluateHammingLoss(
                           dfEdgeScores: DataFrame,
                           dfGroundTruth: DataFrame,
                           groundTruthColName: String,
                           scoresONcolName: String,
                           scoresOFFcolName: String,
                           timeStampColName: String,
                           applianceID: Int,
                           thresholdON: Double,
                           absolutethresholdOFF: Double,
                           downsamplingBinPredictionSec: Int,
                           onOffOutputDirName: String
                         ) = {

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

    val pippo: Double = hammingLoss.head().getLong(0) / dfGroundTruth.count().toDouble

    println("current hamming loss: " + pippo.toString)

    pippo
  }




  def extractingHLoverThresholdAndAppliances(resultsOverAppliances: Array[(Int, String, Array[((Double, Double), Double)], Double)])
  : Array[(Int, String, Double, Double, Double, Double)] = {

    val bestResultOverAppliances: Array[(Int, String, Double, Double, Double, Double)] = resultsOverAppliances.map(tuple => {
      val bestThresholdHL: ((Double, Double), Double) = tuple._3.minBy(_._2)
      val bestThresholdON = bestThresholdHL._1._1
      val bestThresholdOFF = bestThresholdHL._1._2
      val bestHL = bestThresholdHL._2

      (tuple._1, tuple._2, bestThresholdON, bestThresholdOFF, bestHL, tuple._4)
    })

    val HLtotal = bestResultOverAppliances.map(tuple => tuple._5).reduce(_+_)/bestResultOverAppliances.length

    val HLalways0Total = bestResultOverAppliances.map(tuple => tuple._6).reduce(_+_)/bestResultOverAppliances.length

    println("\n\nTotal HL over appliances: " + HLtotal.toString + " HL always0Model: " + HLalways0Total.toString)

    bestResultOverAppliances

  }

}
