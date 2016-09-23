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
                          newColName: String): DataFrame = {

    val predictionRanges: Array[Long] =
      onOffWindows.flatMap(tuple => {
        df.filter(df(timeStampColName) >= tuple._1 && df(timeStampColName) <= tuple._2).select(timeStampColName)
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
                           scoresColName: String,
                           timeStampColName: String,
                           applianceID: Int,
                           threshold: Double,
                           onOffOutputDirName: String
                         ): Double = {

    println("evaluate HAMMING LOSS for appliance: " + applianceID.toString + " with threshold: " + threshold.toString)

    val onOffWindows: Array[(Long, Long)] =
      SimilarityScore.findOnOffIntervals(
        dfEdgeScores, threshold, scoresColName, timeStampColName)

    val outputFilename = onOffOutputDirName+ "/OnOffArray_AppID" +
      applianceID.toString + "_threshold" + (threshold*1E7).toInt.toString +".txt"

    val stringOnOff: String = onOffWindows.mkString("\n")
    Files.write(Paths.get(outputFilename), stringOnOff.getBytes(StandardCharsets.UTF_8))


    val predictionsDf = addOnOffStatusToDF(dfEdgeScores,onOffWindows,
      timeStampColName, "prediction")

    val df = dfGroundTruth.join(predictionsDf, Seq(timeStampColName), "left_outer")

    val predictionXORgroundTruth =
      df.withColumn("XOR", (df(groundTruthColName) !== df("prediction")).cast(IntegerType))

    val hammingLoss =
      predictionXORgroundTruth
        .agg(sum("XOR"))

    val pippo: Double = hammingLoss.head().getLong(0) / dfGroundTruth.count().toDouble

    println("current hamming loss: " + pippo.toString)

    pippo
  }




  def extractingHLoverThresholdAndAppliances(filenameResultsOverAppliances: String): Array[(Int, String, Double, Double)] = {

    val reader = new ObjectInputStream(new FileInputStream(filenameResultsOverAppliances))
    val resultsOverAppliances = reader.readObject().asInstanceOf[Array[(Int, String, Array[(Double, Double)])]]
    reader.close()


    val bestResultOverAppliances: Array[(Int, String, Double, Double)] = resultsOverAppliances.map(tuple => {
      val bestThresholdHL: (Double, Double) = tuple._3.minBy(_._2)

      (tuple._1, tuple._2, bestThresholdHL._1, bestThresholdHL._2)
    })

    val HLtotal = bestResultOverAppliances.map(tuple => tuple._4).reduce(_+_)/bestResultOverAppliances.length

    println("\n\nTotal HL over appliances: " + HLtotal.toString)

    bestResultOverAppliances

  }

}
