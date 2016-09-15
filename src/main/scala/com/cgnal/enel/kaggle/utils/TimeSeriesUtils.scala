package com.cgnal.efm.predmain.uta.timeseries

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{avg, max, min, sum}
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable.ArrayBuffer

/**
  * Created by riccardo.satta on 12/09/16.
  */
object TimeSeriesUtils {

  /***
    * Finds positive peaks on a timeSeries (implements the homonym in Matlab)
    *
    * @param df
    * @param absoluteThreshold
    * @param seriesColName
    * @param timeStampColName
    * @return
    */
  private def findPositivePeaks(
                                 df: DataFrame,
                                 absoluteThreshold: Double,
                                 seriesColName: String,
                                 timeStampColName: String): DataFrame = {

    val w: WindowSpec = Window
      .orderBy(timeStampColName)
      .rangeBetween(-1,1)

    val df2 = df
      .withColumn(
        seriesColName + "_localMax",
        max(seriesColName).over(w))
      .withColumn(
        seriesColName + "_localAvg",
        avg(seriesColName).over(w))

    val df3 = df2
      .filter(df2(seriesColName) === df2(seriesColName + "_localMax"))

    val df4 = df3
      .filter(df3(seriesColName + "_localMax") !== df3(seriesColName + "_localAvg"))
      .select(timeStampColName, seriesColName)

    df4.filter(df4(seriesColName)>absoluteThreshold)
  }

  /***
    * Finds negative peaks on a timeSeries (implements the homonym in Matlab)
    *
    * @param df
    * @param absoluteThreshold
    * @param seriesColName
    * @param timeStampColName
    * @return
    */
  private def findNegativePeaks(
                                 df: DataFrame,
                                 absoluteThreshold: Double,
                                 seriesColName: String,
                                 timeStampColName: String): DataFrame = {

    val w = Window
      .orderBy(timeStampColName)
      .rangeBetween(-1,1)

    val df2 = df
      .withColumn(
        seriesColName + "_localMin",
        min(seriesColName).over(w))
      .withColumn(
        seriesColName + "_localAvg",
        avg(seriesColName).over(w))

    val df3 = df2
      .filter(df2(seriesColName) === df2(seriesColName + "_localMin"))

    val df4 = df3
      .filter(df3(seriesColName + "_localMin") !== df3(seriesColName + "_localAvg"))
      .select(timeStampColName, seriesColName)

    df4.filter(df4(seriesColName)<absoluteThreshold)
  }


  private def findOnOffIntervals(
                                  dfEdgeScores: DataFrame,
                                  absoluteThreshold: Double,
                                  valuesColName: String,
                                  timeStampColName: String): Array[(Long, Long)] = {
    //find positive peaks
    val dfPositivePeaks = findPositivePeaks(
      dfEdgeScores, absoluteThreshold, valuesColName, timeStampColName)
      .withColumnRenamed(valuesColName, "PositivePeak")
      .sort(timeStampColName)

    //find negative peaks
    val dfNegativePeaks = findNegativePeaks(
      dfEdgeScores, absoluteThreshold, valuesColName, timeStampColName)
      .withColumnRenamed(valuesColName, "NegativePeak")
      .sort(timeStampColName)

    var positivePeaks: Array[(Long, Double)] = dfPositivePeaks.map(row => {
      val timeStamp = row.getLong(row.fieldIndex(timeStampColName))
      val peak = row.getDouble(row.fieldIndex("PositivePeak"))
      (timeStamp, peak)
    }).collect().sortBy(_._1)

    if (positivePeaks.isEmpty)
      Array[(Long, Long)]()
    else {
      positivePeaks = positivePeaks :+(Long.MaxValue, Double.MaxValue)

      val negativePeaks = dfNegativePeaks.map(row => {
        val timeStamp = row.getLong(row.fieldIndex(timeStampColName))
        val peak = row.getDouble(row.fieldIndex("NegativePeak"))
        (timeStamp, peak)
      }).collect().sortBy(_._1)

      val OnOffwindows = ArrayBuffer.empty[(Long, Long)]

      do {
        //if there are elements in negativePeaks between positivePeaks[i]
        //and positivePeaks[i+1], take them and find the maximum value;
        //else compare positivePeaks[i] and positivePeaks[i+1], take the
        //maximum and go on
        val timeStampPositivePeakI = positivePeaks.head._1
        val timeStampPositivePeakIplus1 = positivePeaks.tail.head._1

        val negativePeaksBetweenPositivePeaks =
          negativePeaks.filter(t =>
            t._1 > timeStampPositivePeakI && t._1 < timeStampPositivePeakIplus1)

        if (negativePeaksBetweenPositivePeaks.length > 0) {
          OnOffwindows.append((
            positivePeaks.head._1,
            negativePeaksBetweenPositivePeaks.minBy(_._2)._1
            ))
          positivePeaks = positivePeaks.tail
        } else {
          val tmpMaxTuple: (Long, Double) = Array(positivePeaks.head, positivePeaks.tail.head).maxBy(_._2)

          positivePeaks = Array(tmpMaxTuple) ++ positivePeaks.drop(2)
        }
      } while (positivePeaks.length > 1)

      OnOffwindows.toArray
    }
  }

  private def buildPrediction(dfEdgeScores: DataFrame,
                              absoluteThreshold: Double,
                              valuesColName: String,
                              timeStampColName: String): DataFrame = {
    val onOffWindows: Array[(Long, Long)] =
      findOnOffIntervals(
        dfEdgeScores, absoluteThreshold, valuesColName, timeStampColName)

    val firstTwoRows = dfEdgeScores.sort(timeStampColName).select(timeStampColName).take(2)

    val firstRow = firstTwoRows.head
    val secondRow = firstTwoRows.last

    val step = secondRow.getLong(0) - firstRow.getLong(0)
    val predictionRanges: Array[Long] =
      onOffWindows.flatMap(tuple => tuple._1 to tuple._2 by step)



    val predictionRangesBC =
      dfEdgeScores.sqlContext.sparkContext
        .broadcast[Array[Long]](predictionRanges)

    //build prediction
    val df = dfEdgeScores
      .withColumn("prediction",
        dfEdgeScores(timeStampColName)
          .isin(predictionRangesBC.value:_*).cast(IntegerType))

    df.select(timeStampColName, "prediction")

  }


  def addOnOffRangesToDF(
                          df: DataFrame,
                          timeStampColName: String,
                          dfOnOffWindows: DataFrame,
                          applianceId: Int,
                          applianceIdColName: String,
                          onColName: String,
                          offColName: String,
                          newColName: String) = {

    val onOffWindows: Array[(Long, Long)] = dfOnOffWindows
      .filter(dfOnOffWindows(applianceIdColName) === applianceId)
      .select(onColName, offColName).map(r => (r.getLong(0), r.getLong(1))).collect()

    val firstTwoRows = df.sort(timeStampColName).select(timeStampColName).take(2)

    val firstRow = firstTwoRows.head
    val secondRow = firstTwoRows.last

    val step = secondRow.getLong(0) - firstRow.getLong(0)
    val predictionRanges: Array[Long] =
      onOffWindows.flatMap(tuple => tuple._1 to tuple._2 by step)

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
                           nrOfAppliances: Int,
                           threshold: Double
                         ) = {

    val predictionsDf: DataFrame = buildPrediction(
      dfEdgeScores, threshold, scoresColName, timeStampColName)

    val df = dfGroundTruth.join(predictionsDf, Seq(timeStampColName), "left_outer")

    val predictionXORgroundTruth =
      df.withColumn("XOR", (df(groundTruthColName) !== df("prediction")).cast(IntegerType))

    val hammingLoss =
      predictionXORgroundTruth
        .agg(sum("XOR"))

    val pippo = hammingLoss.head().getLong(0) / (dfGroundTruth.count() * nrOfAppliances).toDouble

    pippo
  }

  def hammingLossCurve(
                        dfEdgeScores: DataFrame,
                        dfGroundTruth: DataFrame,
                        groundTruthColName: String,
                        scoresColName: String,
                        timeStampColName: String,
                        nrOfAppliances: Int
                      ) = {

    val thresholds: Array[Double] =
      dfEdgeScores
        .select(scoresColName)
        .collect().map(_.getDouble(0).abs).sorted


    val hammingLosses: Array[Double] = thresholds.map(
      threshold =>
        evaluateHammingLoss(
          dfEdgeScores, dfGroundTruth,
          groundTruthColName, scoresColName,
          timeStampColName, nrOfAppliances, threshold)
    )

    thresholds.zip(hammingLosses)
  }
}
