package com.cgnal.enel.kaggle.models.edgeDetection

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util
import java.util.Collections

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{avg, max, min, sum}
import org.apache.spark.sql.types.IntegerType

import scala.collection.JavaConverters._
import scala.collection.breakOut
import scala.collection.mutable.ArrayBuffer

/**
  * Created by riccardo.satta on 12/09/16.
  */
object SimilarityScore {

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
      .orderBy("IDscoreDownsampling")
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
      .orderBy("IDscoreDownsampling")
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


  def findOnOffIntervals(
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






  def extractingThreshold(dfEdgeScores: DataFrame,
                          scoresColName: String,
                          nrOfThresholds: Int): Array[Double] = {

    val thresholds: Array[Double] =
      dfEdgeScores
        .select(scoresColName)
        .collect().map(_.getDouble(0).abs).sorted

    println("number of available thresholds: " + thresholds.length.toString)


    val thresholdToTest: util.List[Double] = java.util.Arrays.asList(thresholds:_*)
    Collections.shuffle(thresholdToTest)

    val thresholdToTestArray: Array[Double] = thresholdToTest.asScala.map(_.doubleValue)(breakOut).toArray

    val thresholdToTestSorted: Array[Double] = thresholdToTestArray.take(nrOfThresholds).sorted

    thresholdToTestSorted
  }

}
