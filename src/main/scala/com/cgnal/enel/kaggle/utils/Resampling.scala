package com.cgnal.enel.kaggle.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{avg,max,min}


/**
  * Created by cavaste on 15/09/16.
  */
object Resampling {

  def movingAverageReal(df: DataFrame,
                        selectedFeature: String,
                        slidingWindowSize: Int,
                        TimeStamp_ColName: String = "IDtime"): DataFrame ={

    val w: WindowSpec = Window
      .orderBy(TimeStamp_ColName)
      .rangeBetween(0,slidingWindowSize-1)

    val df2 = df
      .withColumn(
        selectedFeature,
        avg(selectedFeature).over(w))
    df2
  }


  def downsampling(df: DataFrame, downsamplingBinSize: Int): DataFrame = {
    // extrapolate indexes
    val idxBinTimeZero = 0
    val dfDownsampled = df.filter(((df("IDtime") - idxBinTimeZero) % downsamplingBinSize) === 0)

    dfDownsampled.withColumn("IDtime", dfDownsampled("IDtime")/downsamplingBinSize)
  }


  def edgeScoreDownsampling(df: DataFrame, selectedFeature: String, downsamplingBinSize: Int) = {

    val dfIDscoreDownsampling = df.withColumn("IDscoreDownsampling", df("IDtime")/downsamplingBinSize)

    val dfScoreDownsampled: DataFrame = dfIDscoreDownsampling.groupBy("IDscoreDownsampling")
      .agg(min(df("Timestamp")).as("TimestampPrediction"),
        max(df("nmsdON_Time_" + selectedFeature)).as("nmsdON_TimePrediction_" + selectedFeature),
        max(df("nmsdOFF_Time_" + selectedFeature)).as("nmsdOFF_TimePrediction_" + selectedFeature))

    // Delta Score in [-2,2]
    val dfFeatureEdgeScoreAppliancePrediction = dfScoreDownsampled.withColumn(
      "DeltaScorePrediction_" + selectedFeature, dfScoreDownsampled("nmsdON_TimePrediction_" + selectedFeature)
        - dfScoreDownsampled("nmsdOFF_TimePrediction_" + selectedFeature)
    )

    dfFeatureEdgeScoreAppliancePrediction

  }





}
