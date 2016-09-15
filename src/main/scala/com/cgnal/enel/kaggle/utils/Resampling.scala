package com.cgnal.enel.kaggle.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._

/**
  * Created by aagostinelli on 15/09/16.
  */
object Resampling {

  def movingAverage(df: DataFrame,
                    harmonics_ColName: String,
                    slidingWindow: Int,
                    TimeStamp_ColName: String): DataFrame ={

    val w: WindowSpec = Window
      .orderBy(TimeStamp_ColName)
      .rangeBetween(0,slidingWindow-1)

    val df2 = df
      .withColumn(
        harmonics_ColName + "_localAvg",
        avg(harmonics_ColName).over(w))
    df2
  }

  def downSampling(df: DataFrame, samplingBinWindow: Int): DataFrame ={
    // extrapolate indexes
    //df.show()
    val idxBinTimeZero = 0
    df.filter(((df("IDtime") - idxBinTimeZero) % samplingBinWindow)===0)
  }


}
