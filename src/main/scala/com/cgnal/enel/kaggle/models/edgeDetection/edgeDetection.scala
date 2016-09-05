package com.cgnal.enel.kaggle.models.edgeDetection

import java.security.Timestamp
import java.sql

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, Column, DataFrame, Row}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection

/**
  * Created by cavaste on 01/09/16.
  */
object edgeDetection {


  def selectingEdgeWindows(dfFeatures: DataFrame, dfTaggingInfo: DataFrame,
                           selectedFeature: String, timestampIntervalPreEdge: Double, timestampIntervalPostEdge: Double,
                           sc: SparkContext, sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._

    val s: Array[Row] = dfTaggingInfo.collect()

    // extract the windows of feature for each ON and OFF event
    val featureWindows: Array[Row] = s.map(row => {
      val onTime = row.getAs[Double]("ON_Time")
      val featureWindowOn: Array[collection.Map[String, Double]] = dfFeatures.filter($"Timestamp".gt(onTime - timestampIntervalPreEdge) &&
        $"Timestamp".lt(onTime + timestampIntervalPreEdge)).orderBy("Timestamp")
        .map(r => r.getMap[String,Double](r.fieldIndex(selectedFeature))).collect()

      val offTime = row.getAs[Double]("OFF_Time")
      val featureWindowOff: Array[collection.Map[String, Double]] = dfFeatures.filter($"Timestamp".gt(offTime - timestampIntervalPreEdge) &&
        $"Timestamp".lt(offTime + timestampIntervalPreEdge)).orderBy("Timestamp")
        .map(r => r.getMap[String,Double](r.fieldIndex(selectedFeature))).collect()

      val idEvent = row.getAs[Int]("IDevent")
      Row(idEvent, featureWindowOn, featureWindowOff)
    })

    // check the length of each window of feature
    val windowSize: Int = featureWindows(0).getAs[Array[collection.Map[String, Double]]](1).size

    featureWindows.foreach(row => {
      if (row.getAs[Array[collection.Map[String, Double]]](1).size != windowSize ||
        row.getAs[Array[collection.Map[String, Double]]](2).size != windowSize) sys.error("feature windows length are not equal")
    })

    // create the dataframe
    val RDDFeatureWindows = sc.parallelize(featureWindows)

    val featureWindowsSchema: StructType =
      StructType(
        StructField("IDevent", IntegerType, false) ::
          StructField("ON_TimeWindow_"+selectedFeature, ArrayType(DoubleType), false) ::
          StructField("OFF_TimeWindow_"+selectedFeature, ArrayType(DoubleType), false) :: Nil)

    val dfFeatureWindows: DataFrame = sqlContext.createDataFrame(RDDFeatureWindows, featureWindowsSchema)
    dfFeatureWindows


/*    val onTimeArray: Column = dfTaggingInfo("ON_Time")

    val dfTimestampsEdge: DataFrame = dfTaggingInfo.select("ON_Time", "OFF_time").flatMap{ row =>
      val onTime = row.getTimestamp(0).toString.toDouble
      val infOn = (onTime - timestampIntervalPreEdge - 1).toInt
      val supOn = (onTime + timestampIntervalPostEdge + 1).toInt
      val onRange = Range(infOn, supOn).toList


      val offTime = row.getTimestamp(1).toString.toDouble
      val infOff = (offTime - timestampIntervalPreEdge - 1).toInt
      val supOff = (offTime + timestampIntervalPostEdge + 1).toInt
      val offRange = Range(infOff, supOff).toList

      onRange ++ offRange
    }.distinct.toDF("TimestampsEdge")

    val dfFeaturesShort: DataFrame = dfFeatures.join(dfTimestampsEdge, dfFeatures("Timestamps") === dfTimestampsEdge("TimestampsEdge"))


    // selecting all the features in the df
/*    val MapFeaturesShort: collection.Map[sql.Timestamp, Seq[Map[String, Double]]] = dfFeaturesShort
      .map((row: Row) => (row.getTimestamp(0), row.toSeq.drop(1).map(_.asInstanceOf[Map[String, Double]])))
      .collectAsMap()
*/
    // selecting only the specified feature from the df
    val MapFeatureShort: collection.Map[sql.Timestamp, Map[String, Double]] = dfFeaturesShort.select("Timestamps", selectedFeature)
      .map((row: Row) => (row.getTimestamp(0), row.getAs[Map[String, Double]](selectedFeature)))
      .collectAsMap()

    val broadcastMapFeatureShort: Broadcast[collection.Map[sql.Timestamp, Map[String, Double]]] = sc.broadcast(MapFeatureShort)

    val frutta = dfTaggingInfo.select("ON_Time").map { row =>

      val timestampEdge: sql.Timestamp = row.getTimestamp(2)

      val windowTimestampEdge = broadcastMapFeatureShort.value
          .filterKeys((k: sql.Timestamp) => (k.before(timestampEdge + qualcosa)) && (k.after(timestampEdge - qualocsa)))
*/


//      Row.fromSeq(broadcastMapFeatureShort.value.get(row.getTimestamp(2)).get)
    }

}
