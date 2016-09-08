package com.cgnal.enel.kaggle.models.edgeDetection

import java.security.Timestamp
import java.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime

import scala.collection
import scala.collection.Map

/**
  * Created by cavaste on 01/09/16.
  */
object edgeDetection {

  // TODO ottimizzare questo metodo che è lento a causa del fatto che dentro il map bisogna capire quale è IDtime più vicino a ON_time e OFF_Time
  def selectingEdgeWindowsSingleFeatureNumberPoints(dfFeatures: DataFrame, dfTaggingInfo: DataFrame,
                                                    selectedFeature: String, timestepsNumberPreEdge: Int, timestepsNumberPostEdge: Int,
                                                    sc: SparkContext, sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._
    var dateTime = DateTime.now()

    println("About to prepare the extraction of Edge feature window")

    val dfFeaturesSorted: DataFrame = dfFeatures.sort("Timestamp").cache
    val minTimestampAllowed: Long = dfFeaturesSorted.take(timestepsNumberPreEdge + 1).last.getAs[Long]("Timestamp")

    val dfFeaturesSortedDesc: DataFrame = dfFeatures.sort($"Timestamp".desc).cache
    val maxTimestampAllowed: Long = dfFeaturesSortedDesc.take(timestepsNumberPostEdge + 1).last.getAs[Long]("Timestamp")

    val dfTaggingInfoAllowed = dfTaggingInfo.filter($"ON_Time" > minTimestampAllowed)
      .filter($"OFF_Time" < maxTimestampAllowed)

    val s: Array[Row] = dfTaggingInfoAllowed.collect()

    println("Time for preparing the extraction of Edge feature window: " + (DateTime.now().getMillis - dateTime.getMillis) / 1000 + " seconds")

    println("About to process " + s.length + " rows")

    // extract the windows of feature for each ON and OFF event
    val featureWindows: Array[Row] = s.map(row => {
      dateTime = DateTime.now()
      val onTime: Long = row.getAs[Long]("ON_Time")

      val IDtimeOnTime: Int = dfFeatures.sort(abs($"Timestamp"-onTime)).head.getAs[Int]("IDtime")
      val startTimeOn: Int = IDtimeOnTime - timestepsNumberPreEdge
      val endTimeOn: Int = IDtimeOnTime + timestepsNumberPostEdge

      val dfFeaturesWindowOn: DataFrame =
        dfFeaturesSorted.filter($"IDtime".gt(startTimeOn))
          .filter($"IDtime".lt(endTimeOn))

      val FeatureArrayWindowOnTime: Array[Map[String, Double]] = dfFeaturesWindowOn
        .select(selectedFeature)
        .map(r => r.getMap[String, Double](r.fieldIndex(selectedFeature)))
        .collect()
      println("Time for featureWindowOn: " + (DateTime.now().getMillis - dateTime.getMillis) / 1000 + " seconds")

      dateTime = DateTime.now()
      val offTime = row.getAs[Long]("OFF_Time")

      val IDtimeOffTime: Int = dfFeatures.sort(abs($"Timestamp"-offTime)).head.getAs[Int]("IDtime")
      val startTimeOff: Int = IDtimeOffTime - timestepsNumberPreEdge
      val endTimeOff: Int = IDtimeOffTime + timestepsNumberPostEdge

      val dfFeaturesWindowOff: DataFrame =
        dfFeaturesSorted.filter($"IDtime".gt(startTimeOff))
          .filter($"IDtime".lt(endTimeOff))

      val FeatureArrayWindowOffTime: Array[Map[String, Double]] = dfFeaturesWindowOff
        .select(selectedFeature)
        .map(r => r.getMap[String, Double](r.fieldIndex(selectedFeature)))
        .collect()
      println("Time for featureWindowOff: " + (DateTime.now().getMillis - dateTime.getMillis) / 1000 + " seconds")

      val timestampArrayWindowOffTime = dfFeaturesWindowOff
        .select("Timestamp")
        .map(r => r.getAs[Long]("Timestamp"))
        .collect()

//      dateTime = DateTime.now()
      val IDedge = row.getAs[Int]("IDedge")
      val outputRow: Row = Row(IDedge, FeatureArrayWindowOnTime, FeatureArrayWindowOffTime, onTime, offTime, timestampArrayWindowOffTime)
//      println("Time for building outputRow: " + (DateTime.now().getMillis - dateTime.getMillis) / 1000 + " seconds")
      outputRow

    })

      // check the length of each window of feature
      val windowSize: Int = featureWindows(0).getAs[Array[collection.Map[String, Double]]](1).size
      println("Lenght of first OnTime Window: " + windowSize.toString)

      val windowSizeOff: Int = featureWindows(0).getAs[Array[collection.Map[String, Double]]](2).size
      println("Lenght of first OffTime Window: " + windowSizeOff.toString)


      featureWindows.foreach(row => {
        if (row.getAs[Array[collection.Map[String, Double]]](1).size != windowSize)
          sys.error("Lenght of OnTime Window of onTime "
            + row.getAs[Int](3).toString
            + " IDedge: " + row.getAs[Int](0).toString + ": "
            + row.getAs[Array[collection.Map[String, Double]]](1).size.toString)
        else{
          println("onTime "
            + row.getAs[Int](3).toString
            + " IDedge: " + row.getAs[Int](0).toString)
        }

        if (row.getAs[Array[collection.Map[String, Double]]](2).size != windowSizeOff)
          {
            row.getAs[Array[Long]](5).foreach(time => println(time))

            sys.error("Lenght of OffTime Window of offTime "
              + row.getAs[Int](4).toString
              + " IDedge: " + row.getAs[Int](0).toString + ": "
              + row.getAs[Array[collection.Map[String, Double]]](2).size.toString)}

        else{
          row.getAs[Array[Long]](5).foreach(time => println(time))
          println("offTime "
            + row.getAs[Int](4).toString
            + " IDedge: " + row.getAs[Int](0).toString)
        }
      })

      // create the dataframe
      val RDDFeatureWindows = sc.parallelize(featureWindows)

      val featureWindowsSchema: StructType =
        StructType(
          StructField("IDedge", IntegerType, false) ::
            StructField("ON_TimeWindow_" + selectedFeature, ArrayType(MapType(StringType, DoubleType)), false) ::
            StructField("OFF_TimeWindow_" + selectedFeature, ArrayType(MapType(StringType, DoubleType)), false) :: Nil)

      val dfFeatureWindows: DataFrame = sqlContext.createDataFrame(RDDFeatureWindows, featureWindowsSchema)
      dfFeatureWindows
  }



  def selectingEdgeWindowsSingleFeatureTimeInterval(dfFeatures: DataFrame, dfTaggingInfo: DataFrame,
                                                    selectedFeature: String,
                                                    timestampIntervalPreEdge: Long, timestampIntervalPostEdge: Long, edgeWindowSize: Int,
                                                    sc: SparkContext, sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._
    var dateTime = DateTime.now()

    println("About to prepare the extraction of Edge feature window")

    val minTimestamp: Long = dfFeatures.agg(min($"Timestamp")).collect().head.getAs[Long](0)
    val maxTimestamp: Long = dfFeatures.agg(max($"Timestamp")).collect().head.getAs[Long](0)

    val minTimestampAllowed = minTimestamp + timestampIntervalPreEdge*10E7.toLong
    val maxTimestampAllowed = maxTimestamp - timestampIntervalPostEdge*10E7.toLong

    val dfTaggingInfoAllowed = dfTaggingInfo.filter($"ON_Time" >= minTimestampAllowed)
      .filter($"OFF_Time" <= maxTimestampAllowed)

    val s: Array[Row] = dfTaggingInfoAllowed.collect()

    val dfFeaturesSorted: DataFrame = dfFeatures.sort("Timestamp").cache

    println("Time for preparing the extraction of Edge feature window: " + (DateTime.now().getMillis - dateTime.getMillis) / 1000 + " seconds")
    println("About to process " + s.length + " rows")

    // extract the windows of feature for each ON and OFF event
    val featureWindows: Array[Row] = s.map(row => {
      dateTime = DateTime.now()
      val onTime: Long = row.getAs[Long]("ON_Time")
      val startTimeOn: Long = onTime - timestampIntervalPreEdge*10E7.toLong
      val endTimeOn: Long = onTime + timestampIntervalPostEdge*10E7.toLong

      val dfFeaturesWindowOn: DataFrame =
        dfFeaturesSorted.filter($"Timestamp".gt(startTimeOn))
          .filter($"Timestamp".lt(endTimeOn))

      val FeatureArrayWindowOnTime: Array[Map[String, Double]] = dfFeaturesWindowOn
        .select(selectedFeature)
        .map(r => r.getMap[String, Double](r.fieldIndex(selectedFeature)))
        .collect()
      val currentWindowSizeOnTime = FeatureArrayWindowOnTime.size
      println("Original feature window ON_Time length: " + currentWindowSizeOnTime.toString)
      // MANUALLY REMOVING EXTRA POINTS IN THE WINDOW
      val FeatureArrayWindowOnTimeCut: Array[Map[String, Double]] =
        if (currentWindowSizeOnTime > edgeWindowSize) FeatureArrayWindowOnTime.take(edgeWindowSize)
        else FeatureArrayWindowOnTime
      println("Time for featureWindowOn: " + (DateTime.now().getMillis - dateTime.getMillis) / 1000 + " seconds")


      dateTime = DateTime.now()
      val offTime = row.getAs[Long]("OFF_Time")
      val startTimeOff: Long = offTime - timestampIntervalPreEdge*10E7.toLong
      val endTimeOff: Long = offTime + timestampIntervalPostEdge*10E7.toLong

      val dfFeaturesWindowOff: DataFrame =
        dfFeaturesSorted.filter($"Timestamp".gt(startTimeOff))
          .filter($"Timestamp".lt(endTimeOff))

      val FeatureArrayWindowOffTime: Array[Map[String, Double]] = dfFeaturesWindowOff
        .select(selectedFeature)
        .map(r => r.getMap[String, Double](r.fieldIndex(selectedFeature)))
        .collect()
      val currentWindowSizeOffTime = FeatureArrayWindowOffTime.size
      println("Original feature window OFF_Time length: " + currentWindowSizeOffTime.toString)
      // MANUALLY REMOVING EXTRA POINTS IN THE WINDOW
      val FeatureArrayWindowOffTimeCut: Array[Map[String, Double]] =
        if (currentWindowSizeOffTime > edgeWindowSize) FeatureArrayWindowOffTime.take(edgeWindowSize)
        else FeatureArrayWindowOffTime
      println("Time for featureWindowOff: " + (DateTime.now().getMillis - dateTime.getMillis) / 1000 + " seconds")

      val timestampArrayWindowOffTime = dfFeaturesWindowOff
        .select("Timestamp")
        .map(r => r.getAs[Long]("Timestamp"))
        .collect()


      dateTime = DateTime.now()
      val IDedge = row.getAs[Int]("IDedge")
      val outputRow: Row = Row(IDedge, FeatureArrayWindowOnTimeCut, FeatureArrayWindowOffTimeCut)//, onTime, offTime)
      println("Time for building outputRow: " + (DateTime.now().getMillis - dateTime.getMillis) / 1000 + " seconds")
      outputRow

    })

    // check the length of each window of feature
/*    val windowSize: Int = featureWindows(0).getAs[Array[collection.Map[String, Double]]](1).size
    println("Lenght of first OnTime Window: " + windowSize.toString)

    val windowSizeOff: Int = featureWindows(0).getAs[Array[collection.Map[String, Double]]](2).size
    println("Lenght of first OffTime Window: " + windowSizeOff.toString)
*/

    featureWindows.foreach(row => {
      if (row.getAs[Array[collection.Map[String, Double]]](1).size != edgeWindowSize)
        {sys.error("Lenght of OnTime Window of onTime "
          + row.getAs[Int](3).toString
          + " IDedge: " + row.getAs[Int](0).toString + ": "
          + row.getAs[Array[collection.Map[String, Double]]](1).size.toString)}
//      else{
//        println("onTime "
//          + row.getAs[Int](3).toString
//          + " IDedge: " + row.getAs[Int](0).toString)
//      }

      if (row.getAs[Array[collection.Map[String, Double]]](2).size != edgeWindowSize)
      {
//        row.getAs[Array[Long]](5).foreach(time => println(time))
        sys.error("Lenght of OffTime Window of offTime "
          + row.getAs[Int](4).toString
          + " IDedge: " + row.getAs[Int](0).toString + ": "
          + row.getAs[Array[collection.Map[String, Double]]](2).size.toString)}

//      else{
//        row.getAs[Array[Long]](5).foreach(time => println(time))
//        println("offTime "
//          + row.getAs[Int](4).toString
//          + " IDedge: " + row.getAs[Int](0).toString)
//      }
    })

    // create the dataframe
    val RDDFeatureWindows = sc.parallelize(featureWindows)

    val featureWindowsSchema: StructType =
      StructType(
        StructField("IDedge", IntegerType, false) ::
          StructField("ON_TimeWindow_" + selectedFeature, ArrayType(MapType(StringType, DoubleType)), false) ::
          StructField("OFF_TimeWindow_" + selectedFeature, ArrayType(MapType(StringType, DoubleType)), false) :: Nil)

    val dfFeatureWindows: DataFrame = sqlContext.createDataFrame(RDDFeatureWindows, featureWindowsSchema)
    dfFeatureWindows
  }


  // TENTATIVO DI FARE IL PRIMO FILTRO SU DFfeatures e poi fare groupby
  /*  def selectingEdgeWindows2(dfFeatures: DataFrame, dfTaggingInfo: DataFrame,
                             selectedFeature: String, timestampIntervalPreEdge: Double, timestampIntervalPostEdge: Double,
                             sc: SparkContext, sqlContext: SQLContext): DataFrame = {

      import sqlContext.implicits._

      val s: Array[Row] = dfTaggingInfo.collect()
      val sBroad: Broadcast[Array[Row]] = sc.broadcast(s)

      val schemaFeatures: StructType = dfFeatures.schema
      val fieldnamesDfFeatures: Array[String] = schemaFeatures.fieldNames

      // extract the windows of feature for each ON and OFF event
      val onTimeFeatureWindows: RDD[Row] = dfFeatures.flatMap((timestampRow: Row) => {
        val time = timestampRow.getAs[Double]("Timestamp")
        val onTimesAssociatedWithTime: Array[Row] = sBroad.value
          .filter(row => {
            val onTime = row.getAs[Double]("ON_Time")
            (time > (onTime - timestampIntervalPreEdge) && time < (onTime + timestampIntervalPreEdge))
          })
          .map(row => Row(row.getAs[Int]("IDedge"), row.getAs[Int]("ON_Time")))

        // dfFeatures + IDedge + ON_Time
        val timestampArrayAssociatedWithOnTime: Array[Row] = onTimesAssociatedWithTime.map(onTimeRow => {
          Row.merge(timestampRow, onTimeRow)
        })
        timestampArrayAssociatedWithOnTime
      })

      val dfFeaturesOnTimeSchema = schemaFeatures.add(StructField("IDedge", IntegerType, false))
        .add(StructField("ON_Time", DoubleType, false))


  //    val by1: RDD[(Int, Iterable[Row])] = onTimeFeatureWindows.groupBy(_.getAs[Int]("IDedge"))


      val by: GroupedData = sqlContext.createDataFrame(onTimeFeatureWindows, dfFeaturesOnTimeSchema)
        .groupBy("IDedge")

      val dfFeaturesOnTime: DataFrame = by.agg(collect("ON_Time"), collect_set("pippo"))
        //collect_set("Timestamp"),
        //collect_set("ON_Time"))


      val offTimeFeatureWindows: RDD[Row] = dfFeatures.flatMap((timestampRow: Row) => {
        val time = timestampRow.getAs[Double]("Timestamp")
        val offTimesAssociatedWithTime: Array[Row] = sBroad.value
          .filter(row => {
            val offTime = row.getAs[Double]("OFF_Time")
            (time > (offTime - timestampIntervalPreEdge) && time < (offTime + timestampIntervalPreEdge))
          }).map(row => Row(row.getAs[Int]("IDedge"), row.getAs[Int]("OFF_Time")))

        val timestampArrayAssociatedWithOffTime: Array[Row] = offTimesAssociatedWithTime.map(offTimeRow => {
          Row.merge(timestampRow, offTimeRow)
        })
        timestampArrayAssociatedWithOffTime
      })

      val dfFeaturesOffTimeSchema = schemaFeatures.add(StructField("IDedge", IntegerType, false))
        .add(StructField("OFF_Time", DoubleType, false))

      val dfFeaturesOffTime = sqlContext.createDataFrame(offTimeFeatureWindows, dfFeaturesOffTimeSchema)
  //      .groupBy("OFF_Time")

  //    val dfFeaturesEdgeWindows: DataFrame = dfFeaturesOnTime.join(dfFeaturesOffTime, dfFeaturesOnTime("IDedge") === dfFeaturesOffTime("IDedge"))
  //      .drop(dfFeaturesOffTime("IDedge"))
      dfFeaturesOffTime

      }
      */

}