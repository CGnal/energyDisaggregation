package com.cgnal.enel.kaggle.models.EdgeDetection

import java.security.Timestamp
import java.util


import com.cgnal.enel.kaggle.helpers.DatasetHelper
import com.cgnal.enel.kaggle.utils.{MSDwithRealFeature, AverageOverComplex}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{WindowSpec, Window}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime
import org.apache.spark.sql.functions.{min,max,abs,avg}

import scala.collection
import scala.collection.Map
import scala.reflect.ClassTag
import com.databricks.spark.avro._

/**
  * Created by cavaste on 01/09/16.
  */
object EdgeDetection {

  // TODO ottimizzare questo metodo che è lento a causa del fatto che dentro il map bisogna capire quale è IDtime più vicino a ON_time e OFF_Time
  def selectingEdgeWindowsFromTagWithNumberPointsSingleFeature(dfFeatures: DataFrame, dfTaggingInfo: DataFrame,
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



  def selectingEdgeWindowsFromTagWithTimeIntervalSingleFeature[SelFeatureType:ClassTag](dfFeatures: DataFrame, dfTaggingInfo: DataFrame,
                                                                                        selectedFeature: String,
                                                                                        timestampIntervalPreEdge: Long, timestampIntervalPostEdge: Long, edgeWindowSize: Int,
                                                                                        sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    var dateTime = DateTime.now()

    val selectedFeatureDataType = dfFeatures.schema(selectedFeature).dataType

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

      val featureArrayWindowOnTime: Array[SelFeatureType] = dfFeaturesWindowOn
        .select(selectedFeature)
        .map(r => r.getAs[SelFeatureType](r.fieldIndex(selectedFeature)))
        .collect()
      val currentWindowSizeOnTime = featureArrayWindowOnTime.size
      println("Original feature window ON_Time length: " + currentWindowSizeOnTime.toString)
      // MANUALLY REMOVING EXTRA POINTS IN THE WINDOW
      val FeatureArrayWindowOnTimeCut =
        if (currentWindowSizeOnTime > edgeWindowSize) featureArrayWindowOnTime.take(edgeWindowSize)
        else featureArrayWindowOnTime
      println("Time for featureWindowOn: " + (DateTime.now().getMillis - dateTime.getMillis) / 1000 + " seconds")


      dateTime = DateTime.now()
      val offTime = row.getAs[Long]("OFF_Time")
      val startTimeOff: Long = offTime - timestampIntervalPreEdge*10E7.toLong
      val endTimeOff: Long = offTime + timestampIntervalPostEdge*10E7.toLong

      val dfFeaturesWindowOff: DataFrame =
        dfFeaturesSorted.filter($"Timestamp".gt(startTimeOff))
          .filter($"Timestamp".lt(endTimeOff))

      val featureArrayWindowOffTime: Array[SelFeatureType] = dfFeaturesWindowOff
        .select(selectedFeature)
        .map(r => r.getAs[SelFeatureType](r.fieldIndex(selectedFeature)))
        .collect()
      val currentWindowSizeOffTime = featureArrayWindowOffTime.size
      println("Original feature window OFF_Time length: " + currentWindowSizeOffTime.toString)
      // MANUALLY REMOVING EXTRA POINTS IN THE WINDOW
      val FeatureArrayWindowOffTimeCut: Array[SelFeatureType] =
        if (currentWindowSizeOffTime > edgeWindowSize) featureArrayWindowOffTime.take(edgeWindowSize)
        else featureArrayWindowOffTime
      println("Time for featureWindowOff: " + (DateTime.now().getMillis - dateTime.getMillis) / 1000 + " seconds")

/*      val timestampArrayWindowOffTime = dfFeaturesWindowOff
        .select("Timestamp")
        .map(r => r.getAs[Long]("Timestamp"))
        .collect()*/

      dateTime = DateTime.now()
      val IDedge = row.getAs[Int]("IDedge")
      val outputRow: Row = Row(IDedge, FeatureArrayWindowOnTimeCut, FeatureArrayWindowOffTimeCut)//, onTime, offTime)
      println("Time for building outputRow: " + (DateTime.now().getMillis - dateTime.getMillis) / 1000 + " seconds")
      outputRow

    })

/*    // check the length of each window of feature
    val windowSize: Int = featureWindows(0).getAs[Array[collection.Map[String, Double]]](1).size
    println("Lenght of first OnTime Window: " + windowSize.toString)

    val windowSizeOff: Int = featureWindows(0).getAs[Array[collection.Map[String, Double]]](2).size
    println("Lenght of first OffTime Window: " + windowSizeOff.toString)


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
    })*/

    // create the dataframe
    val RDDFeatureWindows = sc.parallelize(featureWindows)

    val featureWindowsSchema: StructType =
      StructType(
        StructField("IDedge", IntegerType, false) ::
          StructField("ON_TimeWindow_" + selectedFeature, selectedFeatureDataType, false) ::
          StructField("OFF_TimeWindow_" + selectedFeature, selectedFeatureDataType, false) :: Nil)

    val dfFeatureWindows: DataFrame = sqlContext.createDataFrame(RDDFeatureWindows, featureWindowsSchema)
    dfFeatureWindows
  }



  def computeSimilarityEdgeSignaturesRealFeatureGivenAppliance(dfFeatures: DataFrame, selectedFeature: String,
                                                               OnSignature: Array[Double], OffSignature: Array[Double],
                                                               timestepsNumberPreEdge: Int, timestepsNumberPostEdge: Int,
                                                               sc: SparkContext, sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._
    var dateTime = DateTime.now()


    val dfFeaturesSorted: DataFrame = dfFeatures.sort("Timestamp")
    val minTimestampAllowed: Long = dfFeaturesSorted.take(timestepsNumberPreEdge + 1).last.getAs[Long]("Timestamp")

    val dfFeaturesSortedDesc: DataFrame = dfFeatures.sort($"Timestamp".desc)
    val maxTimestampAllowed: Long = dfFeaturesSortedDesc.take(timestepsNumberPostEdge + 1).last.getAs[Long]("Timestamp")


    val w: WindowSpec = Window
      .orderBy("Timestamp")
      .rangeBetween(-timestepsNumberPreEdge,timestepsNumberPostEdge)

    // define UDAF
    val msdOnRealFeature = new MSDwithRealFeature(selectedFeature, OnSignature)
    val msdOffRealFeature = new MSDwithRealFeature(selectedFeature, OffSignature)

    // Edge score >= 0
    val dfFeaturesEdgeScore = dfFeatures.withColumn(
        "msdON_Time_" + selectedFeature,
        msdOnRealFeature(dfFeatures("Timestamp"),dfFeatures(selectedFeature)).over(w))
      .withColumn(
        "msdOFF_Time_" + selectedFeature,
        msdOffRealFeature(dfFeatures("Timestamp"),dfFeatures(selectedFeature)).over(w))
      .cache()


    // NORMALIZATION OF THE MSD

    val dfMinMaxOn: DataFrame = dfFeaturesEdgeScore.agg(
      min("msdON_Time_" + selectedFeature).as("min"),
      max("msdON_Time_" + selectedFeature).as("max")).cache()
    val minOn = dfMinMaxOn.head.getAs[Double]("min")
    val maxOn = dfMinMaxOn.head.getAs[Double]("max")

    val dfMinMaxOff: DataFrame = dfFeaturesEdgeScore.agg(
      min("msdOFF_Time_" + selectedFeature).as("min"),
      max("msdOFF_Time_" + selectedFeature).as("max")).cache()
    val minOff = dfMinMaxOff.head.getAs[Double]("min")
    val maxOff = dfMinMaxOff.head.getAs[Double]("max")

    // Normalized edge score in [0,1]
    val dfFeaturesNormalizedEdgeScoreTemp = dfFeaturesEdgeScore.withColumn(
      "nmsdON_Time_" + selectedFeature,
      (dfFeaturesEdgeScore("msdON_Time_" + selectedFeature) - minOn)/(maxOn-minOn))
      .withColumn(
        "nmsdOFF_Time_" + selectedFeature,
        (dfFeaturesEdgeScore("msdOFF_Time_" + selectedFeature) - minOff)/(maxOff-minOff))

    // Delta Score in [-2,2]
    val dfFeaturesNormalizedEdgeScoreTemp2 = dfFeaturesNormalizedEdgeScoreTemp.withColumn(
      "DeltaScore_" + selectedFeature, dfFeaturesNormalizedEdgeScoreTemp("nmsdON_Time_" + selectedFeature)
        - dfFeaturesNormalizedEdgeScoreTemp("nmsdOFF_Time_" + selectedFeature)
    )

    val dfFeatureEdgeScoreAppliance = dfFeaturesNormalizedEdgeScoreTemp2.filter($"Timestamp" > minTimestampAllowed)
      .filter($"Timestamp" < maxTimestampAllowed)

    dfFeatureEdgeScoreAppliance
  }


  def computeStoreDfEdgeWindowsSingleFeature(dfFeatures: DataFrame,
                                             filenameTaggingInfo: String, filenameDfEdgeWindowsFeature: String,
                                             selectedFeature: String,
                                             timestampIntervalPreEdge: Long, timestampIntervalPostEdge: Long, edgeWindowSize: Int,
                                             sc: SparkContext, sqlContext: SQLContext) = {

    val arrayTaggingInfo = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameTaggingInfo)
    val dfTaggingInfo: DataFrame = DatasetHelper.fromArrayIndexedToDFTaggingInfo(sc, sqlContext,
      arrayTaggingInfo, DatasetHelper.TagSchema)

    val dfEdgeWindows = selectingEdgeWindowsFromTagWithTimeIntervalSingleFeature[Map[String,Double]](dfFeatures, dfTaggingInfo,
      selectedFeature, timestampIntervalPreEdge, timestampIntervalPostEdge, edgeWindowSize,
      sc, sqlContext)

    val dfEdgeWindowsTaggingInfo: DataFrame = dfEdgeWindows.join(dfTaggingInfo, "IDedge")

    dfEdgeWindowsTaggingInfo.printSchema()

    dfEdgeWindowsTaggingInfo.write
      .avro(filenameDfEdgeWindowsFeature)

    dfEdgeWindowsTaggingInfo
  }


  def computeEdgeSignatureAppliances[SelFeatureType:ClassTag](dfEdgeWindowsFilename: String, edgeWindowSize: Int,
                                                              selectedFeature: String, selectedFeatureType: Class[SelFeatureType],
                                                              filenameSampleSubmission: String,
                                                              sc: SparkContext, sqlContext: SQLContext) = {

    if (!(selectedFeatureType.isAssignableFrom(classOf[Map[String,Double]]) || selectedFeatureType.isAssignableFrom(classOf[Double])))
      sys.error("Selected Feature Type must be Double or Map[String,Double]")

    val dfSampleSubmission = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(filenameSampleSubmission)

    val dfAppliancesToPredict = dfSampleSubmission.select("Appliance").distinct()

    val dfEdgeWindowsTaggingInfo = sqlContext.read.avro(dfEdgeWindowsFilename)

    val dfEdgeSignaturesAll =
      if (selectedFeatureType.isAssignableFrom(classOf[Map[String,Double]])) {
        // define UDAF
        val averageOverComplexON = new AverageOverComplex("ON_TimeWindow_" + selectedFeature, edgeWindowSize)
        val averageOverComplexOFF = new AverageOverComplex("OFF_TimeWindow_" + selectedFeature, edgeWindowSize)


        val dfEdgeSignatures: DataFrame = dfEdgeWindowsTaggingInfo.groupBy("ApplianceID").agg(
          averageOverComplexON(dfEdgeWindowsTaggingInfo.col("ON_TimeWindow_" + selectedFeature)).as("ON_TimeSignature_" + selectedFeature),
          averageOverComplexOFF(dfEdgeWindowsTaggingInfo.col("OFF_TimeWindow_" + selectedFeature)).as("OFF_TimeSignature_" + selectedFeature))

        dfEdgeSignatures.printSchema()
        dfEdgeSignatures
      }
      else {

        val dfEdgeSignatures: DataFrame = dfEdgeWindowsTaggingInfo.groupBy("ApplianceID").agg(
          avg("ON_TimeWindow_" + selectedFeature).as("ON_TimeSignature_" + selectedFeature),
          avg("OFF_TimeWindow_" + selectedFeature).as("OFF_TimeSignature_" + selectedFeature))

        dfEdgeSignatures.printSchema()
        dfEdgeSignatures
      }

    val dfEdgeSignatures = dfEdgeSignaturesAll.join(dfAppliancesToPredict, dfEdgeSignaturesAll("ApplianceID") === dfAppliancesToPredict("Appliance"))
      .drop(dfAppliancesToPredict("Appliance"))

    (dfEdgeSignatures, dfAppliancesToPredict)
  }

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