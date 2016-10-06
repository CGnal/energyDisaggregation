package com.cgnal.enel.kaggle.models.edgeDetection

import java.io.{BufferedWriter, FileOutputStream, ObjectOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Paths, Files}

import com.cgnal.enel.kaggle.helpers.DatasetHelper
import com.cgnal.enel.kaggle.utils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{WindowSpec, Window}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime
import org.apache.spark.sql.functions._

import scala.collection
import scala.collection.mutable.ArrayBuffer
import scala.collection.{mutable, Map}
import scala.reflect.ClassTag
import com.databricks.spark.avro._
import scala.util.control.Breaks._

import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}

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



  def selectingEdgeWindowsRealFromTagWithTimeIntervalSingleFeature(dfFeatures: DataFrame, dfTaggingInfo: DataFrame,
                                                                   selectedFeature: String,
                                                                   timestampIntervalPreEdge: Long, timestampIntervalPostEdge: Long, edgeWindowSize: Int,
                                                                   sc: SparkContext, sqlContext: SQLContext,
                                                                   timestampFactor: Double = 1E7): DataFrame = {
    import sqlContext.implicits._
    var dateTime = DateTime.now()

    val selectedFeatureDataType = dfFeatures.schema(selectedFeature).dataType

    println("About to prepare the extraction of Edge feature window")

    val minTimestamp: Long = dfFeatures.agg(min($"Timestamp")).head.getAs[Long](0)
    val maxTimestamp: Long = dfFeatures.agg(max($"Timestamp")).head.getAs[Long](0)

    val minTimestampAllowed = minTimestamp + timestampIntervalPreEdge*timestampFactor.toLong
    val maxTimestampAllowed = maxTimestamp - timestampIntervalPostEdge*timestampFactor.toLong

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
      val startTimeOn: Long = onTime - timestampIntervalPreEdge*timestampFactor.toLong
      val endTimeOn: Long = onTime + timestampIntervalPostEdge*timestampFactor.toLong

      val dfFeaturesWindowOn: DataFrame =
        dfFeaturesSorted.filter($"Timestamp".gt(startTimeOn))
          .filter($"Timestamp".lt(endTimeOn))

      val featureArrayWindowOnTime: Array[Double] = dfFeaturesWindowOn
        .map(r => r.getAs[Double](r.fieldIndex(selectedFeature))).collect
/*          val pippo = Try{r.getAs[SelFeatureType](r.fieldIndex(selectedFeature))}
          pippo match {
            case Success(value) => value
            case Failure(ex) =>
              println("ciaooo")
              0D.asInstanceOf[SelFeatureType]
          }
          }*/

      val currentWindowSizeOnTime = featureArrayWindowOnTime.size
      println("Original feature window ON_Time length: " + currentWindowSizeOnTime.toString)
      // MANUALLY REMOVING EXTRA POINTS IN THE WINDOW
      val FeatureArrayWindowOnTimeCut =
        if (currentWindowSizeOnTime > edgeWindowSize) featureArrayWindowOnTime.take(edgeWindowSize)
        else featureArrayWindowOnTime
      println("Time for featureWindowOn: " + (DateTime.now().getMillis - dateTime.getMillis) / 1000 + " seconds")


      dateTime = DateTime.now()
      val offTime = row.getAs[Long]("OFF_Time")
      val startTimeOff: Long = offTime - timestampIntervalPreEdge*timestampFactor.toLong
      val endTimeOff: Long = offTime + timestampIntervalPostEdge*timestampFactor.toLong

      val dfFeaturesWindowOff: DataFrame =
        dfFeaturesSorted.filter($"Timestamp".gt(startTimeOff))
          .filter($"Timestamp".lt(endTimeOff))

      val featureArrayWindowOffTime: Array[Double] = dfFeaturesWindowOff
        .select(selectedFeature)
        .map(r => r.getAs[Double](r.fieldIndex(selectedFeature)))
        .collect()
      val currentWindowSizeOffTime = featureArrayWindowOffTime.size
      println("Original feature window OFF_Time length: " + currentWindowSizeOffTime.toString)
      // MANUALLY REMOVING EXTRA POINTS IN THE WINDOW
      val FeatureArrayWindowOffTimeCut: Array[Double] =
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
          StructField("ON_TimeWindow_" + selectedFeature, ArrayType(selectedFeatureDataType), false) ::
          StructField("OFF_TimeWindow_" + selectedFeature, ArrayType(selectedFeatureDataType), false) :: Nil)

    val dfFeatureWindows: DataFrame = sqlContext.createDataFrame(RDDFeatureWindows, featureWindowsSchema)
    dfFeatureWindows
  }



  def computeSimilarityEdgeSignaturesRealFeatureGivenAppliance(dfFeatures: DataFrame, selectedFeature: String,
                                                               OnSignature: Array[Double], OffSignature: Array[Double],
                                                               timestepsNumberPreEdge: Int, timestepsNumberPostEdge: Int,
                                                               partitionNumber: Int,
                                                               sc: SparkContext, sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._
    var dateTime = DateTime.now()

    val edgeWindowSize = timestepsNumberPreEdge + timestepsNumberPostEdge + 1

    val partitonLength = (dfFeatures.count().toDouble/partitionNumber.toDouble).round

    val dfFeaturesIDpartition = dfFeatures.withColumn("IDpartition", (dfFeatures.col("IDtime")/partitonLength).cast(IntegerType))

    val dfFeaturesPartition = dfFeaturesIDpartition.repartition(partitionNumber, dfFeaturesIDpartition("IDpartition"))

    val dfFeaturesSorted = dfFeaturesPartition.sortWithinPartitions("IDtime").cache()

    val RDDfeaturesEdgeScore: RDD[Row] = dfFeaturesSorted.mapPartitions((rows: Iterator[Row]) => {
      val windowList: rows.GroupedIterator[Row] = rows.sliding(edgeWindowSize)
      val pippo: Iterator[Row] = windowList.map({ (window: Seq[Row]) =>
        val squareDistance: (Double, Double) = window.zipWithIndex.map{ (el: (Row, Int)) =>
          val elFeatureValue = el._1.getAs[Double](selectedFeature)
          val squareDistanceOnEl = (elFeatureValue - OnSignature(el._2)) * (elFeatureValue - OnSignature(el._2))
          val squareDistanceOffEl = (elFeatureValue - OffSignature(el._2)) * (elFeatureValue - OffSignature(el._2))
          (squareDistanceOnEl, squareDistanceOffEl)
        }.reduceLeft((x,y) => (x._1 + y._1, x._2 + y._2))

        Row(window(timestepsNumberPreEdge).getAs[Int]("IDtime"), window(timestepsNumberPreEdge).getAs[Long]("Timestamp"),
        squareDistance._1, squareDistance._2)
      })
      pippo
    })

    val edgeScoreSchema: StructType =
      StructType(
        StructField("IDtime", IntegerType, false) ::
          StructField("Timestamp", LongType, false) ::
          StructField("msdON_Time_" + selectedFeature, DoubleType, false) ::
          StructField("msdOFF_Time_" + selectedFeature, DoubleType, false) :: Nil)

    val dfFeaturesEdgeScore = sqlContext.createDataFrame(RDDfeaturesEdgeScore, edgeScoreSchema).cache()


    // NOT WORKING: YOU CANNOT USE AN UDAF WHEN MAKING AGGREGATION WITH A WINDOW FUNCTION
/*    val w: WindowSpec = Window
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
*/


    // NORMALIZATION OF THE MSD

    val dfMinMaxOn: DataFrame = dfFeaturesEdgeScore.agg(
      min("msdON_Time_" + selectedFeature).as("min"),
      max("msdON_Time_" + selectedFeature).as("max")).cache()
    val minOn = dfMinMaxOn.head.getAs[Double]("min")
    val maxOn = dfMinMaxOn.head.getAs[Double]("max")
    dfMinMaxOn.unpersist()

    val dfMinMaxOff: DataFrame = dfFeaturesEdgeScore.agg(
      min("msdOFF_Time_" + selectedFeature).as("min"),
      max("msdOFF_Time_" + selectedFeature).as("max")).cache()
    val minOff = dfMinMaxOff.head.getAs[Double]("min")
    val maxOff = dfMinMaxOff.head.getAs[Double]("max")
    dfMinMaxOff.unpersist()

    // Normalized mean squared distance in [0,1]
    val dfFeaturesNormalizedEdgeScoreTemp = dfFeaturesEdgeScore.withColumn(
      "nmsdON_Time_" + selectedFeature,
      (dfFeaturesEdgeScore("msdON_Time_" + selectedFeature) - minOn)/(maxOn-minOn))
      .withColumn(
        "nmsdOFF_Time_" + selectedFeature,
        (dfFeaturesEdgeScore("msdOFF_Time_" + selectedFeature) - minOff)/(maxOff-minOff))

    val dfFeaturesNormalizedEdgeScoreTemp2 = dfFeaturesNormalizedEdgeScoreTemp.withColumn(
      "scoreON_Time_" + selectedFeature,
      (dfFeaturesNormalizedEdgeScoreTemp("nmsdON_Time_" + selectedFeature) - 1)*(-1))
      .withColumn(
        "scoreOFF_Time_" + selectedFeature,
        (dfFeaturesNormalizedEdgeScoreTemp("nmsdOFF_Time_" + selectedFeature) - 1)*(-1))

    // Delta Score in [-2,2]
    val dfFeatureEdgeScoreAppliance = dfFeaturesNormalizedEdgeScoreTemp2.withColumn(
      "DeltaScore_" + selectedFeature, dfFeaturesNormalizedEdgeScoreTemp2("scoreON_Time_" + selectedFeature)
        - dfFeaturesNormalizedEdgeScoreTemp2("scoreOFF_Time_" + selectedFeature))

    dfFeaturesEdgeScore.unpersist()

    dfFeatureEdgeScoreAppliance
  }


  def computeStoreDfEdgeWindowsTaggingInfoSingleFeature[SelFeatureType:ClassTag](dfFeatures: DataFrame,
                                                                                 dfTaggingInfo: DataFrame,
                                                                                 dfEdgeWindowsFilename: String,
                                                                                 selectedFeature: String,
                                                                                 timestampIntervalPreEdge: Long, timestampIntervalPostEdge: Long,
                                                                                 edgeWindowSize: Int,
                                                                                 sc: SparkContext, sqlContext: SQLContext) = {

    val dfEdgeWindows = selectingEdgeWindowsRealFromTagWithTimeIntervalSingleFeature(dfFeatures, dfTaggingInfo,
      selectedFeature, timestampIntervalPreEdge, timestampIntervalPostEdge, edgeWindowSize,
      sc, sqlContext)


    val dfEdgeWindowsTaggingInfo: DataFrame = dfEdgeWindows.join(dfTaggingInfo, "IDedge")

    val path: Path = Path (dfEdgeWindowsFilename)
    if (path.exists) {
      Try(path.deleteRecursively())
    }
    dfEdgeWindowsTaggingInfo.write
      .avro(dfEdgeWindowsFilename)

    dfEdgeWindowsTaggingInfo
  }


  def computeEdgeSignatureAppliances[SelFeatureType:ClassTag](dfEdgeWindowsTaggingInfo: DataFrame, edgeWindowSize: Int,
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

        val averageOverRealON = new AverageOverReal("ON_TimeWindow_" + selectedFeature, edgeWindowSize)
        val averageOverRealOFF = new AverageOverReal("OFF_TimeWindow_" + selectedFeature, edgeWindowSize)

        val dfEdgeSignatures: DataFrame = dfEdgeWindowsTaggingInfo.groupBy("ApplianceID").agg(
          averageOverRealON(dfEdgeWindowsTaggingInfo.col("ON_TimeWindow_" + selectedFeature)).as("ON_TimeSignature_" + selectedFeature),
          averageOverRealOFF(dfEdgeWindowsTaggingInfo.col("OFF_TimeWindow_" + selectedFeature)).as("OFF_TimeSignature_" + selectedFeature))

        dfEdgeSignatures.printSchema()
        dfEdgeSignatures
      }

    dfEdgeWindowsTaggingInfo.unpersist()

    val dfEdgeSignatures = dfEdgeSignaturesAll.join(dfAppliancesToPredict, dfEdgeSignaturesAll("ApplianceID") === dfAppliancesToPredict("Appliance"))
      .drop(dfAppliancesToPredict("Appliance"))

    (dfEdgeSignatures, dfAppliancesToPredict)
  }



  def computeEdgeSignatureAppliancesWithVar[SelFeatureType](dfEdgeWindowsTaggingInfo: DataFrame,
                                                            edgeWindowSize: Int,
                                                            selectedFeature: String,
                                                            selectedFeatureType: Class[SelFeatureType],
                                                            sc: SparkContext, sqlContext: SQLContext) = {


    if (!(selectedFeatureType.isAssignableFrom(classOf[Map[String, Double]]) || selectedFeatureType.isAssignableFrom(classOf[Double])))
      sys.error("Selected Feature Type must be Double or Map[String,Double]")


    // TODO : implementation of the Variance OVer Complex
    val dfEdgeSignaturesAll =
      if (selectedFeatureType.isAssignableFrom(classOf[Map[String, Double]])) {
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

        val averageOverRealON = new AverageOverReal("ON_TimeWindow_" + selectedFeature, edgeWindowSize)
        val averageOverRealOFF = new AverageOverReal("OFF_TimeWindow_" + selectedFeature, edgeWindowSize)

        val varianceOverRealON = new VarianceOverReal("ON_TimeWindow_" + selectedFeature, "ON_TimeSignature_" + selectedFeature, edgeWindowSize)
        val varianceOverRealOFF = new VarianceOverReal("OFF_TimeWindow_" + selectedFeature, "OFF_TimeSignature_" + selectedFeature, edgeWindowSize)
        dfEdgeWindowsTaggingInfo.printSchema()
        dfEdgeWindowsTaggingInfo.show()


        val dfEdgeSignatures: DataFrame = dfEdgeWindowsTaggingInfo
          .groupBy("ApplianceID")
          .agg(
            averageOverRealON(dfEdgeWindowsTaggingInfo.col("ON_TimeWindow_" + selectedFeature)).as("ON_TimeSignature_" + selectedFeature),
            averageOverRealOFF(dfEdgeWindowsTaggingInfo.col("OFF_TimeWindow_" + selectedFeature)).as("OFF_TimeSignature_" + selectedFeature))

        println("printing schema dfEdgeSignatures: ")
        dfEdgeSignatures.printSchema()
        println("showing schema dfEdgeSignatures: ")
        dfEdgeSignatures.show()

        val tmp: DataFrame = dfEdgeWindowsTaggingInfo.join(dfEdgeSignatures, "ApplianceID")
        val dfEdgeSignaturesVar: DataFrame = tmp
          .groupBy("ApplianceID")
          .agg(
            varianceOverRealON(
              (tmp.col("ON_TimeWindow_" + selectedFeature)),
              (tmp.col("ON_TimeSignature_" + selectedFeature))
            ).as("ON_TimeSignatureVariance_" + selectedFeature),
            varianceOverRealOFF(
              (tmp.col("OFF_TimeWindow_" + selectedFeature)),
              (tmp.col("OFF_TimeSignature_" + selectedFeature)))
              .as("OFF_TimeSignatureVariance_" + selectedFeature)
          )

        println("printing schema dfEdgeSignaturesVar: ")
        dfEdgeSignaturesVar.printSchema()
        println("showing  dfEdgeSignaturesVar: ")
        dfEdgeSignaturesVar.show()
        val finalDF: DataFrame = dfEdgeSignatures.join(dfEdgeSignaturesVar, "ApplianceID")
        finalDF
      }

    dfEdgeWindowsTaggingInfo.unpersist()

    dfEdgeSignaturesAll
  }




  def buildStoreDfSimilarityScoreRealFeature(dfFeature: DataFrame,
                                             dfEdgeSignatures: DataFrame,
                                             applianceID: Int, selectedFeature: String,
                                             timestepsNumberPreEdge: Int, timestepsNumberPostEdge: Int,
                                             downsamplingBinPredictionSize: Int, partitionNumber: Int,
                                             outputDirName: String,
                                             sc: SparkContext, sqlContext: SQLContext) = {

    var dateTime = DateTime.now()

    val onSignature: Array[Double] = dfEdgeSignatures.filter(dfEdgeSignatures("ApplianceID") === (applianceID))
      .head.getAs[mutable.WrappedArray[Double]]("ON_TimeSignature_" + selectedFeature).toArray[Double]

    val offSignature: Array[Double] = dfEdgeSignatures.filter(dfEdgeSignatures("ApplianceID") === (applianceID))
      .head.getAs[mutable.WrappedArray[Double]]("OFF_TimeSignature_" + selectedFeature).toArray[Double]

    // EVALUATION HL OVER TRAINING SET
    val dfRealFeatureEdgeScore = computeSimilarityEdgeSignaturesRealFeatureGivenAppliance(dfFeature,
      selectedFeature,
      onSignature, offSignature,
      timestepsNumberPreEdge, timestepsNumberPostEdge, partitionNumber,
      sc, sqlContext).cache()

    // saving dfRealFeatureEdgeScoreApplianceTrain
    val filenameDfRealFeatureEdgeScore = outputDirName + "/ScoreNoDS_AppID" + applianceID.toString + ".csv"
    CSVutils.storingSparkCsv(dfRealFeatureEdgeScore, filenameDfRealFeatureEdgeScore)


    println("Time for COMPUTING EDGE SIMILARITY for applianceID" + applianceID.toString +
      ": " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")
    println("RESAMPLING SIMILARITY SCORES")
    dateTime = DateTime.now()
    val dfRealFeatureEdgeScoreDS = Resampling.edgeScoreDownsampling(dfRealFeatureEdgeScore,
      selectedFeature, downsamplingBinPredictionSize).cache()

    dfRealFeatureEdgeScore.unpersist()

    println("Time for RESAMPLING SIMILARITY SCORES for applianceID" + applianceID.toString +
      ": " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")

    // saving dfRealFeatureEdgeScoreApplianceTrain DOWNSAMPLED
    val filenameDfRealFeatureEdgeScoreDS = outputDirName + "/ScoreDS_AppID" + applianceID.toString + ".csv"
    CSVutils.storingSparkCsv(dfRealFeatureEdgeScoreDS, filenameDfRealFeatureEdgeScoreDS)


    dfRealFeatureEdgeScoreDS
  }



  def buildPredictionEvaluateHLRealFeature(dfRealFeatureEdgeScoreDS: DataFrame,
                                           dfGroundTruth: DataFrame,
                                           thresholdToTestSorted: Array[(Double, Double)],
                                           applianceID: Int, applianceName: String,
                                           selectedFeature: String,
                                           outputDirName: String,
                                           scoresONcolName: String,
                                           scoresOFFcolName: String,
                                           downsamplingBinPredictionSec: Int,
                                           bw: BufferedWriter) = {

    var dateTime = DateTime.now()

    val hammingLossFake: DataFrame =
      dfGroundTruth
        .agg(sum("GroundTruth"))

    val HLwhenAlways0: Double = hammingLossFake.head().getLong(0) / dfGroundTruth.count().toDouble
    println("current hamming loss with 0 model: " + HLwhenAlways0.toString)

    println("Computing hamming loss for each threshold")

/*    val hammingLosses: Array[Double] = thresholdToTestSorted.map(threshold =>
      HammingLoss.evaluateHammingLoss(
        dfRealFeatureEdgeScoreDS,
        dfGroundTruth, "GroundTruth", scoresONcolName, scoresOFFcolName,
        "TimestampPrediction", applianceID, threshold._1, threshold._2, downsamplingBinPredictionSec, outputDirName)
    )
*/
    val PerfoverThresholdBuffer: ArrayBuffer[((Double,Double), (Double, Double, Double))] = new ArrayBuffer()

    for (threshold <- thresholdToTestSorted) {
      breakable {
        val performances: (Double, Double, Double) = HammingLoss.evaluateHammingLossSensPrec(
          dfRealFeatureEdgeScoreDS,
          dfGroundTruth, "GroundTruth", scoresONcolName, scoresOFFcolName,
          "TimestampPrediction", applianceID, threshold._1, threshold._2, downsamplingBinPredictionSec, outputDirName)
        PerfoverThresholdBuffer.append((threshold, performances))
        if (performances._1 == HLwhenAlways0 || performances._2 == 0d) break // break out of the for loop
      }
    }


    val PerfoverThreshold: Array[((Double, Double), (Double, Double, Double))] = PerfoverThresholdBuffer.toArray
    println("Time for THRESHOLD + FINDPEAKS ESTRAZIONE ON_Time OFF_Time PREDICTED: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")


    PerfoverThreshold.foreach(x => println(x._1._1, x._1._2, x._2._1, x._2._2, x._2._3, HLwhenAlways0))


    PerfoverThreshold.foreach(x => bw.write("\napplianceID: " + applianceID.toString +
      ", thresholdON: " + x._1._1.toString + ", thresholdOFF: " + x._1._2.toString +
      ", HL: " + x._2._1.toString + ", HL0: " + HLwhenAlways0 +
      ", Sensitivity: " + x._2._2.toString + ", Precision: " + x._2._3.toString))

    // (applianceID, applianceName, ((thresholdON, thesholdOFF),(hammingLoss, sensitivity, precision)), hammingLoss0Model)
    (applianceID, applianceName, PerfoverThreshold, HLwhenAlways0)

  }



  def buildStoreDfGroundTruth(dfRealFeatureEdgeScoreDS: DataFrame,
                              dfTaggingInfo: DataFrame,
                              applianceID: Int,
                              downsamplingBinPredictionSec: Int,
                              outputDirName: String) = {

    var dateTime = DateTime.now()

    println("THRESHOLD + FINDPEAKS ESTRAZIONE ON_Time OFF_Time PREDICTED for applianceID" + applianceID.toString)
    dateTime = DateTime.now()

    println("add ground truth prediction")
    val onOffWindowsGroundTruth: Array[(Long, Long)] = dfTaggingInfo
      .filter(dfTaggingInfo("applianceID") === applianceID)
      .select("ON_Time", "OFF_Time").map(r => (r.getLong(0), r.getLong(1))).collect()

    val outputFilename = outputDirName + "/OnOffArrayGroundTruth_AppID" + applianceID.toString + ".txt"

    val stringOnOff: String = onOffWindowsGroundTruth.mkString("\n")
    Files.write(Paths.get(outputFilename), stringOnOff.getBytes(StandardCharsets.UTF_8))


    val dfGroundTruth: DataFrame = HammingLoss.addOnOffStatusToDF(dfRealFeatureEdgeScoreDS, onOffWindowsGroundTruth,
      "TimestampPrediction", "GroundTruth", downsamplingBinPredictionSec)

    println("Time for building dfGroudTruth: " + (DateTime.now().getMillis - dateTime.getMillis) + "ms")


    dfGroundTruth

  }



  def buildPredictionRealFeatureLoopOverAppliances(dfFeature: DataFrame,
                                                   dfEdgeSignatures: DataFrame,
                                                   dfTaggingInfoCurrent: DataFrame,
                                                   appliancesArray: Array[Int],
                                                   selectedFeature: String,
                                                   timestepsNumberPreEdge: Int, timestepsNumberPostEdge: Int,
                                                   downsamplingBinPredictionSize: Int,
                                                   nrThresholdsPerAppliance: Int,
                                                   partitionNumber: Int,
                                                   scoresONcolName: String,
                                                   scoresOFFcolName: String,
                                                   outputDirName: String, bw: BufferedWriter,
                                                   downsamplingBinPredictionSec: Int,
                                                   sc: SparkContext, sqlContext: SQLContext) = {

    val resultsOverAppliances: Array[(Int, String, Array[((Double, Double), (Double, Double, Double))], Double)] =

      appliancesArray.map { (applianceID: Int) =>

        val applianceName = dfTaggingInfoCurrent.filter(dfTaggingInfoCurrent("applianceID") === applianceID).head.getAs[String]("ApplianceName")


        println("\nAnalyzing appliance: " + applianceID.toString + " " + applianceName)

        bw.write("\n\nAnalyzing appliance: " + applianceID.toString + " " + applianceName)

        val dfRealFeatureEdgeScoreDS = EdgeDetection.buildStoreDfSimilarityScoreRealFeature(dfFeature,
          dfEdgeSignatures, applianceID, selectedFeature,
          timestepsNumberPreEdge, timestepsNumberPostEdge,
          downsamplingBinPredictionSize, partitionNumber,
          outputDirName, sc, sqlContext)

        val dfGroundTruth = EdgeDetection.buildStoreDfGroundTruth(dfRealFeatureEdgeScoreDS,
          dfTaggingInfoCurrent, applianceID, downsamplingBinPredictionSec, outputDirName).cache()

        println("Selecting threshold to test")
        val thresholdToTestSortedTrain: Array[(Double, Double)] = SimilarityScore.extractingUniformlySpacedThreshold(dfRealFeatureEdgeScoreDS,
          scoresONcolName, scoresOFFcolName, nrThresholdsPerAppliance)

        val resultsApplianceOverThresholds = EdgeDetection.buildPredictionEvaluateHLRealFeature(dfRealFeatureEdgeScoreDS,
          dfGroundTruth, thresholdToTestSortedTrain, applianceID, applianceName, selectedFeature, outputDirName, scoresONcolName,
          scoresOFFcolName, downsamplingBinPredictionSec, bw)

        dfRealFeatureEdgeScoreDS.unpersist()
        dfGroundTruth.unpersist()

        // Array(applianceID, applianceName, ((thresholdON, thresholdOFF),(hammingLoss, sensitivity, precision)), hammingLoss0Model)
        resultsApplianceOverThresholds

      }
    //save resultsOverAppliancesTrain
//    val temp: List[(Int, String, Array[(Double, Double)])] = resultsOverAppliances.toList
    val store = new ObjectOutputStream(new FileOutputStream(outputDirName + "/resultsOverAppliances.dat"))
    store.writeObject(resultsOverAppliances)
    store.close()


    //(applianceID, applianceName, thresholdON, thesholdOFF, sensitivity, precision, hammingLoss, hammingLoss0Model, hammingLoss/hammingLoss0Model)
    val bestResultOverAppliances =
      HammingLoss.extractingHLoverThresholdAndAppliances(resultsOverAppliances)

    bestResultOverAppliances

  }


  def buildStoreDfEdgeSignature[SelFeatureType](selFeatureTypeClass : Class[SelFeatureType],
                                                dfFeatures: DataFrame, dfTaggingInfo: DataFrame,
                                                selectedFeature: String,
                                                timestampIntervalPreEdge: Long,
                                                timestampIntervalPostEdge: Long,
                                                edgeWindowSize: Int,
                                                dfEdgeSignaturesFileName: String,
                                                sc: SparkContext, sqlContext: SQLContext) = {

    println("Selecting edge windows for a given feature")
    val dfEdgeWindows = EdgeDetection.selectingEdgeWindowsRealFromTagWithTimeIntervalSingleFeature(dfFeatures,
      dfTaggingInfo,
      selectedFeature, timestampIntervalPreEdge, timestampIntervalPostEdge, edgeWindowSize,
      sc, sqlContext)

    val dfEdgeWindowsTaggingInfo: DataFrame = dfEdgeWindows.join(dfTaggingInfo, "IDedge").cache()

    // Computing Edge signature for all the appliances given the feature to use
    println("COMPUTING EDGE SIGNATURE of a single Feature")
    val dfEdgeSignatures = EdgeDetection.computeEdgeSignatureAppliancesWithVar(dfEdgeWindowsTaggingInfo,
      edgeWindowSize, selectedFeature, selFeatureTypeClass,
      sc, sqlContext).cache()

    dfEdgeWindowsTaggingInfo.unpersist()

    val dfEdgeSignaturesGood = dfEdgeSignatures.
      filter(dfEdgeSignatures("ON_TimeSignature_" + selectedFeature).!==(dfEdgeSignatures("OFF_TimeSignature_" + selectedFeature)))

    // saving dfEdgeSignature
    val path: Path = Path (dfEdgeSignaturesFileName + ".avro")
    if (path.exists) {
      Try(path.deleteRecursively())
    }
    dfEdgeSignaturesGood.write
      .avro(dfEdgeSignaturesFileName + ".avro")

    CSVutils.storingSparkCsv(dfEdgeSignaturesGood, dfEdgeSignaturesFileName + ".csv")

    dfEdgeSignaturesGood
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