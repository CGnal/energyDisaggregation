package com.cgnal.enel.kaggle

import java.nio.file.{Paths, Files}
import java.util

import com.cgnal.efm.predmain.uta.timeseries.TimeSeriesUtils
import com.cgnal.enel.kaggle.helpers.DatasetHelper
import com.cgnal.enel.kaggle.models.EdgeDetection.EdgeDetection
import com.cgnal.enel.kaggle.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import java.io.{FileOutputStream, ObjectOutputStream, FileReader, StringReader}
import org.apache.spark.sql.expressions.{Window, WindowSpec}

import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import com.databricks.spark.avro._
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}

import com.cgnal.enel.kaggle.utils.Resampling
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by cavaste on 21/09/16.
  */
object MainFake {

  def main(args: Array[String]) = {

    val conf  = new SparkConf().setMaster("local[4]").setAppName("energyDisaggregation")
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val sc = new SparkContext(conf)
    //val sqlContext = new SQLContext(sc)
    val sqlContext: HiveContext = new HiveContext(sc)
    val filenameCSV = "/Users/aagostinelli/Desktop/EnergyDisaggregation/codeGitHub/ExampleForCodeTest/testV.csv"

    val data = Seq(
      Row(1l,	-101d),
      Row(2l,	100d),
      Row(3l,	105d),
      Row(4l,	-100d),
      Row(5l,	-90d),
      Row(6l,	-120d), Row(7l,	50d), Row(8l,	20d),
      Row(9l,	-20d), Row(10l,	-40d), Row(11l,	-50d), Row(12l,	30d),
      Row(13l,	-30d), Row(14l,	150d), Row(15l,	150d), Row(16l,	100d), Row(17l,	102d), Row(18l,	102d), Row(19l,	90d), Row(20l,	-70d),
      Row(21l,	-70d), Row(22l,	-70d),
      Row(23l,	-60d), Row(24l,	50d), Row(25l,	20d), Row(26l,	-20d), Row(27l,	-30d), Row(28l,	-30d) , Row(29l,	10d), Row(30l,	10d), Row(31l,	-30d)//
    )

    val schema: StructType =
      StructType(
        StructField("Timestamp", LongType, false) ::
          StructField("feature", DoubleType, false) :: Nil)
    val dataDF: DataFrame = sqlContext.createDataFrame(sc.makeRDD(data), schema)

    // SINGLE FEATURE SELECTED FEATURE TYPE: DOUBLE --------------------------------------------------------------------
    type SelFeatureType = Double
    val filenameDfEdgeWindowsFeature = "/Users/aagostinelli/Desktop/EnergyDisaggregation/shortSparkTest/dfEdgeWindowsApplianceProva.csv"
    val filenameSampleSubmission = "/Users/aagostinelli/Desktop/EnergyDisaggregation/SampleSubmission.csv"

    val timestepsNumberPreEdge = 4 // number of points in the interval
    val timestepsNumberPostEdge = 7 // number of points in the interval
    val edgeWindowSize = timestepsNumberPreEdge + timestepsNumberPostEdge + 1
    val selectedFeature = "RealPowerFund"

    println("3b. COMPUTING EDGE SIGNATURE of a single Feature")
    val (dfEdgeSignatures, dfAppliancesToPredict) =
      EdgeDetection.computeEdgeSignatureAppliancesWithVar[SelFeatureType](
        filenameDfEdgeWindowsFeature,
        edgeWindowSize, selectedFeature, classOf[SelFeatureType],
        filenameSampleSubmission, sc, sqlContext)

    dfEdgeSignatures.cache()
    dfAppliancesToPredict.cache()
    dfEdgeSignatures.show()
    dfAppliancesToPredict.show()

    val filenameDfFeatures = "/Users/aagostinelli/Desktop/EnergyDisaggregation/shortSparkTest/dfEdgeSignatures.csv"


    val appliances: Array[Int] = dfEdgeSignatures.select("ApplianceID").collect()
      .map(row => row.getAs[Int]("ApplianceID"))


  }


}
