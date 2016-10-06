package com.cgnal.enel.kaggle

import java.nio.charset.StandardCharsets
import java.nio.file.{Paths, Files}
import breeze.numerics.round
import com.cgnal.enel.kaggle.models.edgeDetection.{SimilarityScore, EdgeDetection}
import com.cgnal.enel.kaggle.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.sum

import java.io._
import org.apache.spark.sql.expressions.{Window, WindowSpec}

import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{GroupedData, DataFrame, SQLContext, Row}
import com.databricks.spark.avro._
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}
/**
  * Created by cavaste on 21/09/16.
  */
object MainFake {

  def main2(args: Array[String]) = {

    val conf  = new SparkConf().setMaster("local[4]").setAppName("energyDisaggregation")
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val sc = new SparkContext(conf)
    //val sqlContext = new SQLContext(sc)
    val sqlContext= new SQLContext(sc)


    val data = Seq(
      Row(1l,	0l),
      Row(0l,	0l),
      Row(0l,	0l),
      Row(0l,	1l),
      Row(1l,	0l),
      Row(1l,	1l),
      Row(1l,	1l))

    val schema: StructType =
      StructType(
        StructField("Timestamp", LongType, false) ::
          StructField("feature", LongType, false) :: Nil)


    val dataDF: DataFrame = sqlContext.createDataFrame(sc.makeRDD(data), schema)
    val dfTP = dataDF.filter(dataDF("Timestamp") === 1 && dataDF("feature") === 1)
    val TP = dfTP.agg(sum("Timestamp")).head.getAs[Long](0)

    println(TP)


/*    val schema: StructType =
      StructType(
        StructField("Timestamp", LongType, false) ::
          StructField("feature", DoubleType, false) :: Nil)
    val dataDF: DataFrame = sqlContext.createDataFrame(sc.makeRDD(data), schema)


    val outputDirName = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT_H4/Train07_27_1343372401_07_27_1343372401_avg6_dw6_preInt5_postInt5"


    val reader = new ObjectInputStream(new FileInputStream(outputDirName + "/resultsOverAppliances.dat"))
    val test = reader.readObject().asInstanceOf[List[(Int, String, Array[(Double, Double)])]]
    reader.close()


    test.foreach(x => println(x))

    val reader2 = new ObjectInputStream(new FileInputStream(outputDirName + "/bestResultsOverAppliances.dat"))
    val test2 = reader2.readObject().asInstanceOf[List[(Int, String, Double, Double)]]
    reader2.close()

    test2.foreach(x => println(x))





*/














  }


}
