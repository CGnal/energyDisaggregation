package com.cgnal.enel.kaggle.helpers

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.immutable.IndexedSeq

/**
  * Created by cavaste on 10/08/16.
  */

object DatasetHelper {


  // questo structype serve per dire a Scala come leggere la roba dal dataframe SQL
  // I paramteri MapType, StringType etc... fanno sì che dopo Scala sappia che la roba dentro il dataframe (che è un oggetto di Spark)
  // è ruspettivamente una mappa, string etc...
  val Vschema: StructType =
    StructType(
      StructField("ID", IntegerType, false) ::
        StructField("Vfund", MapType(StringType, DoubleType), false) ::
        StructField("V1stH", MapType(StringType, DoubleType), false) ::
        StructField("V2ndH", MapType(StringType, DoubleType), false) ::
        StructField("V3ndH", MapType(StringType, DoubleType), false) ::
        StructField("V4ndH", MapType(StringType, DoubleType), false) ::
        StructField("V5ndH", MapType(StringType, DoubleType), false) :: Nil)

  val Ischema: StructType =
    StructType(
      StructField("ID", IntegerType, false) ::
        StructField("Ifund", MapType(StringType, DoubleType), false) ::
        StructField("I1stH", MapType(StringType, DoubleType), false) ::
        StructField("I2ndH", MapType(StringType, DoubleType), false) ::
        StructField("I3ndH", MapType(StringType, DoubleType), false) ::
        StructField("I4ndH", MapType(StringType, DoubleType), false) ::
        StructField("I5ndH", MapType(StringType, DoubleType), false) :: Nil)

  val VIschemaNoID: StructType =
    StructType(
      StructField("fund", MapType(StringType, DoubleType), false) ::
        StructField("1stH", MapType(StringType, DoubleType), false) ::
        StructField("2ndH", MapType(StringType, DoubleType), false) ::
        StructField("3ndH", MapType(StringType, DoubleType), false) ::
        StructField("4ndH", MapType(StringType, DoubleType), false) ::
        StructField("5ndH", MapType(StringType, DoubleType), false) :: Nil)

  val VItimeSchema: StructType =
    StructType(StructField("ID", IntegerType, false) ::
    StructField("Timestamp", TimestampType, false) :: Nil)

  val TagSchema: StructType =
    StructType(StructField("ApplianceID", IntegerType, false) ::
      StructField("ApplianceName", StringType, false) ::
      StructField("ON_Time", TimestampType, false) ::
      StructField("OFF_Time", TimestampType, false) :: Nil)


  def fromCSVwithComplexToDF(sc: SparkContext, sqlContext: SQLContext,
                             filenameCSV: String, schema: StructType): DataFrame = {

    val inputFile = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_26_1343286001/LF1V.csv"

    val input: RDD[String] = sc.textFile(filenameCSV)
    val tableRDD: RDD[Row] = input.map { line =>
      val reader: CSVReader = new CSVReader(new StringReader(line))
      val rowComplexTogether: Array[String] = reader.readNext()
      val complexNumber = rowComplexTogether.length

      if (complexNumber != schema.length) sys.error("schema length is not equal to the number of columns found in the CSV")
      val rowComplexSplit: Array[String] = rowComplexTogether.flatMap((complex: String) => complex.split("((?=(?<=\\d)(\\-|\\+))|[i])"))
      val rowComplexSplitDouble: Array[Double] = rowComplexSplit.map(x => x.toDouble)

      val complexKeys: List[Map[String, Double]] = (
        for (i <- 0 until complexNumber) yield Map(("re",rowComplexSplitDouble(2*i)), ("im", rowComplexSplitDouble(2*i+1)))
        ).toList

      Row(complexKeys: _*)
      // la row è un oggetto di scala (rappresenta una riga del dataframe) ed è quella che vuole Structype per creare poi il df con la struttura voluta
    }

    val df: DataFrame = sqlContext.createDataFrame(tableRDD, schema)
    df
  }


  def fromArrayIndexedToDF(sc: SparkContext, sqlContext: SQLContext,
                           indexedTable: Array[(Array[String], Int)], schema: StructType, complexFlag: Int): DataFrame = {

    val tableScala: Array[Row] = indexedTable.map{ line =>
      val rowComplexTogether: Array[String] = line._1
      val complexNumber = rowComplexTogether.length

      if (complexNumber != schema.length-1) sys.error("schema length is not equal to the number of columns found in the CSV")
      val valuesOnRow = if (complexFlag == 1) {
        val rowComplexSplit: Array[String] = rowComplexTogether.flatMap((complex: String) => complex.split("((?=(?<=\\d)(\\-|\\+))|[i])"))
        val rowComplexSplitDouble: Array[Double] = rowComplexSplit.map(x => x.toDouble)

        val valuesOnRow: List[Map[String, Double]] = (
          for (i <- 0 until complexNumber) yield Map(("re", rowComplexSplitDouble(2 * i)), ("im", rowComplexSplitDouble(2 * i + 1)))
          ).toList
        valuesOnRow
      }
      else {
        val rowComplexSplit: Array[String] = rowComplexTogether.flatMap((complex: String) => complex.split(" "))
        val valuesOnRow: List[Double] = rowComplexSplit.map(x => x.toDouble).toList
        valuesOnRow
      }

      val IDandComplexKeys =  line._2 :: valuesOnRow
      Row(IDandComplexKeys: _* )
    }

    val tableRDD = sc.parallelize(tableScala)
    val df: DataFrame = sqlContext.createDataFrame(tableRDD, schema)
    df
  }

}
