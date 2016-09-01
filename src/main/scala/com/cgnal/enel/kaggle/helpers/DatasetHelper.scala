package com.cgnal.enel.kaggle.helpers

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ArrayBuffer
import scala.io

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
        StructField("V1H", MapType(StringType, DoubleType), false) ::
        StructField("V2H", MapType(StringType, DoubleType), false) ::
        StructField("V3H", MapType(StringType, DoubleType), false) ::
        StructField("V4H", MapType(StringType, DoubleType), false) ::
        StructField("V5H", MapType(StringType, DoubleType), false) :: Nil)

  val Ischema: StructType =
    StructType(
      StructField("ID", IntegerType, false) ::
        StructField("Ifund", MapType(StringType, DoubleType), false) ::
        StructField("I1H", MapType(StringType, DoubleType), false) ::
        StructField("I2H", MapType(StringType, DoubleType), false) ::
        StructField("I3H", MapType(StringType, DoubleType), false) ::
        StructField("I4H", MapType(StringType, DoubleType), false) ::
        StructField("I5H", MapType(StringType, DoubleType), false) :: Nil)

  val VIschemaNoID: StructType =
    StructType(
      StructField("fund", MapType(StringType, DoubleType), false) ::
        StructField("1H", MapType(StringType, DoubleType), false) ::
        StructField("2H", MapType(StringType, DoubleType), false) ::
        StructField("3H", MapType(StringType, DoubleType), false) ::
        StructField("4H", MapType(StringType, DoubleType), false) ::
        StructField("5H", MapType(StringType, DoubleType), false) :: Nil)

  val TSschema: StructType =
    StructType(StructField("ID", IntegerType, false) ::
    StructField("Timestamp", TimestampType, false) :: Nil)

  val TagSchema: StructType =
    StructType(StructField("ApplianceID", IntegerType, false) ::
      StructField("ApplianceName", StringType, false) ::
      StructField("ON_Time", TimestampType, false) ::
      StructField("OFF_Time", TimestampType, false) :: Nil)

  /**
    * read a csv of complex number and create a dataframe with no ID for the rows
    * @deprecated
    * @param sc
    * @param sqlContext
    * @param filenameCSV
    * @param schema
    * @return
    */
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

  /**
    * transform an Array of tuple with (content of the row, index of the row) in a dataframe with a column ID
    * @param sc
    * @param sqlContext
    * @param indexedTable
    * @param schema
    * @param complexFlag 1 in case of complex numbers
    * @return
    */
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

  /**
    * read a CSV file and add an ID index
    * @param filename
    * @return Array of Tuple2 where the first element of the tuple is the row of the csv and the second the index of the row
    */
  def fromCSVtoArrayAddingRowIndex(filename: String):
  Array[(Array[String], Int)] = {
    // each row is an array of strings (the columns in the csv file)
    val rows = ArrayBuffer[Array[String]]()

    // (1) read the csv data
    val bufferedSource = io.Source.fromFile(filename)
    for (line <- bufferedSource.getLines) {
      rows += line.split(",").map(_.trim)
    }
    bufferedSource.close

    val indexedTable: Array[(Array[String], Int)] = rows.toArray.zipWithIndex
    indexedTable

  }

}
