package com.cgnal.enel.kaggle.helpers

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import com.cgnal.enel.kaggle.utils.ComplexMap
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

  val VIschemaNoID: StructType =
    StructType(
      StructField("fund", MapType(StringType, DoubleType), false) ::
        StructField("1H", MapType(StringType, DoubleType), false) ::
        StructField("2H", MapType(StringType, DoubleType), false) ::
        StructField("3H", MapType(StringType, DoubleType), false) ::
        StructField("4H", MapType(StringType, DoubleType), false) ::
        StructField("5H", MapType(StringType, DoubleType), false) :: Nil)

  // questo structype serve per dire a Scala come leggere la roba dal dataframe SQL
  // I paramteri MapType, StringType etc... fanno sì che dopo Scala sappia che la roba dentro il dataframe (che è un oggetto di Spark)
  // è ruspettivamente una mappa, string etc...
  val Vschema: StructType =
    StructType(
      StructField("IDtime", IntegerType, false) ::
        StructField("Vfund", MapType(StringType, DoubleType), false) ::
        StructField("V1H", MapType(StringType, DoubleType), false) ::
        StructField("V2H", MapType(StringType, DoubleType), false) ::
        StructField("V3H", MapType(StringType, DoubleType), false) ::
        StructField("V4H", MapType(StringType, DoubleType), false) ::
        StructField("V5H", MapType(StringType, DoubleType), false) :: Nil)

  val Ischema: StructType =
    StructType(
      StructField("IDtime", IntegerType, false) ::
        StructField("Ifund", MapType(StringType, DoubleType), false) ::
        StructField("I1H", MapType(StringType, DoubleType), false) ::
        StructField("I2H", MapType(StringType, DoubleType), false) ::
        StructField("I3H", MapType(StringType, DoubleType), false) ::
        StructField("I4H", MapType(StringType, DoubleType), false) ::
        StructField("I5H", MapType(StringType, DoubleType), false) :: Nil)

  val TSschema: StructType =
    StructType(StructField("IDtime", IntegerType, false) ::
      StructField("Timestamp", LongType, false) :: Nil)

  val TagSchema: StructType =
    StructType(
      StructField("IDedge", IntegerType, false) ::
        StructField("ApplianceID", IntegerType, false) ::
        StructField("ApplianceName", StringType, false) ::
        StructField("ON_Time", LongType, false) ::
        StructField("OFF_Time", LongType, false) :: Nil)

  val SampleSubmissionSchema: StructType =
    StructType(
      StructField("ID", IntegerType, false) ::
        StructField("House", StringType, false) ::
        StructField("ApplianceID", IntegerType, false) ::
        StructField("TimestampPrediction", LongType, false) ::
        StructField("Predicted", IntegerType, false) :: Nil)


  /**
    * read a csv of complex number and create a dataframe with no ID for the rows
    *
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

      val complexKeys: Array[Map[String, Double]] = (
        for (i <- 0 until complexNumber) yield Map(("re", rowComplexSplitDouble(2 * i)), ("im", rowComplexSplitDouble(2 * i + 1)))
        ).toArray

      Row(complexKeys: _*)
      // la row è un oggetto di scala (rappresenta una riga del dataframe) ed è quella che vuole Structype per creare poi il df con la struttura voluta
    }

    val df: DataFrame = sqlContext.createDataFrame(tableRDD, schema)
    df
  }

  /**
    * transform an Array of tuple with (content of the row, index of the row) in a dataframe with a column ID
    *
    * @param sc
    * @param sqlContext
    * @param indexedTable
    * @param schema
    * @param complexFlag 1 in case of complex numbers
    * @return
    */
  def fromArrayIndexedToDFTimestampOrFeatures(sc: SparkContext, sqlContext: SQLContext,
                                              indexedTable: Array[(Array[String], Int)], schema: StructType, complexFlag: Int,
                                              timestampFactor: Double = 1E7): DataFrame = {

    val tableScala: Array[Row] = indexedTable.map { line =>
      val rowString: Array[String] = line._1
      val rowLength = rowString.length

      if (rowLength != schema.length - 1) sys.error("schema length is not equal to the number of columns found in the CSV")
      val valuesOnRow = if (complexFlag == 1) {
        val rowComplexSplit: Array[String] = rowString.flatMap((complex: String) => complex.split("((?=(?<=\\d)(\\-|\\+))|[i])"))
        val rowComplexSplitDouble: Array[Double] = rowComplexSplit.map(x => x.toDouble)

        val valuesOnRow: Array[Map[String, Double]] = (
          for (i <- 0 until rowLength) yield Map(("re", rowComplexSplitDouble(2 * i)), ("im", rowComplexSplitDouble(2 * i + 1)))
          ).toArray
        valuesOnRow
      }
      else {
        val valuesOnRow: Array[Long] = rowString.map(x => (BigDecimal(x) * timestampFactor).toLongExact) //(x.toDouble*(timestampFactor)).round)
        valuesOnRow
      }

      val IDandComplexKeys = line._2 +: valuesOnRow
      Row(IDandComplexKeys: _*)
    }

    val tableRDD = sc.parallelize(tableScala)
    val df: DataFrame = sqlContext.createDataFrame(tableRDD, schema)
    df
  }


  def fromArrayIndexedToDFTaggingInfo(sc: SparkContext, sqlContext: SQLContext,
                                      indexedTable: Array[(Array[String], Int)], schema: StructType,
                                      timestampFactor: Double = 1E7): DataFrame = {

    val tableScala: Array[Row] = indexedTable.map { line =>
      val rowString: Array[String] = line._1
      val rowLength = rowString.length

      if (rowLength != schema.length - 1) sys.error("schema length is not equal to the number of columns found in the CSV")


      val valuesOnRow: Array[Any] = Array(rowString(0).toInt, rowString(1).toString.replace("\"", ""),
        (BigDecimal(rowString(2)) * timestampFactor).toLongExact, (BigDecimal(rowString(3)) * timestampFactor).toLongExact)


      val IDandComplexKeys = line._2 +: valuesOnRow
      Row(IDandComplexKeys: _*)
    }

    val tableRDD = sc.parallelize(tableScala)
    val df: DataFrame = sqlContext.createDataFrame(tableRDD, schema)
    df
  }

  /**
    * read a CSV file and add an ID index
    *
    * @param filename
    * @return Array of Tuple2 where the first element of the tuple is the row of the csv and the second the index of the row
    */
  def fromCSVtoArrayAddingRowIndex(filename: String, startingIndex: Int = 0): Array[(Array[String], Int)] = {
    // each row is an array of strings (the columns in the csv file)
    val rows = ArrayBuffer[Array[String]]()

    // (1) read the csv data
    val bufferedSource = io.Source.fromFile(filename)
    for (line <- bufferedSource.getLines) {
      rows += line.split(",").map(_.trim)
    }
    bufferedSource.close

    val indexedTable: Array[(Array[String], Int)] = rows.toArray.zipWithIndex

    indexedTable.map(tuple => (tuple._1, tuple._2 + startingIndex))

  }


  def importingDatasetToDfHouseDay(filenameCSV_V: String, filenameCSV_I: String,
                                   filenameTimestamp: String,
                                   sc: SparkContext, sqlContext: SQLContext): DataFrame = {

    val arrayV: Array[(Array[String], Int)] = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameCSV_V)
    val dfV: DataFrame = DatasetHelper.fromArrayIndexedToDFTimestampOrFeatures(sc, sqlContext,
      arrayV, DatasetHelper.Vschema, 1)

    val arrayI = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameCSV_I)
    val dfI: DataFrame = DatasetHelper.fromArrayIndexedToDFTimestampOrFeatures(sc, sqlContext,
      arrayI, DatasetHelper.Ischema, 1)

    val arrayTimestamp = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameTimestamp)
    val dfTS: DataFrame = DatasetHelper.fromArrayIndexedToDFTimestampOrFeatures(sc, sqlContext,
      arrayTimestamp, DatasetHelper.TSschema, 0)

    // dataframe with Voltage, Current and TimeTicks relative to a given Phase
    val dfVI: DataFrame = dfV.join(dfI, "IDtime")
      .join(dfTS, "IDtime")

    dfVI
  }


  def addPowerToDfFeatures(dfFeatures: DataFrame) = {
    // Adding Power = V * conj(I)
    val dfFeaturesPowerTemp = dfFeatures.withColumn("PowerFund", ComplexMap.complexProdUDF(dfFeatures("Vfund"), ComplexMap.complexConjUDF(dfFeatures("Ifund"))))
//      .withColumn("Power1H", ComplexMap.complexProdUDF(dfFeatures("V1H"), ComplexMap.complexConjUDF(dfFeatures("I1H"))))
//      .withColumn("Power2H", ComplexMap.complexProdUDF(dfFeatures("V2H"), ComplexMap.complexConjUDF(dfFeatures("I2H"))))
//      .withColumn("Power3H", ComplexMap.complexProdUDF(dfFeatures("V3H"), ComplexMap.complexConjUDF(dfFeatures("I3H"))))
//      .withColumn("Power4H", ComplexMap.complexProdUDF(dfFeatures("V4H"), ComplexMap.complexConjUDF(dfFeatures("I4H"))))
//      .withColumn("Power5H", ComplexMap.complexProdUDF(dfFeatures("V5H"), ComplexMap.complexConjUDF(dfFeatures("I5H"))))

    val dfFeaturesPower = dfFeaturesPowerTemp.withColumn("RealPowerFund", ComplexMap.realPartUDF(dfFeaturesPowerTemp("PowerFund")))
      .withColumn("ApparentPowerFund", ComplexMap.complexAbsUDF(dfFeaturesPowerTemp("PowerFund")))

    dfFeaturesPower
  }



/*  def fromCSVsampleSubmissiomToDF(sc: SparkContext, sqlContext: SQLContext,
                                  filenameCSV: String, schema: StructType): DataFrame = {

    val input: RDD[String] = sc.textFile(filenameCSV)
    val tableRDD: RDD[Row] = input.map { line =>
      val reader: CSVReader = new CSVReader(new StringReader(line))
      val rowTogether: Array[String] = reader.readNext()
      val columnNumber = rowTogether.length

      if (columnNumber != schema.length) sys.error("schema length is not equal to the number of columns found in the CSV")
      val rowComplexSplit: Array[String] = rowComplexTogether.flatMap((complex: String) => complex.split(",")
      val rowComplexSplitDouble: Array[Double] = rowComplexSplit.map(x => x.toDouble)

      val complexKeys: Array[Map[String, Double]] = (
        for (i <- 0 until complexNumber) yield Map(("re",rowComplexSplitDouble(2*i)), ("im", rowComplexSplitDouble(2*i+1)))
        ).toArray

      Row(complexKeys: _*)
      // la row è un oggetto di scala (rappresenta una riga del dataframe) ed è quella che vuole Structype per creare poi il df con la struttura voluta
    }

    val df: DataFrame = sqlContext.createDataFrame(tableRDD, schema)
    df
  }*/

}
