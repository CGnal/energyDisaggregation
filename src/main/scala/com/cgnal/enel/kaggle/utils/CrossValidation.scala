package com.cgnal.enel.kaggle.utils

import java.util

import com.cgnal.enel.kaggle.helpers.DatasetHelper
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.sql.functions.{row_number}

import scala.reflect.io.Path
import scala.util.Try

/**
  * Created by cavaste on 20/09/16.
  */
object CrossValidation {



  def creatingDfFeatureFixedHouseAndDay(dayFolder: String, house: String, outputDirName: String,
                                        sc: SparkContext, sqlContext: SQLContext,
                                        readingFromFileLabel: Int = 0) = {


    val filenameCSV_V = ReferencePath.datasetDirPath + house + "/Tagged_Training_" + dayFolder + "/LF1V.csv"
    val filenameCSV_I = ReferencePath.datasetDirPath + house + "/Tagged_Training_" + dayFolder + "/LF1I.csv"
    val filenameTimestamp = ReferencePath.datasetDirPath + house + "/Tagged_Training_" + dayFolder + "/TimeTicks1.csv"


    val filenameDfFeaturesSingleDay = outputDirName + "/dfFeature.csv"


    val dfFeaturesSingleDay = if (readingFromFileLabel == 0) {
      val dfVI = DatasetHelper.importingDatasetToDfHouseDay(filenameCSV_V, filenameCSV_I,
        filenameTimestamp,
        sc, sqlContext)
      val dfFeatures = DatasetHelper.addPowerToDfFeatures(dfVI)
      dfFeatures.printSchema()

      val path: Path = Path(filenameDfFeaturesSingleDay)
      if (path.exists) {
        Try(path.deleteRecursively())
      }
      dfFeatures.write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .save(filenameDfFeaturesSingleDay)
      dfFeatures
    }
    else {
      sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true") // Use first line of all files as header
        .option("inferSchema", "true")
        .load(filenameDfFeaturesSingleDay)
    }

    dfFeaturesSingleDay
  }





    def creatingDfFeatureFixedHouseOverDays(dayFolderArray: Array[String], house: String,
                                            sc: SparkContext, sqlContext: SQLContext,
                                            readingFromFileLabel: Int = 0) = {

      val dirNameDataset = ReferencePath.datasetDirPath + house

      val filenameDfFeaturesOverDays = dirNameDataset + "/dfFeature.csv"

      val dfFeaturesOverDays = if (readingFromFileLabel == 0) {

        val rowNumberPerDay =
          dayFolderArray.map(dayFolder => {
            val filenameCSV_V = dirNameDataset + "/Tagged_Training_" + dayFolder + "/LF1V.csv"

            DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameCSV_V).length
          })

        val incrementalRowNumberPerDay: Array[Int] = Array(0, rowNumberPerDay.dropRight(1).toList:_*)
        val dayFolderArrayWithRowNumber = dayFolderArray.zip(incrementalRowNumberPerDay)

        val dfFeaturesOverDaysTemp: DataFrame =
          dayFolderArrayWithRowNumber.map(tuple => {

            val dayFolder = tuple._1
            val filenameCSV_V = dirNameDataset + "/Tagged_Training_" + dayFolder + "/LF1V.csv"
            val filenameCSV_I = dirNameDataset + "/Tagged_Training_" + dayFolder + "/LF1I.csv"
            val filenameTimestamp = dirNameDataset + "/Tagged_Training_" + dayFolder + "/TimeTicks1.csv"


            val arrayV: Array[(Array[String], Int)] = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameCSV_V, tuple._2)
            val dfV: DataFrame = DatasetHelper.fromArrayIndexedToDFTimestampOrFeatures(sc, sqlContext,
              arrayV, DatasetHelper.Vschema, 1)

            val arrayI = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameCSV_I, tuple._2)
            val dfI: DataFrame = DatasetHelper.fromArrayIndexedToDFTimestampOrFeatures(sc, sqlContext,
              arrayI, DatasetHelper.Ischema, 1)

            val arrayTimestamp = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameTimestamp, tuple._2)
            val dfTS: DataFrame = DatasetHelper.fromArrayIndexedToDFTimestampOrFeatures(sc, sqlContext,
              arrayTimestamp, DatasetHelper.TSschema, 0)

            // dataframe with Voltage, Current and TimeTicks relative to a given Phase
            val dfVI: DataFrame = dfV.join(dfI, "IDtime")
              .join(dfTS, "IDtime")


            val dfFeatures = DatasetHelper.addPowerToDfFeatures(dfVI)
            dfFeatures
          }).reduceLeft((x, y) => x.unionAll(y))


        val path: Path = Path(filenameDfFeaturesOverDays)
        if (path.exists) {
          Try(path.deleteRecursively())
        }
        dfFeaturesOverDaysTemp.write
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .save(filenameDfFeaturesOverDays)

        dfFeaturesOverDaysTemp
      }
      else {
        sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true") // Use first line of all files as header
          .option("inferSchema", "true")
          .load(filenameDfFeaturesOverDays)
      }


      dfFeaturesOverDays

    }




  def creatingDfTaggingInfoFixedHouseAndDay(dayFolder: String, house: String,
                                            sc: SparkContext, sqlContext: SQLContext): DataFrame = {


    val filenameTaggingInfo = ReferencePath.datasetDirPath + house + "/Tagged_Training_" + dayFolder + "/TaggingInfo.csv"


    val arrayTaggingInfo: Array[(Array[String], Int)] = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameTaggingInfo)
    val dfTaggingInfo: DataFrame = DatasetHelper.fromArrayIndexedToDFTaggingInfo(sc, sqlContext,
      arrayTaggingInfo, DatasetHelper.TagSchema)
    dfTaggingInfo

  }




  def creatingDfTaggingInfoFixedHouseOverDays(dayFolderArray: Array[String], house: String,
                                              sc: SparkContext, sqlContext: SQLContext): DataFrame = {

    val dirNameDataset = ReferencePath.datasetDirPath + house

    val rowNumberPerDay =
      dayFolderArray.map(dayFolder => {
        val filenameTaggingInfo = dirNameDataset + "/Tagged_Training_" + dayFolder + "/TaggingInfo.csv"

        DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameTaggingInfo).length
      })

    val incrementalRowNumberPerDay: Array[Int] = Array(0, rowNumberPerDay.dropRight(1).toList:_*)
    val dayFolderArrayWithRowNumber = dayFolderArray.zip(incrementalRowNumberPerDay)


    val dfTaggingInfoOverDays = dayFolderArrayWithRowNumber.map(tuple => {
      val dayFolder = tuple._1
      val filenameTaggingInfo = dirNameDataset + "/Tagged_Training_" + dayFolder + "/TaggingInfo.csv"

      val arrayTaggingInfo: Array[(Array[String], Int)] = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameTaggingInfo, tuple._2)
      val dfTaggingInfo: DataFrame = DatasetHelper.fromArrayIndexedToDFTaggingInfo(sc, sqlContext,
        arrayTaggingInfo, DatasetHelper.TagSchema)
      dfTaggingInfo
    }).reduceLeft((x, y) => x.unionAll(y))

    dfTaggingInfoOverDays
  }









  def validationSplit(dfTag: DataFrame, sqlContext: SQLContext) = {
    val dfCount: DataFrame = dfTag.groupBy("ApplianceID").count()

    val dfTagCount = dfTag.join(dfCount, "ApplianceID")
    val dfRepartition = dfTagCount.repartition(dfTagCount("ApplianceID"))

    val rddIDedgeAppliance: RDD[Row] = dfRepartition.mapPartitions((rows: Iterator[Row]) => {
      val rowAndIndex: Iterator[(Row, Int)] = rows.zipWithIndex
      val rowWithIndex: Iterator[Row] = rowAndIndex.map((el: (Row, Int)) => {
        val idEdge = el._1.getAs[Integer]("IDedge")
        val applianceID = el._1.getAs[Integer]("ApplianceID")
        val applianceName = el._1.getAs[String]("ApplianceName")
        val on_Time = el._1.getAs[Long]("ON_Time")
        val off_Time = el._1.getAs[Long]("OFF_Time")
        val count = el._1.getAs[Long]("count")
        Row(applianceID,idEdge,applianceName,on_Time,off_Time,count,el._2)
      })
      rowWithIndex
    })

    val dfIDedgeApplianceTemp = sqlContext.createDataFrame(rddIDedgeAppliance,
      dfRepartition.schema.add("IDedgeAppliance", IntegerType, false))

    val dfIDedgeAppliance = dfIDedgeApplianceTemp.repartition(dfIDedgeApplianceTemp("ApplianceID"))

    val rddTrain = dfIDedgeAppliance.mapPartitions(rows => {
      rows.filter(row => {
        val numberRows: Long = row.getAs[Long]("count")
        val numberRowsTraining = Math.round(numberRows/10d*8).toInt
        (row.getAs[Integer]("IDedgeAppliance") + 1 <= numberRowsTraining)
      })
    })

    val dfTrain = sqlContext.createDataFrame(rddTrain,dfIDedgeAppliance.schema)

    val rddValid = dfIDedgeAppliance.mapPartitions(rows => {
      rows.filter(row => {
        val numberRows: Long = row.getAs[Long]("count")
        val numberRowsTraining = Math.round(numberRows/10d*8).toInt
        (row.getAs[Integer]("IDedgeAppliance") + 1 > numberRowsTraining)
      })
    })

    val dfValid = sqlContext.createDataFrame(rddValid,dfIDedgeAppliance.schema)

    (dfTrain, dfValid)
  }

}
