package com.cgnal.enel.kaggle.utils

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.sql.functions.{row_number}
/**
  * Created by cavaste on 20/09/16.
  */
object CrossValidation {

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