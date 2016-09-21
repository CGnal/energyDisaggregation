package com.cgnal.enel.kaggle.utils

import com.cgnal.enel.kaggle.helpers.DatasetHelper
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, FunSuite}

import CrossValidation$Test._
/**
  * Created by cavaste on 20/09/16.
  */
class CrossValidation$Test extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach{

  override protected def beforeAll(): Unit = {
    conf = new SparkConf().setMaster("local[4]").setAppName("energyDisaggregation")
    sc = new SparkContext(conf)
    sqlContext = new SQLContext(sc)

    val filenameTaggingInfo = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT/Tagged_Training_07_27_1343372401/TaggingInfo.csv"
    arrayTaggingInfo = DatasetHelper.fromCSVtoArrayAddingRowIndex(filenameTaggingInfo)
    dfTaggingInfo = DatasetHelper.fromArrayIndexedToDFTaggingInfo(sc, sqlContext,
      arrayTaggingInfo, DatasetHelper.TagSchema)

  }

  override protected def afterAll(): Unit = {
    sc.stop()
  }


  test("check validation split"){
    val (dfTrain, dfValid) = CrossValidation.validationSplit(dfTaggingInfo, sqlContext)

    dfTrain.count
  }


}

object CrossValidation$Test {

  val filenameCSV = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/ExampleForCodeTest/testV.csv"
  var conf :SparkConf = _
  var sc :SparkContext = _
  var sqlContext : SQLContext= _
  var dfTaggingInfo: DataFrame = _
  var arrayV :Array[(Array[String], Int)] = _
  var arrayTaggingInfo: Array[(Array[String], Int)] = _

}