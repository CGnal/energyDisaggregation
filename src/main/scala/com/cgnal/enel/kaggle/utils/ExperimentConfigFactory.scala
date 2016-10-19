package com.cgnal.enel.kaggle.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by cavaste on 19/10/16.
  */
object ExperimentConfigFactory {

  def sparkContextInitializer(sparkMaster: String, appName: String) = {

    val conf = new SparkConf().setMaster(sparkMaster).setAppName(appName)
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val sqlContext = new HiveContext(sc)

    (sc, sqlContext)
  }

}
