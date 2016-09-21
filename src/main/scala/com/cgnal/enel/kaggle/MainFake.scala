package com.cgnal.enel.kaggle

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


    val conf = new SparkConf().setMaster("local[4]").setAppName("energyDisaggregation")
    val sc = new SparkContext(conf)
    //    val sqlContext = new SQLContext(sc)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val sqlContext = new HiveContext(sc)


    val data = Seq(
      Row(1l,	-101d),
      Row(2l,	100d),
      Row(3l,	105d),  //
      Row(4l,	-100d),
      Row(5l,	-90d),
      Row(6l,	-120d),//
      Row(7l,	50d),//
      Row(8l,	20d),
      Row(9l,	-20d),
      Row(10l,	-40d),
      Row(11l,	-50d), //
      Row(12l,	30d),
      Row(13l,	-30d),
      Row(14l,	150d),//
      Row(15l,	150d),
      Row(16l,	100d),
      Row(17l,	102d),
      Row(18l,	102d),
      Row(19l,	90d),
      Row(20l,	-70d),//
      Row(21l,	-70d),
      Row(22l,	-70d),
      Row(23l,	-60d),
      Row(24l,	50d),//
      Row(25l,	20d),
      Row(26l,	-20d),
      Row(27l,	-30d),//
      Row(28l,	-30d) ,
      Row(29l,	10d),//
      Row(30l,	10d),
      Row(31l,	-30d)//
    )

    val schema: StructType =
      StructType(
        StructField("Timestamp", LongType, false) ::
          StructField("feature", DoubleType, false) :: Nil)
    val dataDF: DataFrame = sqlContext.createDataFrame(sc.makeRDD(data), schema)
    dataDF.printSchema()

    val averagedDF: DataFrame =  Resampling.movingAverageReal(dataDF,selectedFeature="feature",slidingWindowSize=3,
      "IDtime")
    //println(averagedDF.take(1)(2).getDouble(0))
    averagedDF.show()
    println("blablabl4: " + averagedDF.take(1)(0).get(2))//.getDouble(0)

  }


}
