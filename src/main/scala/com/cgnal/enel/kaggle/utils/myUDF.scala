package com.cgnal.enel.kaggle.utils

import org.apache.spark.sql.functions._

/**
  * Created by cavaste on 28/09/16.
  */
object myUDF {

  val reciprocalDouble: (Double) => Double = ((x: Double) => 1/(x+1E-2))
  val reciprocalDoubleUDF = udf(reciprocalDouble)

}
