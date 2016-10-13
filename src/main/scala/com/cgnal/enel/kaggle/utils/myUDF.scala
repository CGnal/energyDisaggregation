package com.cgnal.enel.kaggle.utils

import org.apache.spark.sql.functions._

/**
  * Created by cavaste on 28/09/16.
  */
object myUDF {

  val reciprocalDouble: (Double) => Double = ((x: Double) => 1/(x+1E-2))
  val reciprocalDoubleUDF = udf(reciprocalDouble)

  val netComplexPower = ((xFund: Map[String,Double], x1: Map[String,Double],
    x2: Map[String,Double], x3: Map[String,Double],
    x4: Map[String,Double], x5: Map[String,Double]) => ComplexMap.sum(ComplexMap.sum(ComplexMap.sum(ComplexMap.sum(ComplexMap.sum(xFund,x1),x2),x3),x4),x5))
  val netComplexPowerUDF = udf(netComplexPower)
}
