package com.cgnal.enel.kaggle.utils

import org.apache.spark.sql.functions.udf

import scala.math.pow

/**
  * Created by cavaste on 10/08/16.
  */
object ComplexMap {

  def sum(x: Map[String, Double], y: Map[String, Double]): Map[String, Double] = {
    x.map { case (k, v) => k -> (v + y.get(k).get) }
  }

  def subtraction(x: Map[String, Double], y: Map[String, Double]): Map[String, Double] = {
    x.map { case (k, v) => k -> (v + y.get(k).get) }
  }

  def conj(x: Map[String, Double]) = {
    x.map { case (k, v) => k -> (if (k == "im") -v else v) }
  }

  def abs(x: Map[String, Double]) = {
    pow((x.get("re").get), 2) + pow(x.get("im").get, 2)
  }

  def prod(x: Map[String, Double], y: Map[String, Double]): Map[String, Double] = {
    Map(("re", x.get("re").get * y.get("re").get - x.get("im").get * y.get("im").get),
      ("im", x.get("re").get * y.get("im").get + x.get("im").get * y.get("re").get))
  }





}