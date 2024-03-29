package com.cgnal.enel.kaggle.utils

import org.apache.spark.sql.functions.udf

import scala.collection.mutable
import scala.math.pow

/**
  * Created by cavaste on 10/08/16.
  *
  * Complex number are implemented in the Dataframe as a Map[String,Double]
  * This object implements all the operations needed on complex numbers
  */
object ComplexMap {

  def sum(x: Map[String, Double], y: Map[String, Double]): Map[String, Double] = {
    x.map { case (k, v) => k -> (v + y.get(k).get) }
  }
  val complexSum = ((x: Map[String,Double], y: Map[String,Double]) => sum(x,y))
  val complexSumUDF = udf(complexSum)

  val realPart: (Map[String, Double]) => Double = ((x: Map[String,Double]) => x.get("re").get)
  val realPartUDF = udf(realPart)

  val imPart = ((x: Map[String,Double]) => x.get("im").get)
  val imPartUDF = udf(imPart)

  def subtraction(x: Map[String, Double], y: Map[String, Double]): Map[String, Double] = {
    x.map { case (k, v) => k -> (v - y.get(k).get) }
  }
  val complexSub = ((x: Map[String,Double], y: Map[String,Double]) => subtraction(x,y))
  val complexSubUDF = udf(complexSub)

  def conj(x: Map[String, Double]) = {
    x.map { case (k, v) => k -> (if (k == "im") -v else v) }
  }
  val complexConj: (Map[String, Double]) => Map[String, Double] = ((x: Map[String,Double]) => conj(x))
  val complexConjUDF = udf(complexConj)

  def abs(x: Map[String, Double]) = {
    Math.sqrt(pow((x.get("re").get), 2) + pow(x.get("im").get, 2))
  }
  val complexAbs: (Map[String, Double]) => Double = ((x: Map[String,Double]) => abs(x))
  val complexAbsUDF = udf(complexAbs)

  def prod(x: Map[String, Double], y: Map[String, Double]): Map[String, Double] = {
    Map(("re", x.get("re").get * y.get("re").get - x.get("im").get * y.get("im").get),
      ("im", x.get("re").get * y.get("im").get + x.get("im").get * y.get("re").get))
  }
  val complexProd = ((x: Map[String,Double], y: Map[String,Double]) => prod(x,y))
  val complexProdUDF = udf(complexProd)

  def quotDouble(x: Map[String, Double], y: Double): Map[String, Double] = {
    x.map { case (k, v) => k -> (v/y) }
  }

  def sumArray(x: Array[Map[String,Double]], y: Array[Map[String,Double]]): Array[Map[String, Double]] = {
    val xZipY = x.zip(y)
    xZipY.map(el => ComplexMap.sum(el._1, el._2))
  }

  def quotArray(x: Array[Map[String,Double]], y: Double): Array[Map[String, Double]] = {
    x.map(el => ComplexMap.quotDouble(el,y))
  }

}