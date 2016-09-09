package com.cgnal.enel.kaggle.utils

/**
  * Created by cavaste on 08/09/16.
  */
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable

//Extend UserDefinedAggregateFunction to write custom aggregate function
//You can also specify any constructor arguments. For instance you
//can have CustomMean(arg1: Int, arg2: String)
class CustomMeanComplex(fieldname: String, arraySize: Int) extends UserDefinedAggregateFunction {

  // Input Data Type Schema
  def inputSchema: StructType = StructType(Array(StructField(fieldname, ArrayType(MapType(StringType, DoubleType)))))

  // Intermediate Schema
  def bufferSchema = StructType(Array(
    StructField("sum", ArrayType(MapType(StringType, DoubleType))),
    StructField("cnt", LongType)
  ))

  // Returned Data Type .
  def dataType: DataType = ArrayType(MapType(StringType, DoubleType))

  // Self-explaining
  def deterministic = true

  // This function is called whenever key changes
  def initialize(buffer: MutableAggregationBuffer) = {
    val ente: Array[Map[String, Double]] = Array.fill(arraySize)(Map(("re", 0.toDouble), ("im", 0.toDouble))) // set sum to zero
    buffer(0) = ente
    buffer(1) = 0L // set number of items to 0
  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row) = {

    val buffer0Array= buffer.getAs[mutable.WrappedArray[Map[String,Double]]](0)
      .toArray[Map[String,Double]]
    val buffer1Array = input.getAs[mutable.WrappedArray[Map[String,Double]]](0)
      .toArray[Map[String,Double]]
    buffer(0) = ComplexMap.sumArray(buffer0Array, buffer1Array)
    buffer(1) = buffer.getLong(1) + 1
  }

  // Merge two partial aggregates
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    val buffer0Array = buffer1.getAs[mutable.WrappedArray[Map[String,Double]]](0)
      .toArray[Map[String,Double]]
    val buffer1Array = buffer2.getAs[mutable.WrappedArray[Map[String,Double]]](0)
      .toArray[Map[String,Double]]
    buffer1(0) = ComplexMap.sumArray(buffer0Array, buffer1Array)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // Called after all the entries are exhausted.
  def evaluate(buffer: Row) = {
    ComplexMap.quotArray(buffer.getAs[mutable.WrappedArray[Map[String,Double]]](0).toArray[Map[String,Double]],
      buffer.getLong(1).toDouble)
  }

}
