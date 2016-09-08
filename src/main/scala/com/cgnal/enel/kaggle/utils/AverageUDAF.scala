package com.cgnal.enel.kaggle.utils

/**
  * Created by cavaste on 08/09/16.
  */
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

//Extend UserDefinedAggregateFunction to write custom aggregate function
//You can also specify any constructor arguments. For instance you
//can have CustomMean(arg1: Int, arg2: String)
class CustomMeanComplex(arraySize: Int) extends UserDefinedAggregateFunction {

  // Input Data Type Schema
  def inputSchema: StructType = StructType(Array(StructField("item", ArrayType(MapType(StringType, DoubleType)))))

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
    buffer(0) = Array.fill(arraySize)(Map(("re", 0.toDouble), ("im", 0.toDouble))) // set sum to zero
    buffer(1) = 0L // set number of items to 0
  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row) = {
    buffer(0) = ComplexMap.sumArray(buffer.getAs[Array[Map[String,Double]]](0), input.getAs[Array[Map[String,Double]]](0))
    buffer(1) = buffer.getLong(1) + 1
  }

  // Merge two partial aggregates
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1(0) = ComplexMap.sumArray(buffer1.getAs[Array[Map[String,Double]]](0), buffer2.getAs[Array[Map[String,Double]]](0))
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // Called after all the entries are exhausted.
  def evaluate(buffer: Row) = {
    ComplexMap.quotArray(buffer.getAs[Array[Map[String,Double]]](0), buffer.getLong(1).toDouble)
  }

}
