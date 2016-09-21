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
class AverageOverComplex(fieldname: String, arraySize: Int) extends UserDefinedAggregateFunction {

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
    buffer(0) = Array.fill(arraySize)(Map(("re", 0.toDouble), ("im", 0.toDouble))) // set sum to zero
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




class AverageOverReal(fieldname: String, arraySize: Int) extends UserDefinedAggregateFunction {

  // Input Data Type Schema
  def inputSchema: StructType = StructType(Array(StructField(fieldname, ArrayType(DoubleType))))

  // Intermediate Schema
  def bufferSchema = StructType(Array(
    StructField("sum", ArrayType(DoubleType)),
    StructField("cnt", LongType)
  ))

  // Returned Data Type .
  def dataType: DataType = ArrayType(DoubleType)

  // Self-explaining
  def deterministic = true

  // This function is called whenever key changes
  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = Array.fill(arraySize)(0.toDouble) // set sum to zero
    buffer(1) = 0L // set number of items to 0
  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row) = {

    val buffer0Array = buffer.getAs[mutable.WrappedArray[Double]](0)
      .toArray[Double]
    val buffer1Array = input.getAs[mutable.WrappedArray[Double]](0)
      .toArray[Double]
    val xZipY = buffer0Array.zip(buffer1Array)
    buffer(0) = xZipY.map(el => el._1 + el._2)

    buffer(1) = buffer.getLong(1) + 1
  }

  // Merge two partial aggregates
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    val buffer0Array = buffer1.getAs[mutable.WrappedArray[Double]](0)
      .toArray[Double]
    val buffer1Array = buffer2.getAs[mutable.WrappedArray[Double]](0)
      .toArray[Double]
    val xZipY = buffer0Array.zip(buffer1Array)
    buffer1(0) = xZipY.map(el => el._1 + el._2)

    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // Called after all the entries are exhausted.
  def evaluate(buffer: Row) = {
    val finalArray = buffer.getAs[mutable.WrappedArray[Double]](0).toArray[Double]
    finalArray.map(el => el/buffer.getLong(1).toDouble)
  }

}




///////////////////////////////////// Compute Variance wrt previously computed average
// We have a field for input dat on which the average was computed
// and a field contianing the average itslef.

class VarianceOverReal(fieldname: String, fieldnameSignature: String, arraySize: Int) extends UserDefinedAggregateFunction {

  // Input Data Type Schema
  //  def inputSchema: StructType = StructType(Array(StructField(fieldname, ArrayType(DoubleType))))

  def inputSchema: StructType = StructType(Array(
    StructField(fieldname, ArrayType(DoubleType)),  // colonna 0
    StructField(fieldnameSignature, ArrayType(DoubleType))))  // colonna 1

  // Intermediate Schema
  def bufferSchema = StructType(Array(
    StructField("sum", ArrayType(DoubleType)), // colonna 0
    StructField("cnt", LongType)  // colonna 1
  ))

  // Returned Data Type .
  def dataType: DataType = ArrayType(DoubleType)

  // Self-explaining
  def deterministic = true

  // This function is called whenever key changes
  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = Array.fill(arraySize)(0.toDouble) // set squared difference to zero
    buffer(1) = 0L // set number of items to 0
  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row) = {
    // previous buffer, accorindg to buffer schema
    val buffer0Array = buffer.getAs[mutable.WrappedArray[Double]](0)
      .toArray[Double]
    // new input, current array
    val inputFeatureCurrent = input.getAs[mutable.WrappedArray[Double]](0)
      .toArray[Double]
    // new iput, Signature
    val inputSignature = input.getAs[mutable.WrappedArray[Double]](1)
      .toArray[Double]

    val featureZipSignature = inputFeatureCurrent.zip(inputSignature)
    val currentDeltaSquared = featureZipSignature.map(el => (el._1 - el._2) * (el._1 - el._2))

    //accoppia i  2 arrays
    val xZipY: Array[(Double, Double)] = buffer0Array.zip(currentDeltaSquared)
    buffer(0) = xZipY.map(el => el._1 + el._2)
    buffer(1) = buffer.getLong(1) + 1
  }

  // Merge two partial aggregates
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    val buffer0Array = buffer1.getAs[mutable.WrappedArray[Double]](0)
      .toArray[Double]
    val buffer1Array = buffer2.getAs[mutable.WrappedArray[Double]](0)
      .toArray[Double]
    val xZipY = buffer0Array.zip(buffer1Array)
    buffer1(0) = xZipY.map(el => el._1 + el._2)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // Called after all the entries are exhausted.
  def evaluate(buffer: Row) = {
    val finalArray = buffer.getAs[mutable.WrappedArray[Double]](0).toArray[Double]
    finalArray.map(el => el/(buffer.getLong(1).toDouble-1.))
  }

}




class MSDwithRealFeature(selectedFeature: String, characteristicSignal: Array[Double]) extends UserDefinedAggregateFunction {

  val windowSize = characteristicSignal.length

  // Input Data Type Schema
  def inputSchema: StructType = StructType(
    StructField("Timestamp", LongType, false) ::
    StructField(selectedFeature, DoubleType, false) :: Nil)

  // Intermediate Schema
  def bufferSchema = StructType(Array(
    StructField("window", MapType(LongType,DoubleType)),
    StructField("cnt", LongType)
  ))

  // Returned Data Type .
  def dataType: DataType = DoubleType

  // Self-explaining
  def deterministic = true

  // This function is called whenever key changes
  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) =  Map[Long,Double]() // set sum to zero
    buffer(1) = 0L // set number of items to 0
  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row) = {
    val currentMap: Map[Long, Double] = buffer.getAs[Map[Long,Double]](0)
    buffer(0) = currentMap + (input.getLong(0) -> input.getDouble(1))
    buffer(1) = buffer.getLong(1) + 1
  }

  // Merge two partial aggregates
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1(0) = buffer1.getAs[Map[Long,Double]](0) ++ buffer2.getAs[Map[Long,Double]](0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // Called after all the entries are exhausted.
  def evaluate(buffer: Row) = {
    val completeMap = buffer.getAs[Map[Long,Double]](0)
    val sortedKey: Seq[(Long, Int)] = completeMap.keySet.toSeq.sorted.zipWithIndex

    val msd: Double = sortedKey.map(x => {
      Math.pow(x._1 - characteristicSignal(x._2),2)
    }).sum

    msd
  }
}



class ProvaUDAF extends UserDefinedAggregateFunction {

  // Input Data Type Schema
  def inputSchema: StructType = StructType(
    StructField("IDtime", IntegerType, false) :: Nil)

  // Intermediate Schema
  def bufferSchema = StructType(Array(
    StructField("window", IntegerType),
    StructField("cnt", LongType)
  ))

  // Returned Data Type .
  def dataType: IntegerType = IntegerType

  // Self-explaining
  def deterministic = true

  // This function is called whenever key changes
  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = 0 // set sum to zero
    buffer(1) = 0 // set number of items to 0
  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row) = {
    val currentMap = buffer.getAs[Int](0)
    buffer(0) = currentMap + input.getInt(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  // Merge two partial aggregates
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // Called after all the entries are exhausted.
  def evaluate(buffer: Row) = {
    buffer.getAs[Int](0)
  }
}
