package com.cgnal.enel.kaggle.utils

import scala.collection.mutable.ArrayBuffer
import scala.io
/**
  * Created by cavaste on 12/08/16.
  */
object ManageDataset extends App {


  /**
    * read a CSV file and add an ID index
    * @param filename
    * @return Array of Tuple2 where the first element of the tuple is the row of the csv and the second the index of the row
    */
  def fromCSVtoArrayAddingRowIndex(filename: String):
  Array[(Array[String], Int)] = {
    // each row is an array of strings (the columns in the csv file)
    val rows = ArrayBuffer[Array[String]]()

    // (1) read the csv data
    val bufferedSource = io.Source.fromFile(filename)
    for (line <- bufferedSource.getLines) {
      rows += line.split(",").map(_.trim)
    }
    bufferedSource.close

    val indexedTable: Array[(Array[String], Int)] = rows.toArray.zipWithIndex
    indexedTable

  }
}

