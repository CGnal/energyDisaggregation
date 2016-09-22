package com.cgnal.enel.kaggle.utils

import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source

/**
  * Created by cavaste on 21/09/16.
  */
object CSVutils {

  def Juxtappose(
                  fileNameCSV1: String,
                  fileNameCSV2: String,
                  fileNameOutCSV: String,
                  separator: String) = {

    val lines1 = Source.fromFile(fileNameCSV1).getLines()
    val lines2 = Source.fromFile(fileNameCSV2).getLines()

    val file = new File(fileNameOutCSV)
    val bw = new BufferedWriter(new FileWriter(file))

    lines1.zip(lines2).foreach(tuple => {
      bw.write(tuple._1 + separator + " " + tuple._2)
      bw.newLine()
    })

    bw.close()
  }

}