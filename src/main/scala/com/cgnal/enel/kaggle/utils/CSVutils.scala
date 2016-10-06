package com.cgnal.enel.kaggle.utils

import java.io.{PrintWriter, BufferedWriter, File, FileWriter}
import org.apache.spark.sql.DataFrame

import scala.io.Source
import scala.reflect.io.Path
import scala.util.Try

/**
  * Created by cavaste on 21/09/16.
  */
object CSVutils {


  def storingSparkCsv(df: DataFrame,
                      outputFileName: String) = {

    val path: Path = Path (outputFileName)
    if (path.exists) {
      Try(path.deleteRecursively())
    }
    df.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(outputFileName)
  }



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