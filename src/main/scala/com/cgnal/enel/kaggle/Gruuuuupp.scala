package com.cgnal.enel.kaggle

/**
  * Created by cavaste on 06/09/16.
  */
object Gruuuuupp {

  def main(args: Array[String]) {

    val ints: List[Int] = List(1,2,3,4,5,1,2,3,4,5)
    val by: Map[Boolean, List[Int]] = ints.groupBy(_%2==0)
    println(by)
    println(by.map(x=>(x._1,x._2.distinct)))

    val frutta: List[Char] = ints.map("soca")

  }

}
