val a = "-75.49444580078125-96.3192138671875i"
a.split("((?=(?<=\\d)(\\-|\\+))|[i])")
val b = "-0.023662954568862915+0.020649265497922897i"

val polcevera = "0.050381064414978027-0.37630116939544678i"

b.split("((?=(?<=\\d)(\\-|\\+))|[i])")

polcevera.split("((?=(?<=\\d)(\\-|\\+))|[i])")

val pippo = Map(("x",1),("y",2))

val pappo = Map(("x",3),("y",4))


import scala.math.pow

val d = pow(5,2)

val ori = "pippo"

val ente = "\"pippo\""


import org.apache.spark.sql._
val row = Row(1, true, "a string", null)

val firstValue = row.getInt(0)


val pairs = sql("SELECT key, value FROM src").rdd.map {
  case Row(key: Int, value: String) =>
    key -> value
}




