import java.io.{FileInputStream, ObjectInputStream}

import com.cgnal.enel.kaggle.utils.HammingLoss._

val outputDirName = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT_H4/ResultsTrainRecipMSDscore07_27_1343372401_07_27_1343372401_avg6_dw1_preInt5_postInt5_dwPrediction5"

val reader = new ObjectInputStream(new FileInputStream(outputDirName + "/resultsOverAppliances.dat"))
val resultsOverAppliances = reader.readObject().asInstanceOf[Array[(Int, String, Array[((Double, Double), Double)], Double)]]
reader.close()


val bestResultOverAppliances = extractingHLoverThresholdAndAppliances(resultsOverAppliances)

bestResultOverAppliances.foreach(tuple => println(tuple))
