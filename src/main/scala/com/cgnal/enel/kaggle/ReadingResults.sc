import java.io.{FileInputStream, ObjectInputStream}

import com.cgnal.enel.kaggle.utils.HammingLoss._

val outputDirName = "/Users/cavaste/ProjectsResultsData/EnergyDisaggregation/dataset/CSV_OUT_H4/ResultsRealPowerFund_FirstDiff/TrainRecipMSD_07_27_1343372401_07_27_1343372401_avg6_dw1_preInt5_postInt5_dwPrediction60_nrThr20"

val reader = new ObjectInputStream(new FileInputStream(outputDirName + "/resultsOverAppliances.dat"))
val resultsOverAppliances = reader.readObject().asInstanceOf[Array[(Int, String, Array[((Double, Double), (Double, Double, Double))], Double)]]
reader.close()


val bestResultOverAppliances = extractingPerfOverThresholdAndAppliances(resultsOverAppliances)

bestResultOverAppliances.foreach(tuple => println(tuple))
// COMPUTING GLOBAL PERFORMANCES OVER APPLIANCES AND PRINT THEM
val HLtotalTest = bestResultOverAppliances.map(tuple => tuple._7).reduce(_+_)/bestResultOverAppliances.length
val HLalways0TotalTest = bestResultOverAppliances.map(tuple => tuple._8).reduce(_+_)/bestResultOverAppliances.length
val sensitivityTotalTest = bestResultOverAppliances.map(tuple => tuple._5).reduce(_+_)/bestResultOverAppliances.length
val precisionTotalTest = bestResultOverAppliances.map(tuple => tuple._6).reduce(_+_)/bestResultOverAppliances.length
val pippoTest = HLtotalTest/HLalways0TotalTest
println(f"\n\nTotal Sensitivity over appliances: $sensitivityTotalTest%1.3f, Precision: $precisionTotalTest%1.3f " +
  f"HL : $HLtotalTest%1.5f, HL always0Model: $HLalways0TotalTest%1.5f, HL/HL0: $pippoTest%3.2f\n")

