import sbt.Keys._

val sparkVersion = "1.6.0-cdh5.7.0"
val scalaVersionNumber: String = "2.10.6"

/**
  * working job
  */
lazy val energyDisaggregation: Project = (project in file("."))
  .settings(commonDependecies("energyDisaggregation"))
  //  .settings(name.:=("energyDisaggregation"))
  .settings(libraryDependencies ++= addSparkDependencies("provided"))

/**
  * this is going to be used inside intelliJ IDEA
  */
lazy val energyDisaggregationRunner = project
  .in(file("energyDisaggregationRunner"))
  .dependsOn(energyDisaggregation)
  .settings(commonDependecies("energyDisaggregationRunner"))
  .settings(libraryDependencies ++= addSparkDependencies("compile"))

/**
  * add all common dependencies
  * @param moduleName
  * @return
  */
def commonDependecies(moduleName: String) = {

  Seq(
    organization := "com.cgnal.scava",
    name := moduleName,
    version := "1.0.0-SNAPSHOT",
    scalaVersion := scalaVersionNumber,
    javaOptions += "-Xms512m -Xmx2G",
    resolvers ++= Seq(
      Resolver.mavenLocal,
      "Cloudera CDH" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
    ),
    libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.5" % "test"
  )
}

/**
  * add all spark dependencies
  * @param scope
  * @return
  */
def addSparkDependencies(scope: String): Seq[ModuleID] = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % scope,
  "org.apache.spark" %% "spark-sql" % sparkVersion % scope,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % scope,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % scope
)
