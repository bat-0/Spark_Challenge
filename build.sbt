ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"

lazy val root = (project in file("."))
  .settings(
    name := "Spark_Challenge"
  )
