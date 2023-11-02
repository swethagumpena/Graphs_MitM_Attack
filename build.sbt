ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "Graphs_MitM_Attack",
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % "2.0.5",
      "ch.qos.logback" % "logback-classic" % "1.4.7",
      "org.apache.spark" %% "spark-core" % "3.4.1",
      "org.apache.spark" %% "spark-sql" % "3.4.1",
      "org.apache.spark" %% "spark-graphx" % "3.4.1",
      "org.scalatest" %% "scalatest" % "3.0.8" % "test",
      "com.typesafe" % "config" % "1.4.1",
      "software.amazon.awssdk" % "s3" % "2.17.21",
    )
  )