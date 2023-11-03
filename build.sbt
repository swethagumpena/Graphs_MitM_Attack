import scala.collection.immutable.Seq

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

val scalaTestVersion = "3.2.11"

lazy val root = (project in file("."))
  .settings(
    name := "Graphs_MitM_Attack",
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % "2.0.5",
      "ch.qos.logback" % "logback-classic" % "1.4.7",
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
      "org.scalatestplus" %% "mockito-4-2" % "3.2.12.0-RC2" % Test,
      "org.apache.spark" %% "spark-core" % "3.4.1",
      "org.apache.spark" %% "spark-sql" % "3.4.1",
      "org.apache.spark" %% "spark-graphx" % "3.4.1",
      "org.scalatest" %% "scalatest" % "3.0.8" % "test",
      "com.typesafe" % "config" % "1.4.1",
      "software.amazon.awssdk" % "s3" % "2.17.21",
    ),
    javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
    )
  )

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.14.0",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.14.0",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.14.0",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.0"
)

compileOrder := CompileOrder.JavaThenScala
test / fork := true
run / fork := true
run / javaOptions ++= Seq(
  "-Xms8G",
  "-Xmx100G",
  "-XX:+UseG1GC"
)

Compile / mainClass := Some("Main")
run / mainClass := Some("Main")

val jarName = "graphs_mitm_attack.jar"
assembly / assemblyJarName := jarName

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("com.fasterxml.jackson.**" -> "shaded.jackson.@1").inAll
)

// Merging strategies
ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}