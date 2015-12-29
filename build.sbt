import _root_.sbtassembly.AssemblyPlugin.autoImport._

name := "BigDataSpark"

version := "0.1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2" % "provided",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.1" % "provided",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.6.1" % "provided",
  "org.scalatest" % "scalatest_2.10" % "2.2.5" % "test"
)

test in assembly := {}

assemblyOption in assembly :=
  (assemblyOption in assembly).value.copy(includeScala = false)

dependencyOverrides += "org.scala-lang" % "scala-compiler" % scalaVersion.value
dependencyOverrides += "org.scala-lang" % "scala-reflect" % scalaVersion.value
dependencyOverrides += "org.scala-lang" % "scala-library" % scalaVersion.value
dependencyOverrides += "org.apache.commons" % "commons-lang3" % "3.3.2"
dependencyOverrides += "org.slf4j" % "slf4j-api" % "1.7.10"
