name := "CloudVoting"

version := "2.0"

scalaVersion := "2.11.8"

val sparkVer = "2.0.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVer

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVer

libraryDependencies += "org.apache.spark" %% "spark-graphx" % sparkVer

