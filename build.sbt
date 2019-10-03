organization := "org.charleso"
name := "fuse"
version := "0.1.0"

scalaVersion := "2.11.12"

val sparkVersion  = "2.3.3"

libraryDependencies ++= List(
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided
  , "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided
  , "org.scalaz" %% "scalaz-core" % "7.2.23"
  , "hedgehog"  %% "hedgehog-sbt" % "6d369c1378941b701e7f2cfa1e8f9e10e38b3568" % Test
  )

testFrameworks := Seq(TestFramework("hedgehog.sbt.Framework"))
