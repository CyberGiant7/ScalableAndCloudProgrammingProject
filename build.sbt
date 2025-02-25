ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.12.20"

ThisBuild / libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.4",
  "org.apache.spark" %% "spark-sql" % "3.5.4"
)

lazy val root = (project in file("."))
  .settings(
    name := "ScalableProject"
  )

