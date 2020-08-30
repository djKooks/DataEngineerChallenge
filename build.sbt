import Dependencies._

ThisBuild / scalaVersion     := "2.12.10"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.detest"
ThisBuild / organizationName := "detest"

lazy val root = (project in file("."))
  .settings(
    name := "daasTest",
    libraryDependencies ++= Seq(
      scalaTest % Test,
      sparkCore,
      sparkSql,
    ),
    mainClass in compile := Some("com.detest.WebLogProcessor")
  )

