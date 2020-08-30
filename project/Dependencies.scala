import sbt._

object Dependencies {

  val sparkVersion = "2.4.3"

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"
  lazy val sparkCore = "org.apache.spark" % "spark-core_2.12" % sparkVersion % "provided"
  lazy val sparkSql = "org.apache.spark" % "spark-sql_2.12" % sparkVersion % "provided"
}
