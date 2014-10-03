import sbt._
import Keys._

object B extends Build {
  lazy val root =
    Project("root", file(".")).
      configs( IntegrationTest ).
      settings( Defaults.itSettings : _*).
      settings( libraryDependencies += specs )

  lazy val specs = "org.scalatest" %%  "scalatest" % "2.2.1" % "it,test"
}
