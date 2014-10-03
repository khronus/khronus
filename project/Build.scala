import sbt._
import Keys._
import com.typesafe.sbteclipse.plugin.EclipsePlugin._

object B extends Build {
  lazy val root =
    Project("root", file(".")).
      configs( IntegrationTest ).
      settings( Defaults.itSettings : _*).
      settings( libraryDependencies += specs ).
      settings(EclipseKeys.configurations := Set(Compile, Test, IntegrationTest), EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource)

  lazy val specs = "org.scalatest" %%  "scalatest" % "2.2.1" % "it,test"
}
