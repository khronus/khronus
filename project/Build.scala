import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import spray.revolver.RevolverPlugin._
import sbt.Defaults.itSettings
import sbtassembly.Plugin._
import sbt.Keys._
import sbt._

object Metrik extends Build {

  import Dependencies._
  import MultiJVM._
  import Settings._

  lazy val root =
    Project("metrik", file("."))
      .configs(IntegrationTest)
      .configs(MultiJvm)
      .settings(itSettings: _*)
      .settings(itExtraSettings: _*)
      .settings(basicSettings: _*)
      .settings(formatSettings: _*)
      .settings(multiJvmSettings: _*)
      .settings(eclipseSettings:_*)
      .settings(Revolver.settings:_*)
      .settings(assemblySettings: _*)
      .settings(
        libraryDependencies ++=
          compile(sprayClient, sprayCan, sprayRouting, sprayTestKit, sprayJson, akkaActor, akkaTestKit, akkaRemote, akkaCluster, akkaContrib, multiNodeTestKit, scalaTest, akkaQuartz,
            hdrHistogram, specs2, mockito, astyanaxCore, astyanaxThrift, astyanaxCassandra, kryo, scalaLogging, slf4j, logbackClassic, commonsLang, akkaSlf4j) ++
          test(sprayTestKit, akkaTestKit, multiNodeTestKit, scalaTest, specs2, mockito) ++
          it(scalaTest)
      )
}
