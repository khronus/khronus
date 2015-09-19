import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import spray.revolver.RevolverPlugin._
import sbt.Defaults.itSettings
import sbtassembly.Plugin._
import sbt.Keys._
import sbt._

object Projects extends Build {

  import Dependencies._
  import MultiJVM._
  import Settings._
  import Packager._

  lazy val root = Project("root", file("."))
    .aggregate(khronus,khronusCore, khronusStress, khronusInflux)
    .settings(basicSettings: _*)
    .settings(formatSettings: _*)
    .settings(eclipseSettings: _*)
    .settings(noPublishing: _*)

  lazy val khronus = Project("khronus", file("khronus"))
    .dependsOn(khronusCore, khronusInflux)
    .settings(basicSettings: _*)
    .settings(formatSettings: _*)
    .settings(packagerSettings: _*)
    .settings(extraPackagerSettings: _*)
    .settings(extraTestSettings: _*)
    .settings(libraryDependencies ++=
              compile(sprayCan, sprayJson, akkaActor))


  lazy val khronusCore = Project("khronus-core", file("khronus-core"))
    .settings(basicSettings: _*)
    .settings(formatSettings: _*)
    .configs(IntegrationTest)
    .configs(MultiJvm)
    .settings(itSettings: _*)
    .settings(itExtraSettings: _*)
    .settings(multiJvmSettings: _*)
    .settings(Revolver.settings: _*)
    .settings(assemblySettings: _*)
    .settings(libraryDependencies ++=
    compile(jacksonAfterBurner, jacksonScala, sprayClient, sprayCan, sprayRouting, sprayJson, akkaActor, akkaRemote, akkaCluster, akkaContrib, akkaQuartz, hdrHistogram, cassandraDriver, snappy, kryo, scalaLogging, slf4j, logbackClassic, commonsLang, akkaSlf4j) ++
      test(sprayTestKit, mockito, akkaTestKit, multiNodeTestKit, scalaTest, specs2, mockito) ++
      it(scalaTest))

  lazy val khronusStress = Project("khronus-stress", file("khronus-stress"))
    .dependsOn(khronusCore)
    .settings(basicSettings: _*)
    .settings(formatSettings: _*)
    .settings(noPublishing: _*)
    .settings(libraryDependencies ++=
      compile(jacksonAfterBurner, jacksonScala, sprayClient, sprayCan, sprayJson, akkaActor))


  lazy val khronusInflux = Project("khronus-influx-api", file("khronus-influx-api"))
    .dependsOn(khronusCore % "test->test;compile->compile")
    .configs(IntegrationTest)
    .settings(basicSettings: _*)
    .settings(formatSettings: _*)
    .settings(itSettings: _*)
    .settings(itExtraSettings: _*)
    .settings(libraryDependencies ++=
      compile(parserCombinators, sprayClient, sprayCan, sprayRouting, sprayJson, akkaActor, commonsCodec) ++
      test(sprayTestKit, mockito, akkaTestKit, scalaTest, specs2, mockito) ++
      it(scalaTest)
    )

  val noPublishing = Seq(publish :=(), publishLocal :=(), publishArtifact := false)
}
