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
    .aggregate(metrik,metrikCore, metrikStress, metrikInflux)
    .settings(basicSettings: _*)
    .settings(formatSettings: _*)
    .settings(eclipseSettings: _*)
    .settings(noPublishing: _*)

  lazy val metrik = Project("metrik", file("metrik"))
    .dependsOn(metrikCore, metrikInflux, metrikStress)
    .settings(basicSettings: _*)
    .settings(formatSettings: _*)
    .settings(packagerSettings: _*)
    .settings(libraryDependencies ++=
              compile(sprayCan, sprayJson, akkaActor))


  lazy val metrikCore = Project("metrik-core", file("metrik-core"))
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
    compile(akkaKryo, sprayClient, sprayCan, sprayRouting, sprayJson, akkaActor, akkaRemote, akkaCluster, akkaContrib, akkaQuartz, hdrHistogram, cassandraDriver, kryo, scalaLogging, slf4j, logbackClassic, commonsLang, akkaSlf4j) ++
      test(sprayTestKit, mockito, akkaTestKit, multiNodeTestKit, scalaTest, specs2, mockito) ++
      it(scalaTest))

  lazy val metrikStress = Project("metrik-stress", file("metrik-stress"))
    .dependsOn(metrikCore)
    .settings(basicSettings: _*)
    .settings(formatSettings: _*)
    .settings(noPublishing: _*)
    .settings(libraryDependencies ++=
      compile(sprayClient, sprayCan, sprayJson, akkaActor))


  lazy val metrikInflux = Project("metrik-influx-api", file("metrik-influx-api"))
    .dependsOn(metrikCore)
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
