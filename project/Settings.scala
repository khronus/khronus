import com.typesafe.sbteclipse.core.EclipsePlugin.{EclipseCreateSrc, EclipseKeys}
import sbt._
import Keys._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtNativePackager._
import NativePackagerKeys._

object Settings {

  val ScalaVersion = "2.11.7"

  lazy val basicSettings = Seq(
    scalaVersion  := ScalaVersion,
    organization := "com.searchlight",
    version := "0.2",
    exportJars := true,
    resolvers    ++= Dependencies.resolutionRepos,
    fork in (Test, run) := true,
    javacOptions  := Seq(
      "-source", "1.8", "-target", "1.8"
    ),
    scalacOptions := Seq(
      "-encoding",
      "utf8",
      "-g:vars",
      "-feature",
      "-unchecked",
      "-optimise",
      "-deprecation",
      "-target:jvm-1.8",
      "-language:postfixOps",
      "-language:implicitConversions",
      "-language:reflectiveCalls",
      "-Xlog-reflective-calls"
    ))

  lazy val formatSettings = SbtScalariform.scalariformSettings ++ Seq(
    ScalariformKeys.preferences in Compile := formattingPreferences,
    ScalariformKeys.preferences in Test    := formattingPreferences
  )

  lazy val extraPackagerSettings = Seq(bashScriptExtraDefines += """addJava "-Dconfig.file=${app_home}/../conf/application.conf" """,
    bashScriptExtraDefines += """addJava "-Dlogback.configurationFile=${app_home}/../conf/logback.xml" """,
    bashScriptExtraDefines += """addJava "-Xloggc:${app_home}/../logs/gc.log" """,
    bashScriptExtraDefines += """addJava "-Dkhronus.logdir=${app_home}/../logs" """,
    mappings in (Compile, packageBin) ~= { _.filter(!_._1.getName.equals("logback.xml")) }
  )

  def formattingPreferences =
    FormattingPreferences()
      .setPreference(RewriteArrowSymbols, true)
      .setPreference(AlignParameters, false)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)

  lazy val eclipseSettings = Seq(EclipseKeys.configurations := Set(Compile, Test, IntegrationTest), EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource)
  lazy val itExtraSettings = Seq(
    parallelExecution in IntegrationTest := false
  )

  lazy val extraTestSettings = Seq(javaOptions in Test += "-Dconfig.file=application-test.conf")
}
