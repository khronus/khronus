import com.typesafe.sbteclipse.core.EclipsePlugin.{EclipseCreateSrc, EclipseKeys}
import sbt._
import Keys._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._

object Settings {

  val ScalaVersion = "2.11.2"

  lazy val basicSettings = Seq(
    scalaVersion  := ScalaVersion,
    organization := "com.despegar",
    version := "0.1",
    resolvers    ++= Dependencies.resolutionRepos,
    fork in run   := true,
    javacOptions  := Seq(
      "-source", "1.7", "-target", "1.7"
    ),
    scalacOptions := Seq(
      "-encoding",
      "utf8",
      "-g:vars",
      "-feature",
      "-unchecked",
      "-optimise",
      "-deprecation",
      "-target:jvm-1.6",
      "-language:postfixOps",
      "-language:implicitConversions",
      "-Xlog-reflective-calls"
    ))

  lazy val formatSettings = SbtScalariform.scalariformSettings ++ Seq(
    ScalariformKeys.preferences in Compile := formattingPreferences,
    ScalariformKeys.preferences in Test    := formattingPreferences
  )

  def formattingPreferences =
    FormattingPreferences()
      .setPreference(RewriteArrowSymbols, true)
      .setPreference(AlignParameters, false)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)

  lazy val eclipseSettings = Seq(EclipseKeys.configurations := Set(Compile, Test, IntegrationTest), EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource)
  lazy val itExtraSettings = Seq(parallelExecution in IntegrationTest := false)
}