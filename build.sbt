import scalariform.formatter.preferences._
import AssemblyKeys._

organization  := "com.example"

version       := "0.1"

scalaVersion  := "2.11.2"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

resolvers += "theatr.us" at "http://repo.theatr.us"

libraryDependencies ++= {
  val akkaV = "2.3.6"
  val sprayV = "1.3.1"
  Seq(
    "io.spray"            %%  "spray-can"                 % sprayV,
    "io.spray"            %%  "spray-routing"             % sprayV,
    "io.spray"            %%  "spray-testkit"             % sprayV        % "test",
    "io.spray"            %%  "spray-json"                % "1.2.6",
    "com.typesafe.akka"   %%  "akka-actor"                % akkaV,
    "com.typesafe.akka"   %%  "akka-testkit"              % akkaV         % "test",
    "com.typesafe.akka"   %%  "akka-remote"               % akkaV,
    "com.typesafe.akka"   %%  "akka-cluster"              % akkaV,
    "com.typesafe.akka"   %%  "akka-contrib"              % "2.3.6",
    "com.typesafe.akka"   %%  "akka-multi-node-testkit"   % akkaV         % "test",
    "org.scalatest"       %%  "scalatest"                 % "2.2.1"       % "test",
    "org.specs2"          %%  "specs2-core"               % "2.3.11"      % "test",
    "us.theatr"           %%  "akka-quartz"               % "0.3.0",
    "org.hdrhistogram"    %   "HdrHistogram"              % "1.2.1"
  )
}

Revolver.settings

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(RewriteArrowSymbols, true)
  .setPreference(AlignParameters, false)
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(DoubleIndentClassDeclaration, true)
  .setPreference(PreserveDanglingCloseParenthesis, true)


// Assembly settings
mainClass in Global := Some("com.despegar.metrik.util.Boot")

jarName in assembly := "metrik.jar"

assemblySettings