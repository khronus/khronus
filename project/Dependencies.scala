import sbt._

object Dependencies {

  val resolutionRepos = Seq(
    "spray repo"    at "http://repo.spray.io/",
    "typesafe repo" at "http://repo.typesafe.com/typesafe/releases/",
    "theatr.us"     at "http://repo.theatr.us"
  )

  val akkaV             =       "2.3.6"
  val sprayV            =       "1.3.1"
  val astyanaxVersion   =       "1.56.49"

  val sprayCan          =       "io.spray"                        %%  "spray-can"                   % sprayV
  val sprayRouting      =       "io.spray"                        %%  "spray-routing"               % sprayV
  val sprayTestKit      =       "io.spray"                        %%  "spray-testkit"               % sprayV
  val sprayJson         =       "io.spray"                        %%  "spray-json"                  % "1.2.6"
  val akkaActor         =       "com.typesafe.akka"               %%  "akka-actor"                  % akkaV
  val akkaTestKit       =       "com.typesafe.akka"               %%  "akka-testkit"                % akkaV
  val akkaRemote        =       "com.typesafe.akka"               %%  "akka-remote"                 % akkaV
  val akkaCluster       =       "com.typesafe.akka"               %%  "akka-cluster"                % akkaV
  val multiNodeTestKit  =       "com.typesafe.akka"               %%  "akka-multi-node-testkit"     % akkaV
  val akkaSlf4j         =       "com.typesafe.akka"               %%  "akka-slf4j"                  % akkaV
  val akkaContrib       =       "com.typesafe.akka"               %%  "akka-contrib"                % akkaV
  val scalaTest         =       "org.scalatest"                   %%  "scalatest"                   % "2.2.1"
  val akkaQuartz        =       "us.theatr"                       %%  "akka-quartz"                 % "0.3.0"
  val hdrHistogram      =       "org.hdrhistogram"                %   "HdrHistogram"                % "1.2.1"
  val specs2            =       "org.specs2"          	          %%  "specs2-core"   		          % "2.3.11"
  val mockito           =       "org.mockito"         	          %   "mockito-all"   		          % "1.9.5"
  val astyanaxCore      =       "com.netflix.astyanax"	          %	  "astyanax-core"		            %	astyanaxVersion excludeAll(ExclusionRule(name = "log4j"), ExclusionRule(name = "slf4j-log4j12"))
  val astyanaxThrift    =       "com.netflix.astyanax"	          %	  "astyanax-thrift"	            %	astyanaxVersion excludeAll(ExclusionRule(name = "log4j"), ExclusionRule(name = "slf4j-log4j12"))
  val astyanaxCassandra =       "com.netflix.astyanax"	          %	  "astyanax-cassandra"          %	astyanaxVersion excludeAll(ExclusionRule(name = "log4j"), ExclusionRule(name = "slf4j-log4j12"))
  val kryo              =       "com.esotericsoftware.kryo"       %   "kryo"                        % "2.24.0"
  val scalaLogging      =       "com.typesafe.scala-logging"      %%  "scala-logging"               % "3.1.0"
  val slf4j             =       "org.slf4j"                       %   "slf4j-api"                   % "1.7.7"
  val logbackClassic    =       "ch.qos.logback"                  %   "logback-classic"             % "1.1.2"
  val commonsLang       =       "commons-lang"                    %   "commons-lang"                % "2.6"

  def compile(deps: ModuleID*): Seq[ModuleID]   = deps map (_ % "compile")
  def provided(deps: ModuleID*): Seq[ModuleID]  = deps map (_ % "provided")
  def test(deps: ModuleID*): Seq[ModuleID]      = deps map (_ % "test")
  def runtime(deps: ModuleID*): Seq[ModuleID]   = deps map (_ % "runtime")
  def it(deps: ModuleID*): Seq[ModuleID]        = deps map (_ % "it")
}