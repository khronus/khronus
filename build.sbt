organization  := "com.example"

version       := "0.1"

scalaVersion  := "2.11.2"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  val akkaV = "2.3.5"
  val sprayV = "1.3.1"
  val astyanaxVersion = "1.56.49"
  Seq(
    "io.spray"            	%%  "spray-can"     		% sprayV,
    "io.spray"            	%%  "spray-routing" 		% sprayV,
    "io.spray"            	%%  "spray-testkit" 		% sprayV  % "test",
    "io.spray"            	%%  "spray-json"    		% "1.2.6",
    "com.typesafe.akka"   	%%  "akka-actor"    		% akkaV,
    "com.typesafe.akka"   	%%  "akka-testkit"  		% akkaV   % "test",
    "org.specs2"          	%%  "specs2-core"   		% "2.3.11" % "test",
    "org.scalatest"       	%%  "scalatest"     		% "2.2.1" % "test",
    "org.hdrhistogram"    	%   "HdrHistogram"  		% "1.2.1",
    "org.mockito"         	%   "mockito-all"   		% "1.9.5" % "test",
    "com.netflix.astyanax"	%	"astyanax-core"			%	astyanaxVersion,
    "com.netflix.astyanax"	%	"astyanax-thrift"		%	astyanaxVersion,
    "com.netflix.astyanax"	%	"astyanax-cassandra"	%	astyanaxVersion,
    "com.typesafe" % "config" % "1.2.1",
    "com.esotericsoftware.kryo" % "kryo" % "2.24.0"
  )
}

Revolver.settings
