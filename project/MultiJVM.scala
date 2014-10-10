import sbt._
import Keys._
import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

object MultiJVM {

  lazy val multiJvmSettings = SbtMultiJvm.multiJvmSettings ++ Seq(
    compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test), // make sure that MultiJvm test are compiled by the default test compilation
    parallelExecution in Test := false,                                          // disable parallel tests
    executeTests in Test <<=
      ((executeTests in Test), (executeTests in MultiJvm)) map {
        case ((testResults), (multiJvmResults)) =>
          val overall =
            if (testResults.overall.id < multiJvmResults.overall.id) multiJvmResults.overall
            else testResults.overall
          Tests.Output(overall,
            testResults.events ++ multiJvmResults.events,
            testResults.summaries ++ multiJvmResults.summaries)
      }
  )
}