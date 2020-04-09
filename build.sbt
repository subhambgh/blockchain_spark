lazy val root = (project in file(".")).
  settings(
    name := "intellij_proj",
    version := "1.0",
    scalaVersion := "2.11.12",
    mainClass in Compile := Some("com.blockchain.app.PreProcessinginHDFS")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.2" % "provided",
  "org.apache.spark" %% "spark-graphx" % "2.3.2" % "provided",
  "commons-io" % "commons-io" % "2.6",
  "io.sensesecure" % "hadoop-xz" % "1.4",
  "com.amazonaws" % "aws-java-sdk" % "1.11.427" exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "com.google.code.externalsorting" % "externalsorting" % "1.0" from "file:///C:/Users/Stefan/Desktop/externalsortinginjava-0.4.0.jar",
  "org.graphframes" % "graphframes" % "1.0" from "file:///C:/Users/Stefan/Downloads/graphframes-0.7.0-spark2.3-s_2.11.jar"
)

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}