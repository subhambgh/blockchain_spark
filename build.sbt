lazy val root = (project in file(".")).
  settings(
    name := "intellij_proj",
    version := "1.0",
    scalaVersion := "2.11.12",
    mainClass in Compile := Some("com.blockchain.app.BlockChain1")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.2" % "provided",
  "org.apache.spark" %% "spark-graphx" % "2.3.2" % "provided",
  "commons-io" % "commons-io" % "2.6",
  "io.sensesecure" % "hadoop-xz" % "1.4",
  "com.amazonaws" % "aws-java-sdk" % "1.11.427" exclude("com.fasterxml.jackson.core", "jackson-databind")
)

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}