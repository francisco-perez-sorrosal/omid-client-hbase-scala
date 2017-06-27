name := "omid-client-hbase-scala"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies += "com.lihaoyi" % "ammonite" % "1.0.0-RC9" % "test" cross CrossVersion.full

sourceGenerators in Test += Def.task {
  val file = (sourceManaged in Test).value / "amm.scala"
  IO.write(file, """object amm extends App { ammonite.Main().run() }""")
  Seq(file)
}.taskValue

// Optional, required for the `src` command to work
(fullClasspath in Test) ++= {
  (updateClassifiers in Test).value
    .configurations
    .find(_.configuration == Test.name)
    .get
    .modules
    .flatMap(_.artifacts)
    .collect{case (a, f) if a.classifier == Some("sources") => f}
}

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.5.2",
  "com.typesafe.akka" %% "akka-slf4j" % "2.5.2",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.2" % Test
)

libraryDependencies += "org.apache.hbase" % "hbase-client" % "0.98.6-hadoop1"
libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"
libraryDependencies += "com.google.inject" % "guice" % "3.0"
libraryDependencies += "io.netty" % "netty" % "3.6.10.Final"

libraryDependencies += "org.apache.omid" % "omid-common" % "0.8.2.0"
libraryDependencies += "org.apache.omid" % "omid-transaction-client" % "0.8.2.0"
libraryDependencies += "org.apache.omid" % "omid-tso-server" % "0.8.2.0" % "test"

dependencyOverrides += "com.google.guava" % "guava" % "14.0.1"