name := "dcp"

version := "1.0"

scalaVersion := "2.10.5"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8", "-feature")

lazy val root = (project in file(".")).enablePlugins(PlayScala)

val akkaV = "2.3.9"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.6.2",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.6.2",
  "org.apache.hbase" % "hbase" % "1.1.1",
  "org.apache.hbase" % "hbase-protocol" % "1.1.1",
  "org.apache.hbase" % "hbase-server" % "1.1.1",
  "org.apache.hbase" % "hbase-client" % "1.1.1",
  "org.apache.hbase" % "hbase-common" % "1.1.1",
  "org.json4s" % "json4s-jackson_2.10" % "3.3.0",
  "com.googlecode.xmemcached" % "xmemcached" % "2.0.0",
  "org.apache.httpcomponents" % "httpclient" % "4.5.1",
  "com.typesafe.akka" %% "akka-testkit" % akkaV,
  "org.specs2" %% "specs2" % "2.3.11" % "test",
  "com.typesafe.akka" % "akka-remote_2.10" % "2.3.14"
)

doc in Compile <<= target.map(_ / "none")
