name := "trades_batch"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "1.4.1" % "provided",
"org.apache.spark" %% "spark-sql" % "1.4.1" % "provided",
"com.datastax.cassandra"  % "cassandra-driver-core" % "2.1.6",
"com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-M1",
"com.typesafe.play" % "play-json_2.10" % "2.4.2"
)
