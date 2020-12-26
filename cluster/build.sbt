name := "cluster"

version := "0.1"

scalaVersion := "2.13.4"

val akkaVersion = "2.6.0"

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-encoding", "UTF-8",
  "-unchecked",
  "-Xlint",
)

libraryDependencies ++= Seq(
  "io.netty" % "netty" % "3.10.6.Final",
  "com.typesafe.akka"        %% "akka-remote" % akkaVersion,
  "com.typesafe.akka"        %% "akka-actor"         % akkaVersion,
  "com.typesafe.akka"        %% "akka-cluster"         % akkaVersion,
  "com.novocode"             % "junit-interface"     % "0.11"      % Test
)
