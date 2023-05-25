ThisBuild / scalaVersion := "2.13.10"
ThisBuild / scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-unchecked",
  "-language:postfixOps",
  "-language:higherKinds",
  "-language:experimental.macros",
  "-Xasync")

lazy val coreDependencies = Seq(
  "org.typelevel" %% "cats-effect" % "3.6-1f95fd7",
  "org.typelevel" %% "cats-effect-cps" % "0.4.0",
  "co.fs2" %% "fs2-core" % "3.6.1",
  "co.fs2" %% "fs2-io" % "3.6.1",

  "org.scalatest" %% "scalatest" % "3.2.6" % Test,
  "org.scalatestplus" %% "scalacheck-1-17" % "3.2.16.0" % Test
)

lazy val backendDependencies = Seq(
  "org.apache.hbase" % "hbase" % "2.1.3",
  "org.apache.hbase" % "hbase-client" % "2.1.3",
  "org.typelevel" %% "log4cats-core"    % "2.6.0",  // Only if you want to Support Any Backend
  "org.typelevel" %% "log4cats-slf4j"   % "2.6.0",  // Direct Slf4j Support - Recommended
  "org.scala-lang" % "scala-reflect" % "2.13.10",
  "com.softwaremill.magnolia1_2" %% "magnolia" % "1.1.3",
  "io.grpc" % "grpc-netty-shaded" % scalapb.compiler.Version.grpcJavaVersion
)

lazy val core = (project in file("core"))
  .settings(
    libraryDependencies ++= coreDependencies
  )

lazy val backend = (project in file("backend"))
  .settings(
    libraryDependencies ++= coreDependencies ++ backendDependencies
  )
  .enablePlugins(Fs2Grpc)
  .dependsOn(core)

lazy val root = (project in file("."))
  .aggregate(core, backend)


//libraryDependencies += "org.apache.hbase" % "hbase-client" % "2.5.4"



