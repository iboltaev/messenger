import org.scalajs.linker.interface.ModuleSplitStyle

lazy val messages = project.in(file("messages"))
  .enablePlugins(ScalaJSPlugin)
  .settings(
    libraryDependencies ++= Seq(
      "com.lihaoyi" %%% "upickle" % "3.1.0"
    )
  )

lazy val client = project.in(file("."))
  .enablePlugins(ScalaJSPlugin)
  .dependsOn(messages)
  .settings(
    scalaJSUseMainModuleInitializer := true,

    scalaJSLinkerConfig ~= {
      _.withModuleKind(ModuleKind.ESModule)
        .withModuleSplitStyle(
          ModuleSplitStyle.SmallModulesFor(List("livechart")))
    },

    scalacOptions ++= Seq(
      "-language:experimental.macros",
      "-Xasync"
    ),

    libraryDependencies ++= Seq(
        "org.scala-js" %%% "scalajs-dom" % "2.4.0",
        "com.lihaoyi" %%% "upickle" % "3.1.0",

        "org.typelevel" %%% "cats-effect" % "3.6-1f95fd7",
        "org.typelevel" %%% "cats-effect-cps" % "0.4.0",
        "co.fs2" %%% "fs2-core" % "3.6.1",
        "co.fs2" %%% "fs2-io" % "3.6.1",

        "org.scalatest" %% "scalatest" % "3.2.6" % Test,
        "org.scalatestplus" %% "scalacheck-1-17" % "3.2.16.0" % Test)
  )

lazy val root = (project in file("."))
  .aggregate(client, messages)