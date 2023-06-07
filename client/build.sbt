import org.scalajs.linker.interface.ModuleSplitStyle

lazy val client = project.in(file("."))
  .enablePlugins(ScalaJSPlugin)
  .settings(
    scalaJSUseMainModuleInitializer := true,

    scalaJSLinkerConfig ~= {
      _.withModuleKind(ModuleKind.ESModule)
        .withModuleSplitStyle(
          ModuleSplitStyle.SmallModulesFor(List("livechart")))
    },

    libraryDependencies ++= Seq("org.scala-js" %%% "scalajs-dom" % "2.4.0",
        "org.scalatest" %% "scalatest" % "3.2.6" % Test,
        "org.scalatestplus" %% "scalacheck-1-17" % "3.2.16.0" % Test,
        "com.fasterxml.jackson.core" % "jackson-databind" % "2.14.3" % Test,
        "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.3" % Test)
  )