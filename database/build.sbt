lazy val scalaV = "2.12.1"
lazy val akkaV = "2.5.0"

lazy val root: Project =
  Project("database", file("."))
    .settings(commonSettings)
    .settings(
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-actor" % akkaV,
        "io.suzaku" %% "boopickle" % "1.2.6",

        "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
        "org.slf4j" % "slf4j-api" % "1.7.25",
        "ch.qos.logback" % "logback-classic" % "1.1.7",
        "com.typesafe" % "config" % "1.3.1",

        "com.typesafe.akka" %% "akka-testkit" % akkaV,
        "org.scalactic" %% "scalactic" % "3.0.1",
        "org.scalatest" %% "scalatest" % "3.0.1" % "test"
      ))
    .settings(
      assemblyMergeStrategy in assembly := {
        case PathList("application.conf") => MergeStrategy.discard
        case PathList("logback.xml") => MergeStrategy.discard
        case x =>
          val oldStrategy = (assemblyMergeStrategy in assembly).value
          oldStrategy(x)
      },
      fork in run := true,
      fork in Test := true
    )

lazy val scalariformSupportformatSettings = SbtScalariform.scalariformSettings ++ Seq(
  SbtScalariform.ScalariformKeys.preferences in Compile := formattingPreferences,
  SbtScalariform.ScalariformKeys.preferences in Test    := formattingPreferences
)

import scalariform.formatter.preferences._
def formattingPreferences =
  FormattingPreferences()
    .setPreference(RewriteArrowSymbols, true)
    .setPreference(AlignParameters, true)
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(DoubleIndentClassDeclaration, true)


def commonSettings: Seq[Setting[_]] = Seq(
  scalaVersion := scalaV,
  javaOptions ++= Seq("-Xmx8G", "-Xms4G", "-XX:+UseG1GC")
) ++ scalariformSupportformatSettings
