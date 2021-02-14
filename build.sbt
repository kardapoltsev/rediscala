Global / onChangedBuildSource := ReloadOnSourceChanges

lazy val common = Seq(
  organization := "com.github.Ma27",
  publishTo := sonatypePublishTo.value,
  scalaVersion := "2.12.11",
  crossScalaVersions := Seq(scalaVersion.value, "2.13.4"),
  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  homepage := Some(url("https://github.com/Ma27/rediscala")),
  scmInfo := Some(ScmInfo(url("https://github.com/Ma27/rediscala"), "scm:git:git@github.com:Ma27/rediscala.git")),
  apiURL := Some(url("http://etaty.github.io/rediscala/latest/api/")),
  pomExtra :=
    <developers>
      <developer>
        <id>Ma27</id>
        <name>Valerian Barbot, The Rediscala community</name>
        <url>http://github.com/Ma27/</url>
      </developer>
    </developers>,
  resolvers ++= Seq(
    "Typesafe repository snapshots" at "https://repo.typesafe.com/typesafe/snapshots/",
    "Typesafe repository releases" at "https://repo.typesafe.com/typesafe/releases/",
    // using staging artifact until 2.13 release is ready: https://github.com/apache/logging-log4j-scala/pull/3
    "Apache staging repository" at "https://repository.apache.org/content/repositories/staging"
  ),
  publishMavenStyle := true,
  scalacOptions ++= Seq(
    "-encoding", "UTF-8",
    "-Xlint",
    "-deprecation",
    "-feature",
    "-language:postfixOps",
    "-unchecked"
  ),

  libraryDependencies ++= {
    val akkaVersion = "2.6.12"
    val log4jVersion = "2.14.0"
    Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "org.scalatest"            %% "scalatest"       % "3.2.3" % Test,
      "org.scalacheck"           %% "scalacheck"      % "1.14.0" % Test,
      "org.apache.logging.log4j" % "log4j-api"        % log4jVersion % Test,
      "org.apache.logging.log4j" % "log4j-core"       % log4jVersion % Test,
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion % Test,
      "org.scala-stm" %% "scala-stm" % "0.9.1"
    )
  },

  autoAPIMappings := true,

  // TODO create new github pages target
  apiURL := Some(url("http://etaty.github.io/rediscala/"))
)

lazy val root = (project in file(".")).settings(
  common,
  name := "rediscala",
  logBuffered in Test := true,
  libraryDependencies ++= Seq(
//     log4j-api-scala brings in scalatest 3.2.0-M1
    "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0" % "test->test"
  )
)

lazy val bench = (project in file("src/bench"))
  .settings(
    name := "rediscala-bench",
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
    parallelExecution in Test := false,
    logBuffered := false,
    libraryDependencies ++= Seq(
      "com.storm-enroute" %% "scalameter" % "0.9"
    )
  )
  .dependsOn(root)
