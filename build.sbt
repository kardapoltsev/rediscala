lazy val common = Seq(
  organization := "com.github.Ma27",
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  scalaVersion := "2.12.8",
  crossScalaVersions := Seq(scalaVersion.value, "2.11.12"),
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
    "Typesafe repository snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
    "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/"
  ),
  publishMavenStyle := true,
  scalacOptions ++= Seq(
    "-encoding",
    "UTF-8",
    "-Xlint",
    "-deprecation",
    "-Xfatal-warnings",
    "-feature",
    "-language:postfixOps",
    "-unchecked"
  ),
  libraryDependencies ++= {
    val akkaVersion = "2.5.19"
    Seq(
      "org.scala-stm"            %% "scala-stm"       % "0.9",
      "com.typesafe.akka"        %% "akka-actor"      % akkaVersion,
      "com.typesafe.akka"        %% "akka-testkit"    % akkaVersion % Test,
      "de.heikoseeberger"        %% "akka-log4j"      % "1.6.1" % Test,
      "org.scalatest"            %% "scalatest"       % "3.0.5" % Test,
      "org.scalacheck"           %% "scalacheck"      % "1.14.0" % Test,
      "org.apache.logging.log4j" % "log4j-api"        % "2.11.1" % Test,
      "org.apache.logging.log4j" % "log4j-core"       % "2.11.1" % Test,
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.11.1" % Test,
      "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0" % Test,
    )
  },
  autoAPIMappings := true,
  // TODO create new github pages target
  apiURL := Some(url("http://etaty.github.io/rediscala/"))
)

lazy val root = (project in file(".")).settings(common, name := "rediscala")

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
